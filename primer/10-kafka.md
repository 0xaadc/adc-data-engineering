# 10. Kafka and Event Streaming, From Zero

> As of October 2026. Tags: **(GA)** generally available · **(Preview)** public/private preview · **(⚠ verify)** not confirmed against official docs.

## Why this exists

Around 2010 LinkedIn had dozens of systems, including OLTP databases, search, Hadoop and monitoring, and each one needed data from several others. Connecting them point to point meant N×M custom pipelines, each with its own format, retries and backpressure. Classic message queues didn't solve this. They delete a message once it is acknowledged, they track state for every message, and they can't answer a request like "a new team wants last week's events."

Kafka's bet is one **distributed, partitioned, replicated, append-only commit log** (P5). Producers append records, which stay for a retention period whether or not anyone reads them. Each consumer keeps its own position, called an *offset*. N×M becomes N+M, producers and consumers decouple in time and rate, and replay becomes routine. If you know Databricks, think of a partition as the `_delta_log` idea applied to events, and of a consumer's offset as its Structured Streaming checkpoint.

## Core concepts

### The log, and why it is fast

```
topic "orders", partition 0        offsets are per partition
offset:  0    1    2    3    4    5    6    7   <- append
       [r0] [r1] [r2] [r3] [r4] [r5] [r6] [r7]
                  ^                         ^
  group "lake-ingest" at 2 (lag 6)    group "fraud" at 7 (caught up)
```

Kafka is fast because of its structure:
- **Sequential I/O.** Producers append to segment files and consumers tail them. Nothing is updated in place (P1).
- **The OS page cache acts as the cache.** Consumers that keep up read recent records from memory instead of disk.
- **Batches stay intact end to end.** The producer compresses a batch, the broker stores and serves it unchanged (zero-copy where TLS allows), and the consumer decompresses it.
- **The broker stays simple.** It keeps no delivery state per message. A consumer group's progress is just one integer per partition, so adding another group costs almost nothing.

### Records, keys and partitions

A record has a **key, value, headers and timestamp**. Kafka treats key and value as opaque bytes, so schemas are the client's job (P8). The partition is the unit of **ordering, parallelism, replication and placement**.

The default partitioner sends each record to `hash(key) % partitions`. That means **one key always maps to one partition, and its records stay in order there. Kafka gives no ordering across partitions.** Records with a null key are spread across partitions with no ordering at all. Choose a key that identifies the entity whose events must stay in order, such as `account_id`.

Several consequences follow from that formula:
- **Hot partitions.** One huge key overloads one partition and therefore one consumer, and adding partitions doesn't help. Your options are a finer key, salting the key (which gives up per-entity order), or a separate topic for the heavy keys.
- **Adding partitions remaps keys.** You can increase the partition count but never decrease it. Once N changes, a key's new events land in a different partition than its old ones, so ordering breaks and compacted topics can hold two "latest" values for the same key. Provision headroom up front.
- **Partition count is a parallelism ceiling.** Within a group, each partition has at most one consumer.
- **Over-partitioning has a cost.** Each partition adds files, memory and replication work, slows recovery, shrinks batches and lengthens rebalances.

### Consumer groups, offsets, rebalancing

Consumers that share a `group.id` split the partitions between them, and any extra consumers sit idle. Separate groups are independent, which is how fan-out works. Each group's progress lives as **committed offsets** in the compacted internal topic `__consumer_offsets`. Auto-commit (every 5 s) records what the consumer has *polled*, not what it has *finished*.

Two defaults surprise people:
- `auto.offset.reset=latest` applies when no valid offset exists, for example for a new group or after retention deleted the stored offset. In that case the consumer silently skips the backlog.
- An empty group loses its offsets after `offsets.retention.minutes` (7 days). A pipeline paused for two weeks restarts at `latest`.

> **Databricks note:** Spark's Kafka source keeps offsets in its checkpoint and never commits them to Kafka. Kafka lag tools can't see Spark consumers, and deleting the checkpoint resets their offsets.

A **rebalance** reassigns partitions in three situations: a member joins or leaves, a member misses heartbeats for `session.timeout.ms` (45 s), or a member exceeds `max.poll.interval.ms` (5 min) between polls. That last case means slow processing looks the same as a crash. The classic protocol's eager mode revoked every partition from every member, which stopped the whole group. The fixes:
- **Cooperative sticky assignment** moves only the partitions that change hands.
- **Static membership** (`group.instance.id`) lets a member restart without a rebalance.
- **KIP-848 (GA in 4.0)** moves assignment to the broker, which reconciles members incrementally. Clients opt in with `group.protocol=consumer`. Classic is still the client default, and KIP-1274 plans its deprecation.

**Consumer lag** (log-end offset minus committed offset) is the main health metric. Track it in seconds and watch its trend.

### Share groups: queues for Kafka (KIP-932)

Consumer groups cap parallelism at the partition count, and a slow record blocks its partition. Job-style work needs more workers and acks for each message. **Share groups** allow any number of consumers on the same partitions. The broker leases records under time-limited locks. Consumers accept, release or reject each record, and a delivery-count limit retires poison messages. You give up ordering and get at-least-once delivery. Share groups were early access in 4.0 and became **production-ready in 4.2 (GA, early 2026)**. Use 4.2.1 or later, which fixes a deadlock. Managed-service support varies (⚠ verify).

### Replication, ISR, acks, KRaft

Each partition has RF (replication factor) replicas:
- The **leader** serves writes, and by default reads.
- **Followers** fetch from the leader.
- The **ISR** (in-sync replicas) is the set of replicas within `replica.lag.time.max.ms` (30 s) of the leader.
- Consumers read only up to the **high watermark**, the highest offset every ISR member holds. So a failover can't take back data a consumer has already seen.

| `acks` | Acknowledged when | Lost if |
|---|---|---|
| `0` | the record is sent | anything fails |
| `1` | the leader has written it | the leader dies before followers copy it |
| `all` (default since 3.0) | every current ISR member has it | the whole ISR fails together |

`acks=all` is only as strong as the ISR, which can shrink to just the leader. `min.insync.replicas` sets a floor: below it, writes fail with `NotEnoughReplicas`, so durability wins over availability. The standard setup is **RF=3, min.insync=2, acks=all** across three zones.

**Unclean leader election** (off by default) promotes a stale replica when the whole ISR is gone. That restores availability by truncating data that was already acknowledged, so enable it per topic: perhaps for metrics, never for payments. Kafka acknowledges once data is in the page cache, not after fsync, so durability comes from replicas in separate failure domains.

**KRaft** moved cluster metadata from ZooKeeper into a Raft-replicated internal log (P5, applied to Kafka itself). **3.9 is the last release that supports ZooKeeper. 4.0 (March 2025) runs on KRaft only.**

### Retention vs log compaction

- **Delete policy:** whole closed segments are dropped once they are older than `retention.ms` (default 7 days) or once the partition exceeds `retention.bytes`. Retention ignores whether anyone has read the data, so it must cover your longest replay window.
- **Compact policy:** a background cleaner keeps at least the **latest value per key**, and offsets keep their gaps. A **tombstone** (a record with a null value) deletes its key. Tombstones stay for `delete.retention.ms` (1 day) so slow consumers still see them.

This is **stream–table duality**: a table is the latest value per key of a changelog. Replaying a compacted topic from offset 0 rebuilds the table, much like a Delta table maintained with `MERGE`, except you get it by replay rather than by query. Kafka uses this for `__consumer_offsets`, Connect state and Kafka Streams stores. The active segment is never compacted, so compaction is **not** real-time dedupe.

### Producer semantics and delivery guarantees

- **Batching:** records accumulate per partition until `batch.size` (16 KB) fills or `linger.ms` passes. The `linger.ms` default became 5 ms in 4.0 (⚠ verify). Bigger batches trade latency for throughput and compression (P7).
- **Idempotent producer (default since 3.0, KIP-679):** a lost ack triggers a retry, which would otherwise create a duplicate. The broker assigns a **producer ID**, the producer numbers batches per partition with **sequence numbers**, and the broker drops duplicates, keeping order with up to 5 requests in flight. This covers one producer session only, and an explicit `acks=1` disables it.
- **Transactions:** a stable `transactional.id` lets a producer write to many partitions **and** commit consumer offsets atomically. Commit and abort markers go into each partition, and an epoch **fences zombie** producers. Consumers need `isolation.level=read_committed`, because the default `read_uncommitted` also returns aborted data.

| Semantics | Pattern | On crash |
|---|---|---|
| At-most-once | commit, then process | records lost |
| At-least-once | process, then commit | duplicates |
| Exactly-once (Kafka→Kafka) | consume, transform and produce in one transaction | neither, within Kafka |

EOS (exactly-once semantics) stops at Kafka's boundary. Database writes, API calls and emails sit outside the transaction. End to end you need an **idempotent sink** (an upsert or `MERGE` by key) or a sink that stores offsets atomically with the data (P6, [§11](11-stream-processing.md)).

### Schema Registry and formats

Kafka stores bytes, and producers and consumers deploy independently, so the schema is the contract between them (P8). Without one, a renamed field breaks consumers or produces poison pills. The registry versions schemas per **subject** (by default `<topic>-value`). Serializers prefix each message with a magic byte and a **4-byte schema ID**. Deserializers fetch the schema by ID and cache it. Compatibility is enforced **when a new schema version is registered**:

| Mode | Guarantee | Safe change | Deploy first |
|---|---|---|---|
| BACKWARD (default) | new schema reads previous data | delete field; add field with default | consumers |
| FORWARD | previous schema reads new data | add field; delete field with default | producers |
| FULL | both | add or remove fields with defaults | either |
| `*_TRANSITIVE` | the check runs against **all** prior versions | — | — |

BACKWARD is the default because an upgraded consumer can then rewind to the start of the topic. Use a transitive mode when old versions really get replayed. For formats, **Avro** is compact, evolves through defaults and dominates data platforms. **Protobuf** uses field numbers and suits gRPC shops. **JSON Schema** is readable but larger and evolves more loosely.

### Kafka Connect

Connect replaces each team's hand-written "table→Kafka" and "Kafka→S3" code. **Workers** run **connectors**, which split into **tasks**. In distributed mode the workers form a group and keep configs, offsets and status in compacted topics, so the workers themselves are stateless and tasks fail over to other workers.
- **Source and sink:** source connectors track their position in the external system (for example a binlog offset), and sink connectors use consumer-group offsets.
- **Converters and SMTs:** converters serialize, for example Avro with the registry. **SMTs** (single message transforms) apply stateless per-record changes such as rename, mask, route or set-key. Joins belong in [§11](11-stream-processing.md).
- **DLQ:** sink connectors with `errors.tolerance=all` and `errors.deadletterqueue.topic.name` divert bad records to a dead-letter topic, with the error context in headers.
- **When to use it:** use Connect when a mature connector exists, and write custom code for business-specific or stateful logic. In Databricks terms, Auto Loader plays the same role for files.

### Debezium CDC and the outbox pattern

Polling with `updated_at > :last` misses hard deletes and intermediate updates, and it loads the OLTP database (P1). **Log-based CDC** reads the database's own log instead: the MySQL binlog, or the Postgres WAL via logical decoding. It captures every committed change in commit order at low cost (P5). Debezium works in three steps:
1. **Snapshot** the existing rows (as `op=r` events) and record the log position.
2. **Stream** changes from that position into one topic per table, keyed by **primary key**, so each row's changes stay in order.
3. **Emit an envelope** for each change with `before`, `after`, `source` (table, LSN or binlog position, transaction ID) and `op` (`c`/`u`/`d`/`r`). Deletes are followed by tombstones.

Postgres needs `wal_level=logical`, a **replication slot** and `pgoutput`, plus `REPLICA IDENTITY FULL` for complete `before` images. The classic outage: the connector stops, the slot pins WAL, and the primary's disk fills.

The **outbox pattern** fixes **dual writes**. A service that writes its database and then publishes to Kafka can fail between the two, and 2PC across both isn't practical. Instead, write the business row and an event row in **one local transaction**:

```
BEGIN; UPDATE orders ...; INSERT INTO outbox(event_id, aggregate_id, payload); COMMIT;
  WAL ──> Debezium outbox router ──> "order.events" (key = aggregate_id)
```

The database provides atomicity. Publishing is at-least-once, so consumers dedupe on `event_id` (P6).

### Tiered, diskless, and topics as tables

- **Tiered storage (KIP-405, production-ready in 3.9).** Closed segments move to object storage, and brokers keep only the hot tail locally. Retention is no longer bounded by disk (P2), so long replay is cheap and brokers recover faster. Compacted topics aren't supported.
- **Diskless topics (KIP-1150, accepted March 2026).** Brokers write straight to object storage, which removes cross-AZ replication traffic, a major cloud cost, at the price of latency in the hundreds of milliseconds. The design lives in KIP-1163 and KIP-1164 and isn't in an Apache release yet (⚠ verify). WarpStream and AutoMQ already ship this model.
- **Topics as tables.** Confluent Cloud's **Tableflow** materializes topics as **Iceberg (GA 2025)** and **Delta (GA October 2025, with Unity Catalog sync)** tables. That turns the "Kafka→Spark→bronze" hop into configuration and makes the log and the table two views of one dataset (P5, P11). You still own schema mapping, small files and access policy (P9).

### Failure modes

| Symptom | Usual cause | Response |
|---|---|---|
| Lag on one partition | hot key or stuck consumer | rekey or salt; inspect that consumer |
| Lag on all partitions | slow processing or slow sink | scale to the partition count; batch sink writes |
| Rebalance storm | `max.poll.interval.ms` exceeded; flapping pods | lower `max.poll.records`; static membership; KIP-848 |
| Stuck at one offset | **poison pill** | catch it, route to a DLQ, continue; enforce schemas |
| Data skipped after a restart | offsets expired, so the consumer reset to `latest` | set the reset policy explicitly; keep retention ≥ replay window |
| Broker disk full | time-only retention plus a traffic spike | `retention.bytes`, tiered storage, alerts |
| `RecordTooLargeException` | record over the ~1 MB default (`message.max.bytes`) | claim check: payload in object storage, pointer in Kafka |

### When not to use Kafka

Kafka pays off when you need many independent consumers, replay and high throughput together. It is overhead for a single producer feeding a single consumer, for a simple work queue, or for a team that can't own partitions, schemas and lag (P10). If the need is "OLTP changes in the lakehouse every 15 minutes," managed CDC into files is often enough (P7).

## Comparison

| | Kafka (log) | Classic queue (RabbitMQ, SQS) |
|---|---|---|
| After read | retained | deleted on ack |
| Position kept by | consumer (offset) | broker (per message) |
| Replay / new subscriber | seek / new group, cheap | hard / new queue |
| Ordering | per partition | per queue, weakened by competing consumers |
| Consumer scale-out | ≤ partitions (share groups lift this) | unlimited |

| Platform | Model | Kafka API | Retention | Watch for |
|---|---|---|---|---|
| Apache Kafka (self-run) | brokers with local disks, optional tiering | native | unlimited | you run everything |
| Confluent Cloud | Kafka on proprietary cloud storage; elastic units | native | unlimited | cost; lock-in on Tableflow, Flink and governance |
| Amazon MSK | AWS-managed Apache Kafka; provisioned (incl. Express ⚠ verify) or Serverless | native | configurable; tiered on provisioned | you still size partitions and tune clients |
| Azure Event Hubs | multitenant; TU/PU/CU units; 32 / 100 / 1,024 partitions (Standard / Premium / Dedicated) | protocol endpoint (not on Basic ⚠ verify) | 7 days Standard; 90 days Premium/Dedicated | 1 MB messages; no broker configs; transactions and Kafka Streams in Preview (⚠ verify); compaction GA |
| Amazon Kinesis | shards (~1 MB/s in, 2 MB/s out); provisioned or on-demand (⚠ verify) | no (own SDK/KCL) | 24 h default, up to 365 days (⚠ verify) | per-shard limits; no Kafka ecosystem |
| Apache Pulsar | stateless brokers over BookKeeper segments; built-in tiering, multi-tenancy, geo-replication | via adapter (⚠ verify) | unlimited | two systems to run |
| Redpanda | single C++ binary, Raft per partition, no JVM | compatible | tiered | lags behind the newest KIPs |

## Interview questions

**Q: Can you explain Kafka to me as if I'm a DBA?**

Kafka is the database's WAL taken out of the database and turned into a shared, durable service. Applications append events in order. Any number of readers tail it from their own positions and can rewind, the way a replica replays a log. Partitions hash-partition the log for parallelism, so order holds within a partition but not across partitions. It isn't a query engine. You derive tables from it (P5).

**Q: How does Kafka guarantee ordering?**

Only within a partition. Records route by `hash(key)`, so per-key order holds when three things are true: the key is the entity that needs ordering, idempotence prevents retries from reordering, and the partition count never changes. Global order requires a single partition and therefore a single consumer, so design so you don't need it. Parallel consumer threads can still reorder events, so carry a version field and apply only newer changes.

**Q: How many partitions should a topic have?**

Use the larger of two numbers: target throughput divided by measured per-partition throughput (check the producer and the slowest consumer), or the maximum consumer parallelism you'll need. Add growth headroom, because adding partitions to a keyed topic later remaps keys. Don't overshoot either, since each partition costs memory, replication and recovery time. Most topics land in the tens.

**Q: What happens when a consumer in a group dies?**

Once `session.timeout.ms` passes without a heartbeat, the coordinator reassigns its partitions. The new owner resumes from the last committed offset, so any uncommitted work is redone. That's at-least-once delivery, so sinks must be idempotent. Eager rebalancing pauses the whole group; cooperative assignment and KIP-848 move only the dead member's partitions. A consumer that's alive but slower than `max.poll.interval.ms` gets evicted the same way, which is a classic cause of rebalance storms.

**Q: acks=all with min.insync.replicas=2 and RF=3: what failures can you survive?**

One broker, or one zone if replicas are rack-aware. Leadership fails over, writes continue, and nothing acknowledged is lost, because every acknowledged record was on at least two replicas. With two brokers down, writes stop (`NotEnoughReplicas`), but acknowledged data survives and is still readable. Unclean election would trade that durability for availability. Bad writes and regional outages need cross-cluster replication and backups.

**Q: At-least-once vs exactly-once: how does Kafka implement EOS and what are its limits?**

At-least-once means process, then commit. A crash causes duplicates but never loss, and that's the sane default. EOS combines three pieces: the idempotent producer (a producer ID plus sequence numbers), transactions that atomically commit outputs together with input offsets and fence zombies, and `read_committed` consumers. Its limits: it covers Kafka-to-Kafka processing only, external side effects fall outside it, and it adds latency. End to end, you still need idempotent sinks (P6, [§11](11-stream-processing.md)).

**Q: Log compaction: when and why?**

Use it when a topic holds the current state per key rather than a history: table CDC, reference data, config, or state-store changelogs. A new consumer rebuilds the whole table by replaying from offset 0 without needing infinite retention, and tombstones carry deletes. Don't use it where every event matters. Don't treat it as dedupe either, because the active segment isn't compacted. Tiered storage also excludes compacted topics.

**Q: How do you handle schema evolution?**

Treat the schema as a contract the producer owns (P8): Avro or Protobuf in a registry, compatibility set per subject, checks in CI. BACKWARD means you deploy consumers first, FORWARD means producers first, and FULL allows either order, which suits widely shared topics. For breaking changes such as renames or type changes, add a new field or a versioned topic and dual-publish during migration.

**Q: How would you do CDC from Postgres into the lakehouse?**

I'd use Debezium with `pgoutput` and a replication slot. It takes a snapshot and then streams the WAL to one topic per table, keyed by primary key, in Avro. Land the raw events append-only in bronze, then `MERGE` into silver by key in LSN order, applying deletes ([§11](11-stream-processing.md)). The main risk is a stalled slot pinning WAL and filling the primary's disk, so alert on slot lag. If minutes of latency are acceptable and nothing else consumes the stream, managed CDC to files is simpler (P7, P10).

**Q: What's the outbox pattern?**

It solves dual writes, because a service can't atomically update its database and publish to Kafka. The service writes the business change and an outbox event row in one local transaction. CDC then publishes the committed outbox rows, keyed by aggregate ID. The database provides atomicity; publishing is at-least-once, so consumers dedupe on event ID. The cost is an outbox table to prune and CDC infrastructure to run.

**Q: Kafka vs Event Hubs vs Kinesis: how do you choose?**

Start from the cloud and the team (P10). Event Hubs is Azure-native and gives you a Kafka endpoint with no brokers, but partitions and retention are tier-capped, you can't tune brokers, and newer Kafka features lag. Kinesis is AWS-native and integrates with Lambda and Firehose, but it has its own API and per-shard limits. Choose Kafka (MSK, Confluent or self-run) for Connect, transactions, compaction, long retention or portability. Prefer managed Kafka unless you have a platform team.

**Q: Consumer lag is growing. How do you diagnose it?**

First check whether it's a few partitions (a hot key, a poison pill or a hung consumer) or all of them (a throughput mismatch). Then check rebalances, consumer CPU and the sink. Slow database writes or tiny file commits are the most common cause. Fix it by scaling to the partition count, batching sink writes, rekeying, or sending poison pills to a DLQ. Add partitions only if parallelism really is the ceiling, and judge lag in seconds against the freshness SLA (P7).

**Q: Why did Kafka drop ZooKeeper, and what changes for you?**

ZooKeeper was a second distributed system to run and secure, and controller failover through it was slow, which capped partition counts. KRaft keeps metadata in a Raft-replicated internal log on a controller quorum. That gives faster failover, more partitions and a single system to operate. From 4.0 KRaft is the only mode, so migrate ZooKeeper clusters on 3.x (3.9 is the bridge release) before upgrading.

## Takeaways
- Kafka is a replicated, partitioned, append-only log, and consumers own their position (P5).
- Ordering holds per partition only. Keys pick partitions, and changing the partition count remaps keys.
- Partition count caps consumer parallelism. Size it from throughput plus headroom.
- RF=3 with min.insync=2 and acks=all survives one failure with no loss. Two failures stop writes but still lose nothing.
- EOS covers Kafka-to-Kafka only, so make sinks idempotent (P6).
- Compacted topics are tables; topics with retention are histories.
- BACKWARD means consumers deploy first, FORWARD means producers first, FULL means either order (P8).
- Log-based CDC plus the outbox pattern is how OLTP changes reach the log reliably.
- 4.0 is KRaft-only with KIP-848 GA, share groups went GA in 4.2, tiered storage is GA, and diskless topics are next.

## Sources
- https://kafka.apache.org/blog/2025/03/18/apache-kafka-4.0.0-release-announcement/
- https://kafka.apache.org/blog/2024/11/06/apache-kafka-3.9.0-release-announcement/
- https://kafka.apache.org/blog/2026/02/17/apache-kafka-4.2.0-release-announcement/
- https://kafka.apache.org/blog/2026/06/25/apache-kafka-4.3.1-release-announcement/
- https://kafka.apache.org/42/getting-started/upgrade/
- https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Tiered+Storage+GA+Release+Notes
- https://cwiki.apache.org/confluence/display/KAFKA/KIP-1274:+Deprecate+and+remove+support+for+Classic+rebalance+protocol+in+KafkaConsumer
- https://www.confluent.io/blog/kip-848-consumer-rebalance-protocol/
- https://aiven.io/blog/kip-1150-accepted-and-the-road-ahead
- https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.md
- https://learn.microsoft.com/en-us/azure/Event-Hubs/apache-kafka-configurations
- https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-quotas
- https://learn.microsoft.com/en-us/azure/event-hubs/compare-tiers
- https://learn.microsoft.com/azure/event-hubs/log-compaction
- https://docs.azure.cn/en-us/event-hubs/apache-kafka-transactions
- https://secure.businesswire.com/news/home/20251029878892/en/Confluent-Expands-Tableflow-to-Power-Real-Time-Analytics-and-AI-Across-Clouds
