# 10. Kafka and Event Streaming, From Zero

> As of October 2026. Tags: **(GA)** generally available · **(Preview)** public/private preview · **(⚠ verify)** not confirmed against official docs.

## Why this exists

Around 2010 LinkedIn had dozens of systems (OLTP databases, search, Hadoop, monitoring), and each needed data from several others. Connecting them point to point meant N×M custom pipelines, each with its own format, retry logic and backpressure. Classic message queues didn't solve this: they delete a message once it's acknowledged, track per-message state in the broker, and can't serve a request like "a new team wants last week's events."

Kafka's bet is a single **distributed, partitioned, replicated, append-only commit log** (P5):
- Producers append to the log.
- Records stay for a retention period whether anyone reads them or not.
- Each consumer keeps its own position in the log, called an *offset*.

N×M integrations become N+M. Producers and consumers decouple in time and in rate, and replay becomes a routine operation. In Databricks terms, a partition is the `_delta_log` idea applied to events, and a consumer offset plays the role of a Structured Streaming checkpoint.

## Core concepts

### The log, and why it is fast

```
topic "orders", partition 0        offsets are per partition
offset:  0    1    2    3    4    5    6    7   <- append
       [r0] [r1] [r2] [r3] [r4] [r5] [r6] [r7]
                  ^                         ^
  group "lake-ingest" at 2 (lag 6)    group "fraud" at 7 (caught up)
```

Kafka's speed comes from its design:
- **Sequential I/O.** The broker appends to segment files and consumers tail them. Nothing is updated in place (P1).
- **The page cache does the caching.** Consumers that keep up read recent data from OS memory instead of disk.
- **Batches stay batches end to end.** The producer compresses a batch, the broker stores and serves it unchanged (zero-copy where TLS allows), and the consumer decompresses it.
- **The broker is simple.** It keeps no per-message delivery state. A consumer group's entire progress is one integer per partition, so adding another group costs almost nothing.

### Records, keys and partitions

A record carries a **key, value, headers and timestamp**. Kafka treats the key and value as opaque bytes, so the schema is the client's responsibility (P8). The partition is the unit of **ordering, parallelism, replication and placement**.

The default partitioner computes `hash(key) % partitions`. As a result, **all records with the same key land in the same partition, in order, and there is no ordering across partitions.** Records with a null key are spread across partitions with no ordering at all. Choose as the key the entity whose events must stay in order, for example `account_id`.

Several consequences follow from that formula:
- **Hot partitions.** A very large key overloads one partition and therefore one consumer, and adding partitions won't help. The options are a finer-grained key, salting the key (which gives up per-entity order), or a separate topic for the heavy keys.
- **Adding partitions remaps keys.** The partition count can grow but never shrink. When it changes, new events for an existing key go to a different partition than the old ones. Order breaks at that boundary, and a compacted topic can end up with two "latest" values for the same key. Size with headroom up front, or migrate to a new topic.
- **Parallelism ceiling.** Within a consumer group, each partition has at most one consumer.
- **Over-partitioning has a cost.** Each partition adds broker files, memory and replication work. Too many partitions also slow recovery, shrink producer batches and lengthen rebalances.

### Consumer groups, offsets, rebalancing

Consumers that share a `group.id` divide the partitions among themselves, and extra members sit idle. Separate groups are fully independent, and that independence is what makes fan-out work. Each group records its progress as **committed offsets** in the compacted internal topic `__consumer_offsets`. Auto-commit, on by default, commits every 5 s whatever the consumer has *polled*, not what it has *finished*.

Two defaults cause most surprises:
- `auto.offset.reset=latest` applies whenever there is no valid committed offset (a new group, or an offset that retention has deleted). The consumer silently skips the backlog.
- A group with no active members loses its offsets after `offsets.retention.minutes` (default 7 days). A pipeline paused for two weeks restarts at `latest`.

> **Databricks note:** Spark's Kafka source does not commit offsets to Kafka. Its checkpoint is the source of truth. Kafka lag tools can't see it, and deleting the checkpoint resets the offsets.

A **rebalance** reassigns partitions when a member joins or leaves. A member also counts as gone if it misses heartbeats for `session.timeout.ms` (45 s) or goes longer than `max.poll.interval.ms` (5 min) between polls, so slow processing looks exactly like a crash. The classic protocol's eager mode made every member revoke all its partitions, which stopped the whole group. Fixes arrived in layers:
- **Cooperative sticky assignment** moves only the partitions that change owner.
- **Static membership** (`group.instance.id`) lets a member restart quickly without triggering a rebalance.
- **The KIP-848 protocol (GA in 4.0)** computes assignments on the broker and reconciles members incrementally, with no group-wide barrier. Clients opt in with `group.protocol=consumer`. Classic is still the client default, and KIP-1274 plans its deprecation.

**Consumer lag** is the log-end offset minus the committed offset, and it is the main health metric. Track it in seconds and alert on its trend.

### Share groups: queues for Kafka (KIP-932)

Consumer groups cap parallelism at the partition count, and a single slow record blocks its whole partition. Job-style work needs more workers than partitions and acknowledgement per message. **Share groups** let any number of consumers read the same partitions:
- The broker hands out records under time-limited locks.
- Each consumer accepts, releases or rejects every record it receives.
- A delivery-count limit eventually retires poison messages.

The trade is no ordering and at-least-once delivery. Share groups were early access in 4.0 and became **production-ready in 4.2 (GA, early 2026)**. Use 4.2.1 or later, which fixes a deadlock. Support on managed services varies (⚠ verify).

### Replication, ISR, acks, KRaft

Each partition has RF replicas. One **leader** serves writes, and by default reads too. **Followers** fetch from the leader. The **ISR** (in-sync replicas) is the set of replicas no more than `replica.lag.time.max.ms` (30 s) behind. Consumers read only up to the **high watermark**, the highest offset that every ISR member has. That way, a failover never takes back data a consumer has already seen.

| `acks` | Acknowledged when | Lost if |
|---|---|---|
| `0` | the record is sent | anything fails |
| `1` | the leader has written it | the leader dies before followers copy it |
| `all` (default since 3.0) | all current ISR members have it | all ISR members fail together |

`acks=all` is only as strong as the ISR, which can shrink to the leader alone. `min.insync.replicas` sets a floor: below it, writes are rejected with `NotEnoughReplicas`, so durability wins over availability. The standard configuration is **RF=3, min.insync=2, acks=all**, spread across three zones.

**Unclean leader election** (off by default) promotes a stale replica when no ISR member is left. That restores availability by truncating data that was already acknowledged, so enable it only per topic: maybe for metrics, never for payments. Note also that Kafka acknowledges a write once it reaches the page cache, not after an fsync, so durability comes from having copies in separate failure domains.

**KRaft** replaced ZooKeeper by storing cluster metadata in a Raft-replicated internal log, which is P5 applied to Kafka itself. **3.9 is the last release that supports ZooKeeper, and 4.0 (March 2025) runs on KRaft only.**

### Retention vs log compaction

- **Delete policy:** whole closed segments are dropped once they're older than `retention.ms` (default 7 days) or the partition exceeds `retention.bytes`. Retention ignores whether anyone has read the data, so it must cover your longest replay or backfill.
- **Compact policy:** a background cleaner keeps at least the **latest value per key**. Offsets are preserved, so gaps appear. A **tombstone** (a record with a null value) deletes its key, and the tombstone itself stays for `delete.retention.ms` (1 day) so slow consumers still see the delete.

This is **stream–table duality**: a table is the latest value per key of a changelog. Replay a compacted topic from offset 0 and you rebuild the table. It works like a Delta table kept current by `MERGE`, except you obtain the table by replaying rather than by querying. Kafka itself relies on this for `__consumer_offsets`, Connect's state and Kafka Streams state stores. Compaction never touches the active segment, so it is **not** real-time dedupe.

### Producer semantics and delivery guarantees

- **Batching.** The producer collects records per partition until `batch.size` (16 KB) fills or `linger.ms` expires. The `linger.ms` default became 5 ms in 4.0 (⚠ verify). Larger batches improve throughput and compression at the cost of latency (P7).
- **Idempotent producer (default since 3.0, KIP-679).** A lost ack triggers a retry, which would otherwise duplicate the record. The broker assigns each producer a **producer ID**, the producer numbers its batches per partition with **sequence numbers**, and the broker drops duplicates. Ordering holds with up to 5 requests in flight. This only covers one producer session, and an explicit `acks=1` turns it off.
- **Transactions.** A producer with a stable `transactional.id` can write to many partitions **and** commit consumer offsets as one atomic unit. Commit or abort markers land in each partition, and an epoch **fences zombie** producers. Consumers need `isolation.level=read_committed`. The default, `read_uncommitted`, also returns aborted data.

| Semantics | Pattern | Effect of a crash |
|---|---|---|
| At-most-once | commit, then process | records lost |
| At-least-once | process, then commit | duplicates |
| Exactly-once (Kafka→Kafka) | consume, transform and produce inside one transaction | neither, inside Kafka |

Exactly-once semantics (EOS) end at Kafka's boundary: database writes, API calls and emails are not part of the transaction. End to end you need an **idempotent sink** (upsert or `MERGE` by key) or a sink that stores offsets atomically with the data (P6, [§11](11-stream-processing.md)).

### Schema Registry and formats

Kafka stores bytes, and producers and consumers are deployed independently, so the schema is the contract between them (P8). Without enforcement, a renamed field breaks consumers or turns into poison pills.

A schema registry versions schemas per **subject** (by default `<topic>-value`):
- Serializers prefix each message with a magic byte and a **4-byte schema ID**.
- Deserializers fetch the schema by that ID and cache it.
- The compatibility check runs **at registration time**, against the subject's mode.

| Mode | Guarantee | Safe change | Deploy first |
|---|---|---|---|
| BACKWARD (default) | the new schema can read data written with the previous one | delete a field; add a field with a default | consumers |
| FORWARD | the previous schema can read data written with the new one | add a field; delete a field that has a default | producers |
| FULL | both directions | add or remove fields that have defaults | either side |
| `*_TRANSITIVE` | the same check, against **all** prior versions | — | — |

BACKWARD is the default because an upgraded consumer can then rewind to the start of the topic. Use a transitive mode when old versions really get replayed. As for formats:
- **Avro** is compact, evolves through defaults, and is the most common choice in data platforms.
- **Protobuf** uses field numbers and suits teams already on gRPC.
- **JSON Schema** is readable but larger, with looser evolution rules.

### Kafka Connect

Without Connect, every team writes its own "table→Kafka" or "Kafka→S3" code with its own offset tracking. Connect standardizes that. **Workers** run **connectors**, which are split into **tasks**. In distributed mode the workers form a group and store configs, source offsets and status in compacted topics. Workers therefore hold no state of their own, and tasks move to another worker when one fails.
- **Source and sink.** A source connector tracks its own position in the external system (for example, a binlog offset). A sink connector tracks consumer-group offsets.
- **Converters and SMTs.** Converters handle serialization, such as Avro with the registry. **SMTs** (single message transforms) make stateless, per-record changes: rename, mask, route or set the key. Joins and aggregations belong in a stream processor ([§11](11-stream-processing.md)).
- **Dead-letter queue.** For sink connectors, setting `errors.tolerance=all` and `errors.deadletterqueue.topic.name` diverts bad records to a DLQ topic, with the error context in headers.
- **Connect or code?** Use Connect when a mature connector exists. Write code for business-specific or stateful logic. Auto Loader is the closest Databricks analogue: a managed record of what has already been ingested.

### Debezium CDC and the outbox pattern

Polling with `updated_at > :last` misses hard deletes and intermediate updates, and it puts load on the OLTP database (P1). **Log-based CDC** reads the database's own log instead (the MySQL binlog, or the Postgres WAL through logical decoding). It captures every committed change, in commit order, at low cost (P5). Debezium works in three steps:
1. **Snapshot:** read the existing rows (emitted as `op=r`) and record the log position.
2. **Stream:** continue from that log position into one topic per table, **keyed by primary key**, so each row's changes stay in order.
3. **Envelope:** each event carries `before`, `after`, `source` (table, LSN or binlog position, transaction ID) and `op` (`c`/`u`/`d`/`r`). Every delete is followed by a tombstone so compacted topics drop the key.

On Postgres this requires `wal_level=logical`, a **replication slot** and the `pgoutput` plugin. Full `before` images also need `REPLICA IDENTITY FULL`. The classic outage: the connector stops, the slot pins WAL, and the primary database's disk fills up.

The **outbox pattern** solves **dual writes**. A service that writes to its database and then publishes to Kafka can fail between the two steps, and there's no practical two-phase commit (2PC) across both systems. Instead, the service writes the business row and an event row to an `outbox` table in **one local transaction**:

```
BEGIN; UPDATE orders ...; INSERT INTO outbox(event_id, aggregate_id, payload); COMMIT;
  WAL ──> Debezium outbox router ──> "order.events" (key = aggregate_id)
```

The database provides atomicity. Publishing is at-least-once, so consumers deduplicate on `event_id` (P6).

### Tiered, diskless, and topics as tables

- **Tiered storage (KIP-405, production-ready in 3.9).** Closed segments move to object storage, and brokers keep only the hot tail on local disk. Retention is no longer bounded by disk (P2), so long replay windows are cheap and brokers recover faster. Compacted topics are not supported.
- **Diskless topics (KIP-1150, accepted March 2026).** Brokers write directly to object storage, which removes cross-AZ replication traffic, a major cloud cost, in exchange for latency in the hundreds of milliseconds. The implementation is specified in KIP-1163 and KIP-1164 and is not yet in an Apache Kafka release (⚠ verify). WarpStream and AutoMQ already ship this model.
- **Topics as tables.** Confluent Cloud's **Tableflow** materializes topics as **Iceberg (GA 2025)** and **Delta (GA October 2025, with Unity Catalog sync)** tables. The hand-built "Kafka→Spark→bronze" hop becomes configuration, and the log and the table become two views of the same data (P5, P11). You still have to own schema mapping, small-file management and access policy (P9).

### Failure modes

| Symptom | Usual cause | Response |
|---|---|---|
| Lag on one partition | hot key, or a stuck consumer | rekey or salt; inspect that consumer |
| Lag on all partitions | slow processing or a slow sink | scale up to the partition count; batch sink writes |
| Rebalance storm | `max.poll.interval.ms` exceeded; pods flapping | lower `max.poll.records`; static membership; KIP-848 |
| Consumer stuck at one offset | **poison pill** (a record it can't process) | catch it, send it to a DLQ, keep going; enforce schemas |
| Data skipped after a restart | offsets expired, so `latest` applied | set the reset policy explicitly; retention ≥ replay window |
| Broker disk full | time-only retention plus a traffic spike | `retention.bytes`, tiered storage, disk alerts |
| `RecordTooLargeException` | record exceeds ~1 MB (`message.max.bytes`) | claim check: put the payload in object storage, send a pointer |

### When not to use Kafka

Kafka pays for itself when you need many independent consumers, replay and high throughput together. It is overhead for one producer feeding one consumer, for a plain work queue, or for a team that can't own partitions, schemas and lag (P10). If the real need is "OLTP changes in the lakehouse every 15 minutes," managed CDC into files is often enough (P7).

## Comparison

| | Kafka (log) | Classic queue (RabbitMQ, SQS) |
|---|---|---|
| After a read | retained | deleted on ack |
| Position kept by | consumer (offset) | broker (per message) |
| Replay / new subscriber | seek / new group, both cheap | hard / needs a new queue |
| Ordering | per partition | per queue, weakened by competing consumers |
| Consumer scale-out | up to the partition count (share groups lift this) | unlimited |

| Platform | Model | Kafka API | Retention | Watch for |
|---|---|---|---|---|
| Apache Kafka (self-run) | brokers with local disks, optional tiering | native | unlimited | you operate everything |
| Confluent Cloud | Kafka on a proprietary cloud storage layer; elastic units | native | unlimited | cost; lock-in on Tableflow, Flink and governance features |
| Amazon MSK | AWS-managed Apache Kafka; provisioned (including Express ⚠ verify) or Serverless | native | configurable; tiered storage on provisioned | you still size partitions, tune clients and plan upgrades |
| Azure Event Hubs | multitenant service; TU/PU/CU capacity units; 32 / 100 / 1,024 partitions (Standard / Premium / Dedicated) | protocol endpoint (not on Basic ⚠ verify) | 7 days Standard, 90 days Premium/Dedicated | 1 MB messages; no broker configs; transactions and Kafka Streams in Preview (⚠ verify); compaction GA |
| Amazon Kinesis | shards (~1 MB/s in, 2 MB/s out); provisioned or on-demand (⚠ verify) | no (own SDK/KCL) | 24 h default, up to 365 days (⚠ verify) | per-shard limits; no Kafka ecosystem |
| Apache Pulsar | stateless brokers over BookKeeper segments; built-in tiering, multi-tenancy, geo-replication | via an adapter (⚠ verify) | unlimited | two systems to run; smaller ecosystem |
| Redpanda | single C++ binary, Raft per partition, no JVM | compatible | tiered | trails on the newest KIPs |

## Interview questions

**Q: Can you explain Kafka to me as if I'm a DBA?**

It's your WAL pulled out of the database and turned into a shared, durable service. Applications append events in order, and any number of readers tail the log from their own position and can rewind, the way a replica replays the log. Partitions hash-partition the log for parallelism, so order holds within a partition but not across partitions. It isn't a query engine. You derive tables from it (P5).

**Q: How does Kafka guarantee ordering?**

Only within a partition. Records are routed by `hash(key)`, so per-key order holds as long as three things are true: the key is the entity you need ordered, the idempotent producer keeps retries from reordering, and the partition count never changes. Global ordering requires a single partition and a single consumer, so design so you don't need it. Parallel consumer threads or sinks that write out of order can still break ordering, so carry a version and apply only newer changes.

**Q: How many partitions should a topic have?**

Take the larger of two numbers: target throughput divided by measured per-partition throughput (for both the producer and the slowest consumer), or the maximum consumer parallelism you need. Add growth headroom, because adding partitions to a keyed topic remaps keys. Don't overshoot, since every partition costs memory, replication and recovery time. For most topics that comes out in the tens.

**Q: What happens when a consumer in a group dies?**

After `session.timeout.ms` with no heartbeat, the coordinator reassigns its partitions. The new owners resume from the last committed offset, so any processed but uncommitted work is redone. That's at-least-once delivery, which is why sinks must be idempotent. Eager rebalancing pauses every consumer, while cooperative assignment and KIP-848 move only the dead member's partitions. A consumer that is alive but slower than `max.poll.interval.ms` gets evicted the same way, which is a classic cause of rebalance storms.

**Q: acks=all with min.insync.replicas=2 and RF=3: what failures can you survive?**

You can lose one broker (or one zone, with rack-aware placement): leadership fails over, writes continue, and no acknowledged record is lost because each one was on at least two replicas. Lose two brokers and writes stop with `NotEnoughReplicas`, but the acknowledged data survives and can still be read. Turning on unclean election would trade that durability for availability. None of this protects against bad writes or a regional outage. Those need cross-cluster replication and backups.

**Q: At-least-once vs exactly-once: how does Kafka implement EOS and what are its limits?**

At-least-once means process, then commit: you get duplicates after a crash but never loss, and it's the sane default. EOS combines three pieces: the idempotent producer (producer ID plus sequence numbers), transactions that commit outputs and input offsets atomically and fence zombie producers, and `read_committed` consumers. The limits are that it covers only Kafka-to-Kafka processing, external side effects fall outside it, and it adds latency. End-to-end correctness still needs idempotent sinks (P6, [§11](11-stream-processing.md)).

**Q: Log compaction: when and why?**

Use compaction when a topic holds current state by key rather than history: table CDC, reference data, configuration, state-store changelogs. A new consumer can rebuild the whole table by replaying from offset 0 without infinite retention, and tombstones carry deletes. Don't use it where every event matters, and don't treat it as dedupe, because the active segment is never compacted. Tiered storage excludes compacted topics.

**Q: How do you handle schema evolution?**

Treat the schema as a contract owned by the producer (P8). That means Avro or Protobuf in a registry, compatibility set per subject, and checks in CI. BACKWARD means consumers deploy first, FORWARD means producers deploy first, and FULL allows either order, which is the right choice for widely shared topics. For breaking changes such as renames or type changes, add a new field or a versioned topic and dual-publish during the migration.

**Q: How would you do CDC from Postgres into the lakehouse?**

Run Debezium with `pgoutput` and a replication slot: it takes a snapshot, then streams the WAL into one topic per table, keyed by primary key, as Avro. Land the raw change events append-only in bronze, then `MERGE` into silver by key, ordered by LSN, applying deletes ([§11](11-stream-processing.md)). The main risk is a stalled slot pinning WAL and filling the primary's disk, so alert on slot lag. If minutes of latency are fine and nothing else consumes the stream, managed CDC straight to files is simpler (P7, P10).

**Q: What's the outbox pattern?**

It solves the dual-write problem: a service can't atomically update its database and publish to Kafka. Instead, it writes the business change and an outbox event row in one local transaction, and CDC publishes the committed outbox rows keyed by aggregate ID. Atomicity comes from the database. Publishing is at-least-once, so consumers deduplicate on event ID. The cost is an outbox table to prune and CDC infrastructure to run.

**Q: Kafka vs Event Hubs vs Kinesis: how do you choose?**

Start from the cloud and the team (P10). Event Hubs is Azure-native and gives you a Kafka endpoint with no brokers to run, but partitions and retention are capped by tier, you can't tune brokers, and newer Kafka features arrive late. Kinesis is AWS-native with tight Lambda and Firehose integration, but it has its own API and per-shard limits. Pick Kafka (MSK, Confluent or self-run) when you need Connect, transactions, compaction, long retention or portability, and prefer a managed service unless you have a platform team.

**Q: Consumer lag is growing. How do you diagnose it?**

First ask whether the lag is on a few partitions (hot key, poison pill, hung consumer) or on all of them (a throughput mismatch). Then check rebalance metrics, consumer CPU and the sink. Slow database writes or tiny file commits are the most common cause. Fix by scaling up to the partition count, batching sink writes, rekeying, or sending poison pills to a DLQ. Add partitions only if parallelism is genuinely the ceiling, and judge lag in seconds against the freshness SLA (P7).

**Q: Why did Kafka drop ZooKeeper, and what changes for you?**

ZooKeeper was a second distributed system to run and secure, and controller failover through it was slow, which capped partition counts. KRaft stores metadata in a Raft-replicated internal log on a controller quorum, giving faster failover, more partitions and one system to operate. Since 4.0, KRaft is the only mode, so ZooKeeper clusters must migrate on 3.x (3.9 is the bridge) before upgrading.

## Takeaways
- Kafka is a replicated, partitioned, append-only log, and consumers own their position (P5).
- Ordering is per partition. The key picks the partition, and changing the partition count remaps keys.
- Partition count caps consumer parallelism. Size it from throughput plus headroom.
- RF=3 with min.insync=2 and acks=all survives one failure with no data loss and two failures with writes stopped.
- EOS covers Kafka-to-Kafka only. Make sinks idempotent (P6).
- Compacted topics are tables. Topics with retention are histories.
- BACKWARD means consumers deploy first, FORWARD means producers deploy first, FULL means either (P8).
- Log-based CDC plus the outbox pattern gets OLTP changes onto the log reliably.
- 4.0 is KRaft-only and makes KIP-848 GA, 4.2 brings share groups to GA, tiered storage is GA, and diskless is next.

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
