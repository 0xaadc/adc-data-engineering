# 10. Kafka and Event Streaming, From Zero

> As of October 2026. Tags: **(GA)** generally available · **(Preview)** public/private preview · **(⚠ verify)** not confirmed against official docs.

## Why this exists

Around 2010 LinkedIn had dozens of systems (OLTP databases, search, Hadoop, monitoring), each needing data from several others. With point-to-point pipelines that is N×M custom integrations, each with its own format, retries and backpressure. Classic message queues didn't fix it. They delete a message on acknowledgement, keep per-message state in the broker, and can't serve "a new team wants last week's events."

Kafka's bet is one **distributed, partitioned, replicated, append-only commit log** (P5). Producers append. Records stay for a retention period whether read or not, and each consumer keeps its own position (an *offset*). N×M becomes N+M, producers and consumers decouple in time and rate, and replay becomes routine. In Databricks terms, a partition is the `_delta_log` idea applied to events, and a consumer offset is a Structured Streaming checkpoint.

## Core concepts

### The log, and why it is fast

```
topic "orders", partition 0        offsets are per partition
offset:  0    1    2    3    4    5    6    7   <- append
       [r0] [r1] [r2] [r3] [r4] [r5] [r6] [r7]
                  ^                         ^
  group "lake-ingest" at 2 (lag 6)    group "fraud" at 7 (caught up)
```

Kafka is fast because of how it is built:
- **Sequential I/O.** Writes append to segment files and readers tail them. There are no in-place updates (P1).
- **The page cache.** Consumers that keep up read recent data from OS memory, not disk.
- **End-to-end batching and compression.** The producer compresses a batch, the broker stores and serves it as-is (zero-copy where TLS allows), and the consumer decompresses it.
- **A simple broker.** There is no per-message delivery state. A group's progress is one integer per partition, so another consumer group is nearly free.

### Records, keys and partitions

A record has a **key, a value, headers and a timestamp**. The key and value are opaque bytes, so the schema is the client's problem (P8). The partition is the unit of **ordering, parallelism, replication and placement**. The default partitioner computes `hash(key) % partitions`, so **one key always lands in one partition, in order. There is no ordering across partitions.** Records with a null key are spread across partitions and carry no ordering guarantee. Use as the key the entity whose events must stay ordered, such as `account_id`.

From that formula you can derive:
- **Hot partitions.** A giant key overloads one partition and one consumer, and more partitions won't help. Use a finer key, salt the key (losing per-entity order), or give the giant keys their own topic.
- **Adding partitions remaps keys.** The count can grow but never shrink. When N changes, a key's new events go to a different partition than its old ones. Order breaks at the boundary, and a compacted topic can hold two "latest" values for the same key. Provision headroom up front, or migrate to a new topic.
- **Parallelism ceiling.** Each partition has at most one consumer within a group.
- **Over-partitioning cost.** Every partition adds broker files, memory and replication work, slows recovery, shrinks producer batches and lengthens rebalances. KRaft raised the cluster metadata limit, but the per-broker cost remains.

### Consumer groups, offsets, rebalancing

Consumers sharing a `group.id` divide the partitions among themselves, and extra members sit idle. Separate groups are independent, which is how fan-out works. Progress is stored as **committed offsets** in the compacted internal topic `__consumer_offsets`. Auto-commit, the default every 5 s, commits what was *polled*, not what was *finished*.

Two defaults cause most surprises:
- **`auto.offset.reset=latest`** applies whenever no valid committed offset exists, for example a new group or an offset retention has already deleted. The consumer silently skips the backlog.
- **Offset expiry.** A group with no active members loses its committed offsets after `offsets.retention.minutes` (7 days). A pipeline paused for two weeks restarts at `latest`.

> **Databricks note:** Spark's Kafka source does not commit offsets to Kafka; its checkpoint is the source of truth. Kafka lag tools don't see it, and deleting the checkpoint resets the offsets.

**Rebalances** reassign partitions when a member joins, leaves, misses heartbeats for `session.timeout.ms` (45 s), or exceeds `max.poll.interval.ms` (5 min) between polls, so slow processing looks the same as death. In the **classic** protocol a client-side leader computes the assignment. The original eager mode made everyone revoke everything, so the whole group stopped.

The fixes came in layers:
- **Cooperative sticky assignment** moves only the partitions that change owner.
- **Static membership** (`group.instance.id`) survives quick restarts without a rebalance.
- **KIP-848 (GA in 4.0)** moves assignment to the broker-side coordinator, which reconciles members incrementally with no group-wide barrier. Clients opt in with `group.protocol=consumer`. Classic remains the client default while KIP-1274 plans its deprecation.

**Consumer lag** is the log-end offset minus the committed offset. It is the key health metric. Track it in seconds and alert on its trend.

### Share groups: queues for Kafka (KIP-932)

Consumer groups cap parallelism at the partition count, and one slow record blocks its whole partition. Job-style work needs more workers than that and acknowledgement per message. **Share groups** let any number of consumers read the same partitions. The broker hands out records under time-limited locks. Consumers accept, release or reject each record, and a delivery-count limit eventually removes poison messages. You give up ordering, and delivery is at-least-once. Share groups were early access in 4.0 and became **production-ready in 4.2 (GA)**, released early 2026. Use 4.2.1 or later, which fixes a deadlock. Managed-service support varies (⚠ verify).

### Replication, ISR, acks, KRaft

Each partition has RF replicas: a **leader** that serves writes and, by default, reads, and **followers** that fetch from it. The **ISR** (in-sync replica set) holds the replicas within `replica.lag.time.max.ms` (30 s) of the leader. Consumers read only up to the **high watermark**, the highest offset every ISR member has, so a failover can never take back data they have already seen.

| `acks` | Acknowledged when | Lost if |
|---|---|---|
| `0` | the record is sent | anything fails |
| `1` | the leader has written it | the leader dies before followers copy it |
| `all` (default since 3.0) | every current ISR member has it | all ISR members fail together |

`acks=all` is only as strong as the ISR. If the ISR shrinks to the leader alone, `min.insync.replicas` takes over: below that floor, writes are rejected (`NotEnoughReplicas`), trading availability for durability. The standard setup is **RF=3, min.insync=2, acks=all** across three zones. **Unclean leader election** (default off) promotes a stale replica when every ISR member is gone. It restores availability by truncating acknowledged data, so allow it per topic: perhaps for metrics, never for payments. Kafka acknowledges a write once it reaches the page cache, not after an fsync. Durability comes from replicas in separate failure domains.

**KRaft.** ZooKeeper was a second distributed system to run, and controller failover was slow at high partition counts. KRaft keeps cluster metadata in a Raft-replicated internal log on a controller quorum, which is P5 applied to Kafka itself. **3.9 was the last ZooKeeper-capable release. 4.0 (March 2025) runs on KRaft only**, so you migrate on 3.x before upgrading.

### Retention vs log compaction

- **Delete policy.** Kafka drops closed segments older than `retention.ms` (7 days by default) or beyond `retention.bytes`. Retention ignores whether anyone has read the data, so it must cover your longest replay or backfill.
- **Compact policy.** A cleaner keeps at least the **latest value per key**. Offsets are preserved, so gaps appear. A **tombstone** (null value) deletes a key and lingers for `delete.retention.ms` (1 day) so slow consumers still see it.

This is **stream–table duality**: a table is the latest value per key of a changelog. Replay a compacted topic from offset 0 and you rebuild the table, much like a Delta table maintained by `MERGE`, except you get it by replay rather than query. Kafka uses this itself for `__consumer_offsets`, Connect state and Kafka Streams state stores. The active segment is never compacted, so this is **not** real-time dedupe.

### Producer semantics and delivery guarantees

- **Batching.** Records are batched per partition up to `batch.size` (16 KB) or `linger.ms`, whose default rose from 0 to 5 ms in 4.0 (⚠ verify). Bigger batches give better throughput and compression at the cost of latency (P7).
- **Idempotent producer (default since 3.0, KIP-679).** A lost ack makes the producer retry, which would duplicate the record. Instead, the broker assigns a **producer ID**, the producer numbers each partition's batches with **sequence numbers**, and the broker drops duplicates. Order holds with up to 5 requests in flight. This covers retries within one producer session only, and an explicit `acks=1` disables it.
- **Transactions.** A stable `transactional.id` lets one producer atomically write to many partitions **plus** commit consumer offsets. Commit or abort markers land in each partition, and an epoch **fences zombie** instances. Consumers must set `isolation.level=read_committed`; the default, `read_uncommitted`, also sees aborted data.

| Semantics | Pattern | On crash |
|---|---|---|
| At-most-once | commit, then process | lost records |
| At-least-once | process, then commit | duplicates |
| Exactly-once (Kafka→Kafka) | consume-transform-produce in one transaction | neither, inside Kafka |

EOS (exactly-once semantics) stops at Kafka's boundary. Database writes, API calls and emails fall outside the transaction. End to end you need an **idempotent sink** (an upsert or `MERGE` by key with a version check) or a sink that stores offsets atomically with the data (P6). Pipeline-level exactly-once is covered in [§11](11-stream-processing.md).

### Schema Registry and formats

Kafka sees bytes, and producers and consumers deploy independently, so the schema is the contract (P8). Without one, a renamed field breaks consumers or turns into poison pills. The registry versions schemas per **subject** (`<topic>-value` by default). Serializers prefix each message with a magic byte and a **4-byte schema ID**, and deserializers fetch the schema by ID and cache it. Enforcement happens **at registration**, against the subject's compatibility mode:

| Mode | Guarantee | Safe change | Deploy first |
|---|---|---|---|
| BACKWARD (default) | new schema reads previous data | delete field; add field with default | consumers |
| FORWARD | previous schema reads new data | add field; delete field with default | producers |
| FULL | both | add or remove fields with defaults | either |
| `*_TRANSITIVE` | the same check against **all** prior versions | — | — |

BACKWARD is the default because it lets an upgraded consumer rewind to the start of the topic. Use transitive modes when old versions really get replayed, for example with long retention or compacted topics. Among formats, **Avro** is compact, evolves through defaults and dominates data platforms. **Protobuf** uses field numbers and fits gRPC teams. **JSON Schema** is readable but larger and evolves more loosely.

### Kafka Connect

Without a framework, every team rewrites "table→Kafka" and "Kafka→S3" with its own offset tracking. In Connect, **workers** run **connectors** split into **tasks**. In distributed mode the workers form a group and keep configs, source offsets and status in compacted topics. Workers are therefore stateless, and tasks move to another worker when one fails.
- **Source and sink.** Source connectors track a position in the external system, such as a binlog offset. Sink connectors track consumer-group offsets.
- **Converters and SMTs.** Converters serialize, for example Avro with the registry. **SMTs** (single message transforms) make stateless per-record changes: rename, mask, route, set the key. Joins and aggregations belong in [§11](11-stream-processing.md).
- **DLQ.** For sink connectors, `errors.tolerance=all` plus `errors.deadletterqueue.topic.name` diverts bad records to a dead-letter topic, with the error context in headers.
- **Connect or code?** Use Connect when a mature connector exists. Write code for business-specific, stateful or poorly served integrations. Auto Loader is the file-world analogue: a managed record of what has already been ingested.

### Debezium CDC and the outbox pattern

Polling with `updated_at > :last` misses hard deletes and intermediate updates, and it loads the OLTP database (P1). **Log-based CDC** reads the database's own log, the MySQL binlog or the Postgres WAL via logical decoding, and so captures every committed change in commit order at low cost (P5). Debezium works in three steps:
1. **Snapshot.** It reads existing rows (`op=r`) and records the log position. Incremental snapshots can re-snapshot a table while streaming continues.
2. **Stream.** It continues from that position into one topic per table, **keyed by primary key**, so each row's changes stay ordered.
3. **Envelope.** Each change event carries `before`, `after`, `source` (table, LSN or binlog position, transaction ID) and `op` (`c`/`u`/`d`/`r`). Deletes are followed by a tombstone for compaction.

On Postgres you need `wal_level=logical`, a **replication slot** and `pgoutput`, plus `REPLICA IDENTITY FULL` if you want complete `before` images. The classic outage: the connector stops, the slot pins WAL, and the primary's disk fills up.

**The outbox pattern** solves **dual writes**. Writing the database and then publishing to Kafka can fail halfway, and there is no practical two-phase commit across them. Instead, write the business row and an event row to an `outbox` table in **one local transaction**. Debezium's outbox router then publishes it:

```
BEGIN; UPDATE orders ...; INSERT INTO outbox(event_id, aggregate_id, payload); COMMIT;
  WAL ──> Debezium outbox router ──> "order.events" (key = aggregate_id)
```

The database provides atomicity. Publishing is at-least-once, so consumers dedupe on `event_id` (P6).

### Tiered, diskless, and topics as tables

- **Tiered storage (KIP-405, production-ready in 3.9).** Closed segments move to object storage, and brokers keep only the hot tail locally. Retention is no longer bounded by disk (P2), so long replay becomes cheap and brokers recover faster. Compacted topics aren't supported.
- **Diskless topics (KIP-1150, accepted March 2026).** Brokers write directly to object storage instead of replicating across zones. That removes cross-AZ replication traffic, a major cloud cost, in exchange for higher latency (hundreds of ms). The implementation is specified in KIP-1163 and KIP-1164 and is not yet in an Apache release (⚠ verify). WarpStream and AutoMQ already ship the model.
- **Topics as tables.** Confluent Cloud's **Tableflow** materializes topics as **Iceberg (GA 2025)** and **Delta (GA October 2025, with Unity Catalog sync)** tables. The custom "Kafka→Spark→bronze" hop becomes configuration, and the log and the table become two views of one dataset (P5, P11). You still have to decide who owns schema mapping, small files and access policy (P9).

### Failure modes

| Symptom | Usual cause | Response |
|---|---|---|
| Lag on one partition | hot key or stuck consumer | rekey or salt; inspect that consumer |
| Lag on all partitions | slow processing or sink | scale to the partition count; batch sink writes |
| Rebalance storm | `max.poll.interval.ms` exceeded; flapping pods | fewer `max.poll.records`; static membership; KIP-848 |
| Stuck at one offset | **poison pill** | catch it, send to a DLQ, continue; enforce schemas |
| Data skipped after a restart | offsets expired, so `latest` applied | set the reset policy explicitly; retention ≥ replay need |
| Broker disk full | time-only retention plus a traffic spike | `retention.bytes`, tiered storage, alerts |
| `RecordTooLargeException` | record over ~1 MB (`message.max.bytes`) | claim check: payload in object storage, pointer in Kafka |

### When not to use Kafka

Kafka pays off when you need many independent consumers, replay and high throughput all at once. For one producer and one consumer, a plain work queue, or a team that can't own partitions, schemas and lag, it is overhead (P10). If the actual need is "OLTP changes in the lakehouse every 15 minutes," managed CDC into files is often enough (P7).

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
| Apache Kafka (self-run) | brokers with local disks plus optional tiering | native | unlimited | you run everything |
| Confluent Cloud | Kafka with proprietary cloud storage; elastic units | native | unlimited | cost; lock-in on Tableflow, Flink, governance |
| Amazon MSK | AWS-managed Apache Kafka; provisioned (incl. Express ⚠ verify) or Serverless | native | configurable; tiered on provisioned | you still size partitions, tune clients and plan upgrades |
| Azure Event Hubs | multitenant; TU/PU/CU units; 32 / 100 / 1,024 partitions (Standard / Premium / Dedicated) | protocol endpoint (not Basic ⚠ verify) | 7 days Standard, 90 days Premium/Dedicated | 1 MB messages; no broker configs; transactions and Kafka Streams in Preview (⚠ verify); compaction GA |
| Amazon Kinesis | shards (~1 MB/s in, 2 MB/s out); provisioned or on-demand (⚠ verify) | no (own SDK/KCL) | 24 h default, up to 365 days (⚠ verify) | per-shard limits; no Kafka ecosystem |
| Apache Pulsar | stateless brokers plus BookKeeper segments; built-in tiering, multi-tenancy, geo-replication | via adapter (⚠ verify) | unlimited | two systems to run; smaller ecosystem |
| Redpanda | C++ single binary, Raft per partition, no JVM | compatible | tiered | lags on the newest KIPs |

## Interview questions

**Q: Can you explain Kafka to me as if I'm a DBA?**

Kafka is your redo log or WAL, pulled out of the database and turned into a shared, durable service. Applications append events in order. Any number of readers tail the log from their own position, much as a replica replays the log, and each reader can rewind. Partitions hash-partition the log for parallelism, so order holds within a partition but not across partitions. Kafka isn't a query engine: you derive tables from it rather than query it (P5).

**Q: How does Kafka guarantee ordering?**

Only within a partition. The producer routes by `hash(key)`, so per-key order holds if three things are true: the key is the entity that needs ordering, the idempotent producer prevents retries from reordering, and the partition count never changes. Global order requires a single partition and therefore a single consumer, so design so you don't need it. Parallel consumer threads or out-of-order sink writes can still break ordering downstream, so carry a version and apply only newer values.

**Q: How many partitions should a topic have?**

Size it to the larger of two numbers: target throughput divided by measured per-partition throughput (check both producer and slowest consumer), and the maximum consumer parallelism you'll need. Then add growth headroom, because adding partitions to a keyed topic later remaps keys. Don't overshoot either, since each partition costs memory, replication, recovery time and batch efficiency. Most topics need tens of partitions. Strict global order at low volume means one.

**Q: What happens when a consumer in a group dies?**

It stops heartbeating, so after `session.timeout.ms` the coordinator reassigns its partitions. The new owners resume from the last committed offset, so any work done but not yet committed is redone. That's at-least-once, which is why sinks must be idempotent. Eager rebalancing pauses everyone, while cooperative assignment and KIP-848 move only the dead member's partitions. A live but slow consumer that exceeds `max.poll.interval.ms` is evicted the same way, which is a classic cause of rebalance storms.

**Q: acks=all with min.insync.replicas=2 and RF=3: what failures can you survive?**

You can lose one broker, or one zone if replicas are placed rack-aware. Leadership moves to an in-sync follower, writes continue, and no acknowledged record is lost because each one was on at least two replicas. Lose two brokers and writes stop with `NotEnoughReplicas`, but acknowledged data survives and stays readable. Enabling unclean election would trade that durability for availability. None of this protects against bad writes, deletes or a regional outage: those need cross-cluster replication and backups.

**Q: At-least-once vs exactly-once: how does Kafka implement EOS and what are its limits?**

At-least-once means process, then commit. A crash replays the uncommitted records, giving duplicates but no loss, and it's the sane default. EOS combines three pieces: the idempotent producer (a producer ID plus sequence numbers dedupes retries), transactions (outputs and input offsets committed atomically, with zombie fencing), and `read_committed` consumers. Its limits: it only covers Kafka-to-Kafka processing, external side effects fall outside it, it adds latency, and consumers left on the default `read_uncommitted` see aborted data. End to end, use idempotent sinks (P6, [§11](11-stream-processing.md)).

**Q: Log compaction: when and why?**

Use it when a topic represents current state by key rather than history: table CDC, reference data, configuration, state-store changelogs. A new consumer can rebuild the full table by replaying from offset 0 without infinite retention, and tombstones propagate deletes. Don't use it where every event matters, such as clicks or payments, and don't treat it as dedupe, because the active segment isn't compacted. Tiered storage excludes compacted topics, so size local disks for them.

**Q: How do you handle schema evolution?**

Treat the schema as a contract the producer owns (P8). Keep Avro or Protobuf schemas in a registry, set compatibility per subject, and check it in CI. BACKWARD means consumers upgrade first, FORWARD means producers do, and FULL allows either order, which suits widely shared topics. Additive changes with defaults are safe. Renames and type changes are breaking, so add a new field or a versioned topic and dual-publish during migration. Propagate changes into Delta deliberately rather than auto-merging everything.

**Q: How would you do CDC from Postgres into the lakehouse?**

Run Debezium on `pgoutput` with a replication slot: snapshot first, then stream the WAL into one topic per table, keyed by primary key, in Avro. Land raw change events append-only in bronze for replay and audit. Then `MERGE` into silver by primary key, ordered by LSN, applying deletes ([§11](11-stream-processing.md)). Watch three risks: a stalled slot pins WAL and can fill the primary's disk, replica identity determines how complete `before` images are, and source DDL needs a change process. If minutes of latency are fine and nothing else consumes the stream, managed CDC straight to files is simpler (P7, P10).

**Q: What's the outbox pattern?**

It solves dual writes. A service can't atomically update its database and publish to Kafka, so a crash between the two leaves them disagreeing. Instead, the service writes the business change and an outbox event row in one local transaction. CDC then publishes the committed outbox rows, keyed by aggregate ID so each entity's events stay ordered. Atomicity comes from the database, publishing is at-least-once, and consumers dedupe on event ID. The cost is an outbox table to prune plus CDC infrastructure.

**Q: Kafka vs Event Hubs vs Kinesis: how do you choose?**

Start from the cloud and the team (P10). Event Hubs is Azure-native: a Kafka endpoint with no brokers, plus Capture to ADLS. In exchange, partitions and retention are capped by tier (90 days maximum), you can't tune brokers, and newer Kafka features lag. Kinesis is AWS-native with Lambda and Firehose integration, but it has its own API and per-shard limits. Pick Kafka (MSK, Confluent or self-run) when you need Connect, transactions, compaction, long or tiered retention, or portability, and prefer managed unless you have a platform team.

**Q: Consumer lag is growing. How do you diagnose it?**

First check whether it's a few partitions or all of them. A few points to a hot key or a stuck consumer (poison pill, hung thread). All of them points to a throughput mismatch. Then look at rebalance metrics, consumer CPU, and the sink: slow DB writes or many tiny file commits are the most common cause. Fix by scaling consumers up to the partition count, batching sink writes, rekeying, and sending poison pills to a DLQ. Add partitions only if parallelism really is the ceiling. Report lag in seconds against the freshness SLA (P7).

**Q: Why did Kafka drop ZooKeeper, and what changes for you?**

ZooKeeper was a second distributed system to run and secure, and its controller model made failover slow and capped partition counts. KRaft makes metadata a Raft-replicated internal log on a controller quorum. That gives faster failover, more partitions and one system to operate. Since 4.0 KRaft is the only mode, so you migrate ZooKeeper clusters on 3.x (3.9 is the bridge) before upgrading. On managed services the change is mostly invisible.

## Takeaways
- Kafka is a replicated, partitioned, append-only log, and consumers own their position, so fan-out and replay are cheap (P5).
- Ordering is per partition only. The key picks the partition, and changing the partition count remaps keys.
- Partition count caps consumer parallelism. Size it from throughput plus headroom, and don't overshoot.
- RF=3 with min.insync=2 and acks=all survives one failure with no loss, and survives two with writes stopped.
- Idempotence and acks=all are defaults since 3.0. EOS covers Kafka-to-Kafka only, so make sinks idempotent (P6).
- Compacted topics act as tables, and retention topics act as histories.
- Compatibility mode sets deploy order: BACKWARD means consumers first, FORWARD means producers first, FULL means either (P8).
- Log-based CDC plus the outbox pattern is how OLTP changes reach the log reliably.
- 4.0 is KRaft-only with KIP-848 GA, share groups went GA in 4.2, tiered storage is GA, and diskless is next. Streams and tables are converging.

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
