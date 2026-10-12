# 10. Kafka and Event Streaming, From Zero

> As of October 2026. Tags: **(GA)** generally available · **(Preview)** public/private preview · **(⚠ verify)** not confirmed against official docs.

## Why this exists

Around 2010 LinkedIn had dozens of systems (OLTP databases, search, the Hadoop warehouse, monitoring) that each needed data from several others. Point-to-point pipelines grow as N×M. Every new source or destination needed custom code with its own format, retries and backpressure, and a slow consumer could stall a producer. Traditional message queues didn't solve it. They delete a message once it is acknowledged, track delivery state per message in the broker, degrade as backlogs grow, and can't answer "a new team wants the last 7 days of events."

Kafka's bet was to replace the web of pipes with one **distributed, partitioned, replicated, append-only commit log** (P5). Producers append. The log keeps records for a retention period whether or not anyone has read them. Each consumer stores its own position (an *offset*) and reads at its own pace. This turns N×M integrations into N+M, decouples producers from consumers in time and rate, and makes replay a normal operation instead of a recovery project. In Databricks terms, a partition applies the `_delta_log` idea to events, and a consumer's offset plays the role of a Structured Streaming checkpoint.

## Core concepts

### The log, and why it is fast

```
topic "orders", partition 0   (append-only; offsets are per partition)
offset:   0    1    2    3    4    5    6    7   <- producers append here
        [r0] [r1] [r2] [r3] [r4] [r5] [r6] [r7]
                   ^                         ^
   group "lake-ingest" committed=2     group "fraud" committed=7
   (lag = 6)                           (caught up)
```

A partition is a sequence of segment files. Kafka's speed comes from its structure, not from clever code:
- **Sequential I/O only.** Appends and tailing reads are sequential. There are no B-trees and no in-place updates (P1, write-optimized).
- **The OS page cache is the cache.** Recent writes are still in memory, so consumers that keep up read from RAM. Kafka avoids a large JVM heap cache.
- **End-to-end batching and compression.** The producer compresses a batch. The broker stores and serves it unchanged, and the consumer decompresses it. Brokers can hand file bytes to the socket without copying them through user space (zero-copy; TLS removes most of this benefit).
- **Dumb broker, smart client.** The broker keeps no per-message delivery state. A consumer group's entire progress is one integer per partition, so an extra consumer group costs almost nothing.

### Records, keys and partitions

A record has a **key, value, headers and timestamp**. Kafka treats the key and value as opaque bytes, so meaning (the schema) is the client's concern (P8). Topics are split into partitions. The partition is the unit of **ordering, parallelism, replication and placement**.

The default partitioner hashes the key (murmur2) modulo the partition count. **All records with the same key land in the same partition, in order. There is no ordering across partitions.** Records with a null key are spread across partitions with "sticky" batching and have no ordering. Pick as the key the entity whose events must stay ordered, such as `account_id` or `order_id`.

Several consequences follow from `hash(key) % N`:
- **Hot partitions.** One huge key (a marketplace's biggest seller) lands on one partition and therefore one consumer, and adding partitions doesn't help. The fixes are a finer key (which loses per-entity order), salting (key plus a bucket, merged downstream), or a separate topic for the outsized keys.
- **Adding partitions to a keyed topic remaps keys.** You can increase the partition count but never decrease it. When N changes, a key's new events go to a different partition than its old ones. Ordering breaks across the change, and a compacted topic ends up with two "latest" values for some keys. Size the count up front with headroom, or create a new topic and migrate.
- **Partition count is the parallelism ceiling.** Within a group, each partition has at most one consumer.
- **Too many partitions cost you.** Each one adds broker files and memory and replication traffic, slows recovery, shrinks producer batches (worse compression) and lengthens rebalances. KRaft raised the cluster-wide metadata ceiling, but the per-broker costs remain.

### Consumer groups, offsets and rebalancing

Consumers that share a `group.id` split the topic's partitions between them. Each partition goes to exactly one member, and any extra members sit idle. Different groups are completely independent, which is how fan-out works.

Progress is stored as **committed offsets** in the internal compacted topic `__consumer_offsets`, which is itself a log (P5). By default the client auto-commits every 5 s whatever it has *polled*, not what it has *finished*, and this is where delivery semantics come from. Two defaults cause most surprises:
- `auto.offset.reset` defaults to `latest` and applies whenever there is no valid committed offset: a new group, or an offset that retention has already deleted. `latest` silently skips the backlog, and `earliest` reprocesses everything.
- A group with no active members loses its committed offsets after `offsets.retention.minutes` (default 7 days). A pipeline paused for two weeks restarts at `latest` and loses data without raising an error.

> **Databricks note:** Spark's Kafka source does not commit offsets back to Kafka. Progress lives in the streaming checkpoint. Kafka lag tools won't see a Spark consumer unless you export its progress yourself, and deleting the checkpoint amounts to an offset reset.

A **rebalance** reassigns partitions when a member joins or leaves. A member also counts as gone if it misses heartbeats for `session.timeout.ms` (default 45 s) or goes longer than `max.poll.interval.ms` (default 5 min) between polls, so slow processing looks like a crash. In the **classic protocol** a client-side group leader computes the assignment. The original "eager" strategy made every member revoke all its partitions first, so the whole group stopped. The fixes came in layers:
- **Cooperative sticky assignment** moves only the partitions that change owner.
- **Static membership** (`group.instance.id`) lets a restarting pod rejoin without triggering a rebalance.
- **The new consumer group protocol (KIP-848, GA in 4.0)** moves assignment to the broker-side coordinator. Members reconcile incrementally through heartbeats, with no group-wide barrier. Brokers enable it by default. Java clients opt in with `group.protocol=consumer`, and classic remains the client default while KIP-1274 plans its deprecation.

**Consumer lag** (log-end offset minus committed offset, per partition) is the key health metric. Express it in time ("seconds behind") as well as in messages, and alert on its trend.

### Share groups: queues for Kafka (KIP-932)

Consumer groups tie parallelism to the partition count and suffer head-of-line blocking: one slow record stalls its whole partition. Job-style work (send an email, call an API) needs more workers than partitions and acknowledgement per message.

**Share groups** let any number of consumers read the same partitions. The broker hands out records under a time-limited lock. The consumer then acknowledges each record: accept, release (for redelivery) or reject. The broker tracks per-record state and a delivery count, so a poison message eventually gets dropped instead of retried forever. You give up ordering, and delivery is at-least-once.

Status: early access in 4.0, then **production-ready in 4.2 (GA)**, released early 2026. Run 4.2.1 or later, which fixes a share-group deadlock. Support in managed services lags behind (⚠ verify for your provider).

### Replication, ISR and acks

Each partition has RF replicas. One **leader** handles writes and, by default, reads. **Followers** fetch from the leader the same way consumers do. The **ISR** (in-sync replicas) is the set of replicas caught up within `replica.lag.time.max.ms` (30 s). The **high watermark** is the highest offset copied to every ISR member. Consumers read only up to it, so they never see a record that a failover could erase.

| Producer `acks` | Acknowledged when | Data lost if |
|---|---|---|
| `0` | the record is sent | anything fails |
| `1` | the leader has written it | the leader dies before followers copy it |
| `all` (default since 3.0) | every current ISR member has it | every ISR member fails together |

`acks=all` alone is weak once the ISR has shrunk to just the leader. `min.insync.replicas` sets a floor. Below it, the broker rejects `acks=all` writes (`NotEnoughReplicas`), giving up availability to keep durability. The standard pattern is **RF=3, min.insync.replicas=2, acks=all**, spread across three zones.

**Unclean leader election** (`unclean.leader.election.enable`, default false) applies when every ISR member is gone. Promoting a stale replica restores the partition but truncates records that were already acknowledged. Make the choice per topic: tolerable for metrics, never for payments.

Kafka acknowledges a write once it reaches the page cache, not after an fsync. Durability comes from copies in independent failure domains, not from the disk.

**KRaft.** Cluster metadata used to live in ZooKeeper, a second distributed system to operate, and controller failover was slow at high partition counts. KRaft stores metadata in a Raft-replicated internal log run by a small controller quorum, which is P5 applied to Kafka itself. **3.9 was the last release that supports ZooKeeper. 4.0 (March 2025) runs on KRaft only.** A ZooKeeper cluster must migrate to KRaft on 3.x before it can upgrade to 4.x.

### Retention vs log compaction

- **Delete policy.** Kafka drops whole closed segments once they are older than `retention.ms` (default 7 days) or once the partition exceeds `retention.bytes`. Retention ignores whether anyone has read the data, so if backfills need 30 days, retention must be at least 30 days.
- **Compact policy.** A background cleaner keeps at least the **latest record per key** and removes older ones. Offsets are preserved, so gaps appear. A **tombstone** (a key with a null value) deletes the key. Tombstones are kept for `delete.retention.ms` (1 day) so slow consumers still see the delete. You can combine both policies with `compact,delete`.

This is **stream–table duality**: a table is the latest value per key of a changelog, and a changelog is a table's history. Replaying a compacted topic from offset 0 rebuilds the table. Kafka relies on this for `__consumer_offsets`, Connect's state and Kafka Streams state stores, and it fits CDC and reference data well. In Databricks terms, it resembles a Delta table maintained by `MERGE` on key, except that you get the table by replaying, not by querying. The active segment is never compacted, so compaction is **not** real-time dedupe.

### Producer: batching, idempotence, transactions

**Batching.** Records accumulate per partition until `batch.size` (16 KB) fills or `linger.ms` expires. The `linger.ms` default rose from 0 to 5 ms in 4.0 (⚠ verify). Longer lingering gives bigger batches, better compression and more throughput, at the price of latency (P7).

**Idempotent producer (on by default since 3.0, KIP-679).** Retries create duplicates: the leader writes a record, the ack is lost, and the producer resends. To prevent this, the broker assigns each producer a **producer ID**, and the producer numbers its batches per partition with **sequence numbers**. The broker discards duplicates and rejects gaps, so ordering holds even with up to 5 requests in flight. The protection covers retries within one producer session only. A restarted app gets a new ID, and a resend by the application itself is a new record. Conflicting settings, such as an explicit `acks=1`, turn idempotence off.

**Transactions.** A producer with a stable `transactional.id` can write to many partitions **and** commit consumer offsets as one atomic unit. A transaction coordinator writes commit or abort markers into each partition. An epoch attached to the `transactional.id` **fences zombies**, meaning an old instance can't commit after a failover. Consumers must set `isolation.level=read_committed` (the default is `read_uncommitted`) to stop at the last stable offset and skip aborted data.

### Delivery semantics

| Semantics | Consumer pattern | On crash |
|---|---|---|
| At-most-once | commit, then process | uncommitted work is lost |
| At-least-once | process, then commit | work is redone, producing duplicates |
| Exactly-once (Kafka→Kafka) | consume-transform-produce in one transaction (outputs plus input offsets); `read_committed` downstream | no loss or duplicates inside Kafka |

Kafka's exactly-once guarantee is real but **bounded to Kafka**. A database write, HTTP call or email inside the loop sits outside the transaction. End to end, you need an **idempotent sink** (upsert or `MERGE` by business key with a version check) or a sink that commits offsets atomically with the data (P6). Pipeline-level exactly-once, including Spark checkpoints into Delta, is covered in [§11](11-stream-processing.md).

### Schema Registry and formats

Kafka sees only bytes, and producers and consumers deploy independently, so the schema is the contract (P8). Without enforcement, one renamed field breaks every consumer or turns into a stream of poison pills.

A Schema Registry stores versioned schemas per **subject** (by default `<topic>-value`). The serializer looks up or registers the schema and prefixes each message with a magic byte and a **4-byte schema ID**. The deserializer fetches the schema by ID and caches it. Enforcement happens **at registration**, when the registry checks a new version against the subject's compatibility mode:

| Mode | Guarantee | Safe change | Deploy first |
|---|---|---|---|
| BACKWARD (default) | the new schema reads data written with the previous one | delete a field; add a field with a default | consumers |
| FORWARD | the previous schema reads data written with the new one | add a field; delete a field that has a default | producers |
| FULL | both directions | add or remove fields that have defaults | either |
| `*_TRANSITIVE` | the same check, against **all** earlier versions | — | — |

BACKWARD is the default because it lets a newly upgraded consumer rewind to the beginning of the topic. Use the transitive variants when long retention or compacted topics mean old versions really get replayed.

Format choice:
- **Avro** is compact, evolves through defaults, and is the most common choice on data platforms.
- **Protobuf** identifies fields by number and suits teams that already use gRPC or code generation.
- **JSON Schema** is readable but larger, with looser evolution semantics.

### Kafka Connect

Every team was rewriting "table to Kafka" and "Kafka to S3" with its own offset tracking and retries. **Kafka Connect** packages that work. **Workers** run **connectors**, and each connector is split into **tasks**. In distributed mode the workers form a group and keep connector configs, source offsets and status in compacted Kafka topics, so workers are stateless and tasks move when a worker dies.
- **Source** connectors pull data from a system and track a position there, such as a binlog offset. **Sink** connectors push to a system and track progress with consumer-group offsets.
- **Converters** handle serialization (for example, Avro with Schema Registry). **SMTs** (single message transforms) make stateless per-record changes such as rename, mask, route or set-key. Joins and aggregations belong in a stream processor ([§11](11-stream-processing.md)).
- **Error handling.** With `errors.tolerance=all` and `errors.deadletterqueue.topic.name` set, bad records go to a DLQ topic with error context in their headers. This built-in DLQ is a **sink**-side feature.
- **Connect or custom code?** Use Connect for standard system-to-Kafka movement when a mature connector exists. Write code when the logic is business-specific or stateful, or the connector is immature. Auto Loader plays the same role for files in Databricks: a managed tracker of what has already been ingested.

### Debezium CDC and the outbox pattern

Polling with `WHERE updated_at > :last` misses hard deletes and intermediate updates, depends on a trustworthy timestamp column, and loads the OLTP database (P1). **Log-based CDC** reads the database's own log instead: the MySQL binlog, or the Postgres WAL through logical decoding. That captures every committed change in commit order at low overhead (P5). Debezium runs as Connect source connectors:
1. **Snapshot.** It reads existing rows (`op=r`) and records the log position at which the snapshot was taken. Incremental snapshots can re-snapshot a table while streaming continues.
2. **Stream.** It continues from that log position, with one topic per table and **key = primary key**, so changes to each row stay in order.
3. **Envelope.** Each event carries `before`, `after`, `source` (table, LSN or binlog position, transaction ID, commit time), `op` (`c`/`u`/`d`/`r`) and `ts_ms`. A delete is followed by a tombstone, so compacted topics drop the key.

Postgres needs `wal_level=logical`, a **replication slot** and the `pgoutput` plugin. Full `before` images also require `REPLICA IDENTITY FULL`. The classic outage: the connector stops, the slot pins WAL, and the primary's disk fills up. Monitor slot lag.

**The outbox pattern** solves the **dual-write** problem. A service that writes its database and then publishes to Kafka can crash between the two steps, leaving them out of sync, and two-phase commit across the database and Kafka isn't practical. Instead, the service writes the business row **and** an event row into an `outbox` table in **one local transaction**. Debezium reads the outbox from the log and publishes the events, with its outbox router keying each event by aggregate ID.

```
BEGIN; UPDATE orders ...; INSERT INTO outbox(event_id, aggregate_id, type, payload); COMMIT;
   WAL ──> Debezium (outbox event router) ──> topic "order.events" (key = aggregate_id)
```

The database transaction provides atomicity. Publishing is at-least-once, so consumers deduplicate on `event_id` (P6).

### Tiered storage, diskless topics, topics as tables

- **Tiered storage (KIP-405, production-ready in 3.9).** Closed segments go to object storage, and brokers keep only the hot tail on local disk. Reads of old offsets are fetched from remote storage transparently. Retention is no longer bounded by broker disks (P2), so long replay windows become cheap and broker replacement gets faster. Compacted topics are not supported.
- **Diskless topics (KIP-1150, accepted March 2026).** Brokers write batches straight to object storage instead of replicating them between local disks in different zones. That removes cross-AZ replication traffic, often the largest cost of running Kafka in the cloud, in exchange for latency in the hundreds of milliseconds instead of single digits. The detailed design lives in KIP-1163 and KIP-1164, and no Apache Kafka release ships it yet (⚠ verify). Vendors such as WarpStream and AutoMQ already ship this model.
- **Topics as tables.** Confluent Cloud's **Tableflow** materializes topics as **Iceberg (GA 2025)** and **Delta Lake (GA October 2025, with Unity Catalog sync)** tables. This matters to an architect because the hand-built "Kafka → Spark job → bronze table" hop turns into configuration. The log and the table become two views of one dataset (P5, P11). You still have to assign ownership of schema mapping, compaction, small files and access policy (P9).

### Failure modes

| Symptom | Usual cause | Response |
|---|---|---|
| Lag climbs on one partition | hot key, or a stuck consumer | rekey or salt; check that consumer |
| Lag climbs on all partitions | slow processing or a slow sink | scale consumers up to the partition count; batch sink writes |
| Constant rebalances ("storm") | processing exceeds `max.poll.interval.ms`; flapping pods | lower `max.poll.records`; static membership; cooperative assignment or KIP-848 |
| Consumer stuck at one offset | **poison pill** (bad or undeserializable record) | catch it, route it to a DLQ, continue; enforce schemas upstream |
| Data "missing" after a restart | offsets expired or out of range, so `latest` applied | alert on idle groups; set the reset policy explicitly; retention ≥ replay need |
| Broker disk full | time-only retention plus a traffic spike | `retention.bytes`, tiered storage, disk alerts |
| `RecordTooLargeException` | record over the ~1 MB default (`message.max.bytes`) | store the payload in object storage and send a pointer (claim check) |

### When not to use Kafka

Kafka pays off when many independent consumers need high throughput and replay. It is overhead (P10) in these cases:
- a low-volume integration between two systems
- a plain work queue (SQS, Service Bus, or share groups if Kafka is already in place)
- a team that can't run brokers and tune clients

If the real need is "OLTP changes in the lakehouse every 15 minutes," managed CDC into files or a native connector may be enough (P7). Without a platform team, a managed bus (Event Hubs, Kinesis, Pub/Sub) or fully managed Kafka gives up some control in exchange for operability.

## Comparison

**Log vs queue**

| | Kafka (log) | Traditional queue (RabbitMQ, SQS) |
|---|---|---|
| After a read | kept until retention or compaction removes it | deleted on ack |
| Position tracked by | the consumer (an offset per partition) | the broker (state per message) |
| Replay | seek to any offset or timestamp | generally not possible |
| New subscriber | a new group, nearly free | a new queue or copy |
| Ordering | per partition | per queue, weakened by competing consumers |
| Consumer scale-out | up to the partition count (share groups lift this) | add workers freely |

**Platforms**

| | Storage and scaling model | Kafka API | Max retention | Watch for |
|---|---|---|---|---|
| Apache Kafka, self-run | brokers with local disks, optional tiering | native | unlimited | you run everything |
| Confluent Cloud | Kafka with a proprietary cloud storage layer; elastic units | native | unlimited | cost; lock-in on proprietary features (Tableflow, Flink, governance) |
| Amazon MSK | real Kafka on AWS-managed brokers; provisioned (Standard, Express ⚠ verify) or Serverless | native | configurable, tiered on provisioned | you still size partitions, tune clients, plan upgrades |
| Azure Event Hubs | multitenant service sold in throughput, processing or capacity units; 32 / 100 / 1,024 partitions per hub (Standard / Premium / Dedicated) | protocol endpoint (not on Basic) | 7 days Standard; 90 days Premium and Dedicated | 1 MB message limit; no broker configs; transactions and Kafka Streams in Preview (⚠ verify); compaction GA |
| Amazon Kinesis Data Streams | shards (~1 MB/s in, ~2 MB/s out each); provisioned or on-demand | no (its own SDK/KCL) | 24 h default, up to 365 days | per-shard limits; no Kafka ecosystem |
| Apache Pulsar | stateless brokers plus BookKeeper segment storage; built-in tiering, multi-tenancy, geo-replication | via a protocol adapter (⚠ verify) | unlimited | two systems to run; smaller ecosystem |
| Redpanda | single C++ binary, thread-per-core, Raft per partition, no JVM | compatible | tiered | gaps on the newest KIPs |

## Interview questions

**Q: Explain Kafka to me as if I'm a DBA.**

Kafka takes the database's redo log or WAL, pulls it out of the database, and makes it a shared, durable service. Applications append events in order. Any number of readers tail the log from their own position, much like replicas replaying a log, and each can rewind. Partitions hash-partition the log for parallelism: order holds within a partition, not across partitions. `acks=all` with `min.insync.replicas` is synchronous replication with a quorum floor. Kafka is not a query engine: you derive tables from the log rather than `SELECT` from it (P5).

**Q: How does Kafka guarantee ordering?**

Only within a partition. A partition is an append-only sequence with one leader, and within a group it has one consumer. Per-key order holds if three conditions are met: the key is the entity you need ordered, the idempotent producer prevents retries from reordering records (it is on by default), and the partition count never changes. Global order requires a single partition, which caps throughput at one consumer, so design so that you don't need it. Downstream, parallel processing threads or out-of-order sink writes can still break ordering, so carry a version or offset and apply only newer values.

**Q: How many partitions should a topic have?**

Derive the number. It must be at least the target throughput divided by per-partition throughput, measured for the producer and for the slowest consumer, and at least the maximum consumer parallelism you'll want. Add headroom for growth, because adding partitions to a keyed topic later remaps keys. Don't overshoot: each partition costs broker memory, replication, recovery time and batch efficiency. Most topics need tens of partitions, a few need hundreds once the need is measured, and a topic that requires strict global order at low volume gets one.

**Q: What happens when a consumer in a group dies?**

It stops heartbeating. After `session.timeout.ms` (45 s by default) the coordinator removes it and reassigns its partitions. The new owners resume from the last committed offset, so any records that were processed but not committed are processed again: at-least-once, which is why the sink must be idempotent. The classic eager protocol pauses the entire group; cooperative assignment and KIP-848 move only the dead member's partitions, and static membership avoids a rebalance on a quick restart. A consumer that is alive but slow, exceeding `max.poll.interval.ms`, gets evicted the same way, and this is a common cause of rebalance storms.

**Q: acks=all with min.insync.replicas=2 and RF=3: what failures can you survive?**

You can lose one broker, or one zone if replicas are spread rack-aware. Leadership moves to an in-sync follower, writes continue, and no acknowledged record is lost because each one was on at least two replicas. Lose two and the ISR drops to 1, below 2: producers get `NotEnoughReplicas` and writes stop, but acknowledged data survives and remains readable. Lose all three and the partition is unavailable; with unclean election enabled, a returning stale replica would cause data loss. None of this protects against bad writes, deletions or a regional outage. Those need cross-cluster replication (MirrorMaker 2 or a vendor equivalent) and backups.

**Q: At-least-once vs exactly-once: how does Kafka implement EOS and what are its limits?**

At-least-once means process, then commit: a crash replays the uncommitted records, so you get duplicates but no loss, and that is the sane default. Exactly-once semantics (EOS) combines three mechanisms. The idempotent producer uses a producer ID and sequence numbers to dedupe retries. Transactions write outputs atomically across partitions together with the input offsets, and epochs fence zombie instances. Consumers read with `read_committed`. The limits: EOS covers only Kafka→Kafka read-process-write, and any database, API or file side effect falls outside it. It also adds latency and overhead, and consumers left on the default `read_uncommitted` see aborted data. End to end, make the sink idempotent or commit offsets with the data (P6, [§11](11-stream-processing.md)).

**Q: Log compaction: when and why?**

Use compaction when a topic represents current state by key rather than a history of events: table CDC, reference data, profiles, configuration, state-store changelogs. Because it keeps the latest value per key, a new consumer can bootstrap the full table by replaying from offset 0 without infinite retention, and tombstones propagate deletes. Don't use it for histories where every event matters (clicks, payments), and don't treat it as dedupe. The active segment is never compacted, so consumers still see repeats and must upsert. Tiered storage excludes compacted topics, so size local disks for them.

**Q: How do you handle schema evolution?**

Treat the schema as a versioned contract owned by the producer (P8). That means Avro or Protobuf in a registry, with compatibility set per subject and checked in CI, not discovered in production. BACKWARD means consumers upgrade first and can read the full history. FORWARD means producers upgrade first. FULL lets either side go first and suits widely shared topics, and the transitive modes apply when old data really gets replayed. Additive changes with defaults are safe. Renames and type changes are breaking, so publish a new field or a versioned topic and dual-write during the migration. Map evolution into Delta deliberately instead of auto-merging every change.

**Q: How would you do CDC from Postgres into the lakehouse?**

Run Debezium's Postgres connector (on Connect or a managed equivalent) with `pgoutput` and a replication slot: an initial snapshot, then WAL streaming into one topic per table, keyed by primary key, serialized as Avro with a registry. Land the raw change events append-only in a bronze Delta table for replay and audit. Then `MERGE` into silver by primary key, ordering by LSN and applying `op='d'` as deletes; Databricks' declarative CDC APIs handle the ordering and SCD types for you ([§11](11-stream-processing.md)). Main risks: the slot pins WAL if the pipeline stalls (alert on slot lag), replica identity determines what the `before` images contain, and source DDL needs a change process. If minutes of latency are acceptable and there's no other consumer, a managed CDC-to-files service is simpler (P7, P10).

**Q: What's the outbox pattern?**

It solves dual writes. A service that updates its database and publishes an event can't make the two steps atomic, so a crash between them leaves the database and the stream disagreeing. Instead, the service writes the business change and an event row into an outbox table in one local transaction. CDC (Debezium's outbox event router) reads the committed outbox rows from the log and publishes them, keyed by aggregate ID so each entity's events stay in order. You get atomicity from the database and at-least-once publishing, so consumers dedupe on event ID. The costs are an outbox table to prune and CDC infrastructure to run.

**Q: Kafka vs Event Hubs vs Kinesis: how do you choose?**

Start from the cloud ecosystem and the team (P10). **Event Hubs** is Azure-native and offers a Kafka protocol endpoint with no brokers to run, plus Capture to ADLS. In exchange you accept tier-capped partitions and retention (90 days at most), no broker tuning, and newer Kafka features such as transactions trailing behind. **Kinesis** is AWS-native with tight Lambda and Firehose integration, but it has its own API, per-shard limits and no Kafka ecosystem. Choose **Kafka** (MSK, Confluent or self-run) when you need the full API: Connect, transactions, compaction, Kafka Streams or Flink, long or tiered retention, and portability. Prefer managed Kafka over self-run unless a platform team has a clear cost or control reason.

**Q: Consumer lag is growing. How do you diagnose it?**

First, split the cases. Lag on a few partitions points to a hot key or a stuck consumer (a poison pill, or a hung thread holding the assignment). Lag on all partitions points to a throughput mismatch. Then check whether the group is rebalancing (a sawtooth lag pattern), whether consumers are CPU-bound, or whether they're waiting on the sink, which is the most common cause: slow database writes or many tiny file commits. Fixes include scaling consumers up to the partition count, batching sink writes, rekeying, and sending poison pills to a DLQ. Add partitions only if parallelism is truly the ceiling and the keys can tolerate remapping. Report lag in seconds against the freshness SLA (P7).

**Q: How is Kafka different from a message queue, and where do share groups fit?**

A queue broker tracks every message and deletes it on ack. Its consumers compete for messages, and replaying data or adding a subscriber is hard. Kafka retains the log while consumers track offsets, so groups are independent and replay is just a seek. The price is parallelism capped by partitions, plus head-of-line blocking. Share groups (KIP-932, GA in 4.2) add queue semantics on the same topics: per-record acks, redelivery, delivery limits and more consumers than partitions, at the cost of ordering. Use them for job-style work on data already in Kafka, not as a reason to adopt Kafka for a simple queue.

**Q: Why did Kafka drop ZooKeeper, and what changes for you?**

ZooKeeper was a second distributed system with its own operations, security and scaling limits, and a controller had to load metadata from it, which made failover slow and capped partition counts. KRaft makes metadata an internal Raft-replicated log on a controller quorum. The result is faster failover, higher partition limits, and one system to secure. Since 4.0 KRaft is the only mode, and ZooKeeper clusters must migrate on 3.x (3.9 is the bridge release) before upgrading. On managed services the change is mostly invisible.

**Q: When would you not recommend Kafka?**

When there's one producer, one consumer and modest volume, a queue or a direct integration is cheaper to run. I'd also avoid it when the only need is analytics freshness measured in minutes to hours, where CDC-to-files or a native ingestion connector does the job (P7), and when no team owns partition design, schema contracts and lag monitoring, since an unowned Kafka becomes an outage generator (P10). Kafka earns its cost when many independent consumers, replay, and high throughput or low latency are all real requirements.

## Takeaways
- Kafka is a replicated, partitioned, append-only log. Consumers own their position, so fan-out and replay are cheap (P5).
- Ordering is per partition only. The key determines the partition, and changing the partition count remaps keys.
- Partition count is the ceiling on consumer parallelism. Size it from throughput with headroom, and don't overshoot.
- RF=3, `min.insync.replicas=2` and `acks=all` survive one failure with no loss of acknowledged data; two failures stop writes but lose nothing.
- Idempotence and `acks=all` are on by default since 3.0. Transactions plus `read_committed` give exactly-once only from Kafka to Kafka; elsewhere you need idempotent sinks (P6).
- Compacted topics are tables (latest value per key). Retention-based topics are histories.
- The schema is a contract: BACKWARD means upgrade consumers first, FORWARD means producers first, FULL allows either order (P8).
- Log-based CDC plus the outbox pattern is the reliable way to get OLTP changes onto the log.
- 4.0 is KRaft-only and made KIP-848 GA. Share groups became GA in 4.2. Tiered storage is GA, diskless storage is coming, and topic-to-table materialization is closing the gap between streams and lakehouse tables.

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
- https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html
- https://learn.microsoft.com/en-us/azure/event-hubs/apache-kafka-configurations
- https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-quotas
- https://learn.microsoft.com/en-us/azure/event-hubs/compare-tiers
- https://learn.microsoft.com/azure/event-hubs/log-compaction
- https://azure.microsoft.com/updates/generally-available-kafka-compaction-support-in-azure-event-hubs/
- https://docs.azure.cn/en-us/event-hubs/apache-kafka-transactions
- https://secure.businesswire.com/news/home/20251029878892/en/Confluent-Expands-Tableflow-to-Power-Real-Time-Analytics-and-AI-Across-Clouds
- https://debezium.io/documentation/reference/stable/connectors/postgresql.html
