# 11. Stream Processing: Time, State, and Exactly-Once

> As of October 2026. Tags: **(GA)** generally available · **(Preview)** public/private preview · **(⚠ verify)** not confirmed against official docs.

## Why this exists

Some decisions lose value by the minute: blocking a fraudulent card, paging on a failing machine, reranking a feed while the user is still on the page. Batch answers questions about a **bounded** dataset ("yesterday's orders"), but these events form an **unbounded** stream that never "finishes". The Dataflow model's insight is that **batch is a special case of streaming**: a bounded stream is one whose input happens to end. If you get streaming right, batch falls out for free. In practice, unified APIs like Spark's `readStream`/`read` and Flink's DataStream/Table API are built on that bet.

Moving records quickly is the easy part. Every hard problem in this section comes from three things:
- **Time**: events arrive late and out of order, so "the 10:00–10:05 window" is never obviously complete.
- **State**: aggregations, joins and dedup must remember the past, and that memory grows.
- **Failure**: a job that runs forever will crash, and it must resume without losing or double-counting anything.

Kafka is the usual source (partitions, consumer groups, offsets: see [§10](10-kafka.md)). Lambda vs Kappa architecture is covered in [§02](02-storage-paradigms.md).

## Core concepts

### Three clocks: event, ingestion, processing time

```
event time      (when it happened)       10:00  10:01  10:02  10:03
ingestion time  (broker stamped it)      10:00  10:07  10:02  10:03   <- phone was offline
processing time (engine saw it)          10:00  10:08  10:02  10:04
arrival order:  e0, e2, e3, e1   (e1 is 6 min late and out of order)
```

- **Event time** is a field in the record (in Kafka, `CreateTime` set by the producer). It is the only clock that gives correct, **reproducible** business answers: replaying the same data next month produces the same windows.
- **Ingestion time** (Kafka `LogAppendTime`) is monotonic per partition and cheap, but it measures pipeline behavior, not the business.
- **Processing time** is the engine's wall clock. It is simple and has no disorder, but results change with lag, outages and replays, so a backfill gives different numbers.

Choosing event time forces you to handle **disorder**: you have to decide how long to wait for stragglers. That decision is the watermark.

### Windows

A window groups an unbounded stream into finite buckets so it can be aggregated.

```
event time ->   0    5    10   15   20   25   30
tumbling(10):   [   W1    )[   W2    )[   W3    )        fixed, non-overlapping
hopping(10,5):  [   A     )
                     [   B     )
                          [   C     )                    each event lands in 10/5 = 2 windows
session(gap 5): x x  x         x             x x
                [ S1     )     [S2 )         [S3  )      closes after 5 units of inactivity
```

- **Tumbling** windows suit periodic metrics.
- **Hopping/sliding** windows suit moving averages. They cost more state, because each event belongs to size/slide windows.
- **Session** windows are per-key and data-driven (user activity). They merge when a new event bridges a gap, so their state is the hardest to bound.
- **Global windows plus triggers** (Flink, Beam): one window per key that never closes, with a trigger that decides when to emit ("every 100 events", "every 30 s"). This is the escape hatch for custom logic, and you must evict state yourself.

### Watermarks and late data

A **watermark** is a heuristic assertion: "I don't expect any more events with event time < T." When the watermark passes a window's end, the engine can finalize that window and drop its state. Common formulations:
- **Spark**: `watermark = max(event_time seen) − delay threshold`, recomputed after each micro-batch. With several input streams, the global watermark is the **minimum** by default, so a slow stream never causes data loss.
- **Flink**: generated per source partition (bounded out-of-orderness). An operator's watermark is the min of its inputs.

**The watermark is the completeness-vs-latency dial (P7):**
- **Aggressive (small delay)**: results come out fast and state stays small, but real late events get **dropped**.
- **Lax (large delay)**: results are more complete, but output is delayed and **state grows** linearly with the delay times the event rate.

There is no correct value, only a business choice. Measure the actual lateness distribution (for example, p99.9 of `processing_time − event_time`) and pick a delay that covers the tail you care about.

**What happens to an event behind the watermark:**

| Policy | Mechanism | Engine support |
|---|---|---|
| Drop | Default. Spark guarantees not to drop data within the threshold and *may* drop data beyond it | Spark, Flink |
| Allowed lateness | Keep window state an extra period past the watermark; late events re-fire an updated result | Flink `allowedLateness`, Beam |
| Side output | Route too-late events to a separate stream or table for repair or audit | Flink `sideOutputLateData` |
| Update downstream | Emit corrections; the sink must upsert (MERGE) | Spark update mode, Flink retract/upsert streams |
| Reconcile in batch | Stream gives a fast provisional answer; a nightly batch recomputes and overwrites | Any (Kappa or Lambda trade-off, [§02](02-storage-paradigms.md)) |

**Spark output modes** define what each micro-batch emits from the result table:
- **append**: rows are emitted once and never change. For windowed aggregations, a window is emitted only after the watermark passes its end, so append mode needs a watermark and adds latency.
- **update**: emits only the rows that changed in this batch. Lowest latency, but the sink must upsert.
- **complete**: rewrites the whole result every batch. State is unbounded, so use it only for small aggregates.

### State

State is anything the operator remembers between records: running aggregates, open windows, join buffers, dedup keys, ML features. It is **partitioned by key**, so it scales horizontally and survives failure only through checkpoints.

- **Where state lives.** In-JVM hash maps are fast, but they hit GC pauses and memory limits. **RocksDB** (an embedded LSM key-value store on local disk) is the default answer for large state in Spark, Flink and Kafka Streams. On Databricks, RocksDB is the default state store provider from DBR 17.3 (GA). Flink 2.0 added **disaggregated state** (the ForSt backend, with remote object storage as primary and local disk as cache, plus async state access). It decouples state size from local disk and speeds up rescaling, and it is opt-in.
- **Size and TTL.** State ≈ (distinct keys) × (bytes per key) × (retention). Bound it with watermarks (window and join state expire automatically), or with TTL for keyed state that never closes on its own: Flink `StateTtlConfig` and `table.exec.state.ttl`, or Spark `transformWithState` TTL per state variable.
- **Arbitrary stateful logic.** Spark's `transformWithState` (DBR 16.2+; Apache Spark 4.0 ⚠ verify OSS version) replaces `(flat)MapGroupsWithState`. It provides multiple typed state variables, TTL, and processing-time and event-time timers. Flink's `KeyedProcessFunction` is the equivalent.

**Joins, by state cost:**
- **Stream–static**: each micro-batch joins against the current snapshot of a table. It is stateless and cheap. The catch is that it is *not time-consistent*: a replay joins against today's dimension values, not those at event time.
- **Stream–table / temporal join**: Flink `FOR SYSTEM_TIME AS OF` joins against the version of the dimension *as of the event's time*. Replays stay correct, but the dimension's history must be kept in state.
- **Stream–stream**: neither side is complete, so the engine must buffer **both** sides until no future match is possible. To bound that buffer you need watermarks on both inputs *and* an event-time range condition (`b.ts BETWEEN a.ts AND a.ts + 10 min`). Without them, state grows forever. In Spark, outer joins require both. Outer-join null rows are emitted only once the watermark proves no match can come, so they arrive late.

**Dedup:** `dropDuplicates(key)` alone keeps every key forever. Add event time plus a watermark, or use Spark's `dropDuplicatesWithinWatermark` (Spark 3.5+ ⚠ verify version), so keys expire. The watermark delay must exceed the largest time gap between duplicates. The alternative is to push dedup into the sink with a keyed MERGE, which moves "state" into the table.

### Fault tolerance: checkpoints and savepoints

**Flink: asynchronous barrier snapshots** (Chandy–Lamport-style):

```
source --[e5 e4 |B7| e3 e2]--> map --> window(keyed state) --> sink
            barrier 7 flows with the data, never overtakes it
1. Coordinator tells sources to inject barrier 7 and record their Kafka offsets.
2. An operator with several inputs *aligns*: it waits for B7 on all inputs.
3. It snapshots its state to durable storage, asynchronously (processing continues), then forwards B7.
4. When every sink acks B7, checkpoint 7 is complete.
   Recovery = reset all state to checkpoint 7 and rewind sources to its offsets.
```

Under backpressure, barriers get stuck behind queued data and checkpoints time out. **Unaligned checkpoints** let barriers overtake buffers and store the in-flight data in the snapshot. **Savepoints** are user-triggered, portable snapshots used for upgrades, rescaling and migrations. They map state to operators by **operator UID**, so set `uid()` explicitly, or a refactor will orphan your state.

**Spark: per-micro-batch write-ahead log.** For batch N, the driver first writes the planned offset range to `checkpoint/offsets/N`. It then runs the batch and commits the new state-store version, and only then writes `checkpoint/commits/N`. After a crash, an uncommitted batch N is re-run with *exactly the same offsets*. That is safe only if the processing is deterministic and the sink is idempotent.

**Why code changes break checkpoints.** The checkpoint stores state laid out by operator and key schema, plus the shuffle partition count. Changes that are generally unsafe:
- changing grouping keys
- adding or removing a stateful operator
- changing the aggregation or state schema
- changing `spark.sql.shuffle.partitions` (it is fixed by the checkpoint)
- changing the source topic or subscription

Stateless changes, such as editing a projection or filter, are usually fine. When state can't be migrated, start a new checkpoint and **backfill** (see the interview questions). Flink 1.x savepoints are not guaranteed to restore on 2.x, so plan major upgrades as a migration.

### End-to-end exactly-once

"Exactly-once" means **exactly-once effect on state and output**, not that each message crosses the network once. Retries make duplicate *delivery* unavoidable. What engines provide is **effectively-once** results. It takes three legs, and missing any one breaks it:

1. **Replayable source (P5).** Kafka offsets, the Delta log, or a file listing. The engine can re-read exactly the same input.
2. **Deterministic processing with checkpointed state.** Offsets and state are snapshotted together, so recovery rewinds both consistently. Avoid `now()`, random values, or lookups that change between attempts inside the replayed path.
3. **Idempotent or transactional sink (P6).** Re-running batch N must not double-write:
   - **Delta**: the streaming sink commits batch N with a transaction marker (query id plus batch id) in the log, so a re-run of N is detected and skipped. For `foreachBatch`, pass `txnAppId`/`txnVersion` to get the same guarantee.
   - **Iceberg**: Flink and Connect sinks commit data files when a checkpoint completes, and record the checkpoint id in the snapshot.
   - **Kafka transactional sink**: Flink uses a two-phase commit (pre-commit at the barrier, commit when the checkpoint completes). Kafka Streams uses `exactly_once_v2`, putting the consume, produce and offset commit in one transaction ([§10](10-kafka.md)). Consumers must read with `read_committed`, and they see data only at checkpoint cadence.
   - **MERGE/upsert on a business key**: duplicates collapse no matter how many times a batch runs. This is the most portable option.
   - **Store the source offset atomically with the data**: Snowpipe Streaming channels keep an offset token, and the connector resumes from the last committed token.

**What breaks it: side effects outside the transaction.** Emails, webhooks, payment calls, or a `foreach` that writes to an external DB will fire again on every replay. To fix it, give each effect an idempotency key that the receiver dedups on. Alternatively, use an outbox: write *intents* idempotently to a table or topic, and have a separate sender mark each intent as done.

### Engines

- **Spark Structured Streaming.** A micro-batch engine: each trigger runs a small batch job. It shares the DataFrame/SQL API with batch, so one codebase covers backfill and streaming. On Databricks it is the native engine under Auto Loader and Lakeflow pipelines. **Real-time mode (Preview)** is a new trigger, available on Databricks from DBR 16.4 LTS and in Apache Spark 4.1 (Dec 2025). Its mechanism:
  - Batches are long-running (the interval, default 5 min, is the *checkpoint* cadence, not latency).
  - All stages are scheduled at once, and a streaming shuffle passes records between stages as they are produced.
  - Vendor-claimed latency goes down to ~5 ms.
  - Constraints on Databricks: update mode only, classic job compute with autoscaling and Photon off (⚠ verify current compute matrix), and Kafka/Kinesis/Event Hubs sources. There is no Delta source or sink, no stream–stream joins, no session windows, and no event-time timers in `transformWithState`. The OSS 4.1 version is stateless-only.
  - Legacy `Trigger.Continuous` is not supported on Databricks.
- **Apache Flink.** A true pipelined engine that processes records one at a time. It has the richest event-time and state model (timers, allowed lateness, side outputs, temporal joins, CEP) and handles large state. The cost is more operations: cluster sizing, checkpoint tuning, state migrations. Flink SQL covers most use cases. Flink 2.0 (Mar 2025) removed the DataSet and Scala APIs and made Java 17 the default. Flink 2.1 and 2.2 added AI functions such as `ML_PREDICT` and improved **materialized tables** (declare a query plus a freshness target, and the engine maintains the table). Managed options include Confluent Cloud for Apache Flink, Amazon Managed Service for Apache Flink and Ververica.
- **Kafka Streams.** A **library** inside your Java app, with no cluster. Parallelism equals input partitions, spread across app instances. State lives in local RocksDB backed by **changelog topics**, so Kafka is the durable store, and rebuilding a lost instance means replaying its changelog. It is Kafka-in, Kafka-out only.
- **SQL-first and streaming databases.** ksqlDB (SQL over Kafka Streams), and Flink SQL as Confluent's strategic streaming SQL (⚠ verify ksqlDB's current investment status). Streaming databases (RisingWave, Materialize) maintain incrementally updated materialized views that you can query over the Postgres wire protocol. They are a good fit when the consumer wants a *queryable, always-fresh table* rather than a pipeline (P10).

### Landing streams in the lakehouse and warehouse

**Databricks:**
- Read Kafka directly with `spark.readStream.format("kafka")`. Use **Auto Loader** (`cloudFiles`) when the producer drops files in object storage.
- **Lakeflow Spark Declarative Pipelines** (formerly DLT, now `from pyspark import pipelines as dp`) declare **streaming tables** (append-only, incremental, exactly-once per source) and **materialized views**. A pipeline runs *triggered* or *continuous*. Real-time mode in pipelines is also (Preview).
- Triggers, which are the cost dial:
  - default or `processingTime='1 minute'`: always-on micro-batches.
  - `availableNow=True`: drain the backlog, then stop. It is the *only* kind supported on serverless, alongside `Once`, and `Once` is deprecated.
  - `realTime`: (Preview).
- **Small-file flood**: every micro-batch commits at least one file per writing task. A 10 s trigger with 200 tasks can produce millions of files per day. Fixes:
  - longer triggers
  - optimized writes (shuffle before write)
  - auto-compaction or `OPTIMIZE`
  - **liquid clustering**, which compacts and clusters incrementally without a partition explosion
  - predictive optimization to schedule the maintenance

  Never partition a streaming table by a high-cardinality column (P4, [§13](13-performance-cost.md)).

**Snowflake:**

| Path | Mechanism | Typical latency |
|---|---|---|
| Kafka connector v3, Snowpipe (file) mode | Buffers records into files, stages them, Snowpipe loads | ~1–several min (flush default ~120 s, min 10 s ⚠ verify) |
| Kafka connector v3, Snowpipe Streaming *classic* | Row-level inserts into channels; `buffer.flush.time` default 10 s, min 1 s | seconds |
| **Kafka connector v4 (GA Apr 2026)** | Rewrite on the **high-performance architecture**: server-side validation, schema evolution and COPY-style transforms in a `PIPE` object; exactly-once ordered; priced per GB ingested; up to 10 GB/s per table (vendor figure) | 5–10 s |
| **Snowpipe Streaming SDK / REST, high-performance** (GA AWS Sep 2025, Azure and GCP Nov 2025) | Your app writes rows to channels with offset tokens; needs the new Java/Python SDK or REST API | < 10 s typical |
| **Openflow** (GA May 2025; Kafka connector status ⚠ verify) | Managed, NiFi-based integration service; an alternative to running Kafka Connect yourself | connector-dependent |

Downstream inside Snowflake:
- **Dynamic Tables**: declarative, minimum `TARGET_LAG` 60 s, best-effort, not a schedule.
- **Streams + Tasks**: a stream is a change-tracking offset on a table. Triggered tasks run `WHEN SYSTEM$STREAM_HAS_DATA` and skip when there is nothing to do. The minimum trigger interval can be lowered to 10 s (default ⚠ verify).

Realistic end-to-end freshness for a curated Snowflake table is therefore **~1 minute or more**. A raw landing table is seconds. See [§06](06-snowflake-features.md).

**Stream-to-table bridges:** Confluent **Tableflow** materializes topics as Iceberg or Delta tables (Delta + Unity Catalog GA Oct 2025), so both engines can read one copy (P11, [§10](10-kafka.md)).

### Batch vs micro-batch vs true streaming

| Mode | Freshness | Compute | Complexity / ops | Example |
|---|---|---|---|---|
| Scheduled batch | hours | runs, then stops | lowest | nightly ELT |
| Incremental batch (`availableNow`, scheduled tasks) | 15 min–hours | runs, then stops; streaming code path | low | hourly bronze→silver |
| Always-on micro-batch | seconds–minutes | 24×7 cluster | medium: state, checkpoints, on-call | ops dashboards, CDC mirrors |
| Record-at-a-time (Flink, Kafka Streams, Spark real-time mode) | ms–sub-second | 24×7, tuned | highest | fraud scoring, alerting, personalization |

**When real-time isn't worth it:** the consumer is a dashboard refreshed hourly, the decision is human-speed, or the source itself updates in batch (a nightly file, an hourly API). **Streaming as incremental batch** is the default answer. Write it as a streaming query (checkpoints, exactly-once, incremental), run it on a schedule with `availableNow`, and turn the latency dial later without rewriting (P7). Hidden costs of true real-time:
- always-on compute
- state operations (RocksDB tuning, checkpoint storage, migrations)
- a pager rotation
- harder schema changes
- reprocessing complexity

### Failure modes

- **Unbounded state growth**: missing watermark, dedup without TTL, stream–stream join without a time bound, or `complete` mode. Symptoms are rising batch duration, checkpoint size and RocksDB disk use.
- **Watermark misconfig**: too tight drops data silently; too loose explodes state. In Flink, one **idle partition** holds the min watermark back and no window ever fires (fix with `withIdleness`).
- **Checkpoint incompatibility**: a deploy that changes stateful logic fails to restart, or restarts with wrong state.
- **Backpressure**: a slow sink fills upstream buffers. Kafka consumer lag grows, Flink checkpoints time out, and Spark batches overrun their trigger. Cap intake (`maxOffsetsPerTrigger`), scale the sink, or use unaligned checkpoints.
- **Skewed keys**: one hot key pins one task. Fix with salting plus two-phase aggregation, or local pre-aggregation.
- **Small-file flood** and **late-data surprises**: daily totals change after the dashboard has been screenshotted.
- **Duplicate side effects** on replay.

## Comparison

| | Spark Structured Streaming | Apache Flink | Kafka Streams | Streaming DB (RisingWave, Materialize) |
|---|---|---|---|---|
| Execution | Micro-batch; real-time mode (Preview) | Pipelined, record-at-a-time | Record-at-a-time in app threads | Incremental view maintenance |
| Typical latency | ~1 s to minutes; ms in real-time mode | ms to sub-second | ms | sub-second to seconds |
| State | Per-partition state store (RocksDB), checkpointed per batch | Keyed state (heap/RocksDB/ForSt), barrier snapshots | Local RocksDB + Kafka changelog topics | Managed internally |
| Event-time richness | Watermarks, windows, `transformWithState` timers | Richest: allowed lateness, side outputs, temporal joins, CEP | Windows, grace period, suppression | SQL semantics, varies |
| Ops model | Job on cluster; fully managed on Databricks | Cluster (JobManager/TaskManagers) or managed service | Library: deploy like any service | Database service |
| Languages | Python, Scala, Java, SQL | Java, Python, SQL | Java/JVM | SQL |
| I/O | Broad; lakehouse-native | Broad connector ecosystem | Kafka → Kafka only | Kafka/CDC in, SQL queries out |
| Best fit | Lakehouse ETL, unified batch and stream, Databricks shops | Low-latency, event-time-heavy, large state | Kafka-native microservices, enrichment | Always-fresh queryable views for SQL users |

## Interview questions

**Q: Event time vs processing time: why does it matter?**

Event time is when the thing happened. Processing time is when the engine saw it, and the gap includes network delay, offline devices, outages and backlogs. Business logic ("orders per minute") must use event time, because processing-time results change every time lag changes or you replay, so backfills won't reproduce. The price is disorder: you have to decide how long to wait for late events, which is what watermarks do. Processing time is fine for operational metrics about the pipeline itself.

**Q: Explain watermarks to a non-streaming engineer.**

A watermark is the engine saying "I'm now fairly sure I've seen everything up to 10:05." It's usually computed as the latest event time seen minus an allowed delay. Once it passes the end of a window, the engine publishes that window's result and frees its memory. It is a heuristic, not a guarantee: set it tight and you get fast results but drop stragglers; set it loose and results wait longer while memory grows. It's the latency-vs-completeness dial (P7), so I set it from the measured lateness distribution, not by guessing.

**Q: How do you handle late-arriving events?**

First decide what "late" costs the business. Then pick a policy: drop (default), extend allowed lateness so windows re-fire corrected results, side-output too-late events to a repair table, or emit updates to an upserting sink. For financial or regulatory numbers, I pair a fast provisional stream with a scheduled batch reconciliation that overwrites from the full log. Whatever the policy, make late drops observable by counting them, because silent drops are what blow up in a review.

**Q: What makes a stream–stream join expensive?**

Neither side is ever complete, so the engine must buffer both sides in state until it can prove no future match will arrive. State is roughly event rate × retention on both inputs. You bound it with watermarks on both sides plus an event-time range condition; outer joins require both, and their unmatched rows come out only once the watermark passes. If the business only needs "current dimension value", use a stream–static join instead. If it needs "dimension as of event time", use a temporal join. Both are far cheaper than a symmetric stream–stream join.

**Q: How does a streaming job recover from failure without losing or double-counting?**

It snapshots source offsets and operator state together, consistently. Flink injects barriers that flow with the data, and each operator snapshots asynchronously when the barrier passes. Spark writes the planned offsets before each micro-batch and the commit after it, so an incomplete batch is re-run with identical input. Recovery rewinds both state and offsets to the last snapshot. That gives exactly-once *state*, but output is only exactly-once if the sink is idempotent or transactional.

**Q: How do you get exactly-once from Kafka to a Delta table and to a Snowflake table?**

Exactly-once needs a replayable source (Kafka offsets), deterministic processing with checkpointed state, and an idempotent or transactional sink. For Delta, Structured Streaming's sink records the batch id in the transaction log, so a replayed batch is skipped. With `foreachBatch` I use `txnAppId`/`txnVersion`, or a MERGE on the business key. For Snowflake, the Kafka connector v4 on Snowpipe Streaming high-performance tracks committed offset tokens per channel and resumes from them. That is "store the offset with the data." I still dedup with a MERGE on the event id downstream, because producer retries upstream of Kafka can create duplicate events that no sink protocol will catch.

**Q: Flink vs Spark Structured Streaming: when each?**

Spark is my default on a Databricks lakehouse: one API for batch and streaming, Delta-native exactly-once, managed operations, and second-to-minute latency covers most analytics needs. Flink wins when you need sub-second latency *with* heavy event-time logic: large keyed state, timers, allowed lateness, temporal joins, CEP, or Kafka-to-Kafka services. Spark's real-time mode (Preview) narrows the latency gap, but it currently excludes stream–stream joins, session windows and Delta sinks. The cost of Flink is a second platform and the skills to run it, so I'd want a managed Flink service unless the team already operates it.

**Q: Kafka Streams vs Flink?**

Kafka Streams is a library: no cluster, deployed like any microservice, scaled by adding instances up to the partition count, with state made durable through Kafka changelog topics. It fits Kafka-in, Kafka-out enrichment owned by an application team. Flink is a separate distributed engine with broader connectors, richer event-time semantics, and better handling of very large state and rescaling. I'd choose Kafka Streams for service-local logic and Flink for shared, platform-level pipelines or non-Kafka sinks. Kafka Streams state restore after losing an instance can be slow without standby replicas.

**Q: Design: stream from Kafka into both Databricks and Snowflake.**

Kafka is the shared log (P5) with Schema Registry contracts (P8). Each platform consumes independently, so neither blocks the other. Databricks reads with Structured Streaming or a Lakeflow streaming table into a bronze Delta table. Snowflake ingests through the Kafka connector v4 (Snowpipe Streaming, ~5–10 s) into a raw table, with Dynamic Tables downstream. To avoid two divergent copies, I'd consider a single open-table landing instead: Tableflow or a Delta/Iceberg bronze table that Snowflake reads as an external or Iceberg table (P11). The trade-offs are governance in two catalogs, duplicate storage and compute, and reconciliation: I'd run a daily count and checksum comparison per partition and hour.

**Q: When would you push back on a real-time requirement?**

When the consumer can't act faster than the data refreshes: an hourly dashboard, a daily decision, a human in the loop, or a source that only lands in batch. I ask what action happens in the first minute and what a 15-minute delay costs. If the answer is "nothing", I propose streaming-as-incremental-batch: the same streaming code run with `availableNow` on a schedule. That gives near-batch cost, keeps the upgrade path open, and avoids 24×7 compute, state operations and a pager rotation (P7). I'd still go real-time when delay destroys value, as with fraud, outages or in-session personalization.

**Q: How do you reprocess or backfill a streaming pipeline?**

Treat the raw log as the source of truth and make the sink idempotent (P5, P6). If Kafka retention covers the range, start the new version with a fresh checkpoint into a *new* table, catch up, validate, then swap (blue/green). If it doesn't, backfill from the bronze Delta/Iceberg table with the same code in batch or `availableNow` mode, then hand off to the live stream at a known offset or timestamp. Watch event-time skew during fast replays, because partitions racing ahead inflate window and join state. Flink's watermark alignment, or rate limits like `maxOffsetsPerTrigger`, keep it bounded.

**Q: You changed a streaming query and it won't restart from its checkpoint. Why, and what now?**

The checkpoint encodes the state layout: operators, grouping keys, state schema, and in Spark the shuffle partition count. Changing any of those makes old state unreadable or wrong. Stateless edits (projections, filters) are usually safe. For stateful changes, either migrate state (Flink savepoint with stable operator UIDs and state processor tooling; `transformWithState` has some schema-evolution support), or start a fresh checkpoint and backfill state from the log. Prevention: version checkpoint paths alongside code, and review "is this change state-compatible?" in PRs.

**Q: A streaming job's state keeps growing until it falls over. What do you check?**

Look for something that never expires. Common causes are an aggregation with no watermark, `dropDuplicates` without event time, a stream–stream join without a time-range bound, `complete` output mode, or keyed state with no TTL. Next check for key explosion, such as a session id or request id used as a grouping key. Then check the watermark itself: an idle partition in Flink, or a lagging stream under Spark's min-watermark policy, can freeze it so nothing expires. Fix the logic first and the RocksDB tuning second.

**Q: How do you deduplicate events from an at-least-once pipeline?**

Dedup needs a stable event id from the producer, not a hash of the payload's arrival. In-stream, use watermark-bounded dedup (`dropDuplicatesWithinWatermark` in Spark, or keyed state with TTL in Flink), sized to the maximum gap between duplicates. At the sink, a MERGE on the event id makes writes idempotent regardless of upstream behavior. I usually do both: cheap in-stream dedup for the common case, and sink-side MERGE as the correctness backstop.

**Q: Streaming into Delta created millions of small files. What do you do?**

The cause is mechanical: each micro-batch commits at least one file per writing task. Lengthen the trigger interval if freshness allows, and reduce write parallelism or enable optimized writes. Turn on auto-compaction, or schedule `OPTIMIZE` (predictive optimization can manage this). Prefer liquid clustering over high-cardinality partitioning. For downstream readers, a compaction job decoupled from ingestion keeps both latency and query performance acceptable.

## Takeaways

- Batch is bounded streaming. The hard problems are time, state and failure, not moving bytes.
- Use event time for business logic and processing time only for pipeline health.
- A watermark is a guess about completeness. Its delay trades latency and state size against dropped data (P7).
- Every stateful operator needs an expiry story: watermark, time-bounded join, or TTL.
- Exactly-once = replayable source + deterministic checkpointed processing + idempotent or transactional sink. External side effects need idempotency keys.
- Checkpoints encode state layout. Stateful code changes need migration or a backfill.
- Spark for lakehouse-native unified pipelines, Flink for low-latency event-time-heavy logic, Kafka Streams for Kafka-native services.
- Snowflake: Kafka connector v4 / Snowpipe Streaming give ~5–10 s landing. Dynamic Tables add at least 60 s.
- Default to streaming-as-incremental-batch (`availableNow`) and buy real-time only when delay destroys value.

## Sources

- https://docs.databricks.com/aws/en/structured-streaming/real-time
- https://docs.databricks.com/aws/en/structured-streaming/real-time/reference
- https://docs.databricks.com/aws/en/ldp/real-time
- https://docs.databricks.com/aws/en/structured-streaming/triggers
- https://docs.databricks.com/aws/en/structured-streaming/watermarks
- https://docs.databricks.com/aws/en/stateful-applications
- https://docs.databricks.com/aws/en/ldp/where-is-dlt
- https://apache.googlesource.com/spark/+/master/docs/streaming/real-time-mode.md
- https://www.databricks.com/blog/introducing-apache-sparkr-41
- https://docs.snowflake.com/release-notes/2026/other/2026-04-20-kafka-connector-v4-ga
- https://docs.snowflake.com/user-guide/kafka-connector/migrate-v3-to-v4
- https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming-kafka
- https://docs.snowflake.com/release-notes/2025/other/2025-09-23-snowpipe-streaming-high-performance-architecture
- https://docs.snowflake.com/release-notes/2025/other/2025-11-05-snowpipe-streaming-azure-ga
- https://docs.snowflake.com/release-notes/2025/other/2025-11-10-snowpipe-streaming-gcp-ga
- https://docs.snowflake.com/en/user-guide/dynamic-tables/target-lag
- https://docs.snowflake.com/en/user-guide/tasks-triggered
- https://docs.snowflake.com/release-notes/2025/other/2025-05-30-openflow
- https://flink.apache.org/2025/12/04/apache-flink-2.2.0-advancing-real-time-data--ai-and-empowering-stream-processing-for-the-ai-era/
- https://docs.aws.amazon.com/managed-flink/latest/java/flink-2-2.html
- https://www.vldb.org/pvldb/vol18/p4846-mei.pdf
- https://www.confluent.io/press-release/Tableflow-powers-real-time-AI-across-clouds/
