# 11. Stream Processing: Time, State, and Exactly-Once

> As of October 2026. Tags: **(GA)** generally available · **(Preview)** public/private preview · **(⚠ verify)** not confirmed against official docs.

## Why this exists

Some decisions lose value by the minute: blocking a fraudulent card, paging on a failing machine, reranking a feed while the user is still on the page. Batch answers questions about a **bounded** dataset. Events, though, form an **unbounded** stream that never finishes. The Dataflow model's insight is that **batch is a special case of streaming**: a bounded stream is one whose input happens to end. Unified APIs (Spark `readStream`/`read`, Flink DataStream/Table) are built on that bet.

Moving records quickly is the easy part. The hard problems are:
- **Time**: events arrive late and out of order, so no window is obviously complete.
- **State**: aggregations, joins and dedup must remember the past.
- **Failure**: a job that runs forever will crash, and it must resume without losing or double-counting anything.

Kafka internals are in [§10](10-kafka.md). Lambda vs Kappa is in [§02](02-storage-paradigms.md).

## Core concepts

### Three clocks: event, ingestion, processing time

```
event time      (when it happened)   10:00  10:01  10:02  10:03
ingestion time  (broker stamped it)  10:00  10:07  10:02  10:03   <- phone was offline
processing time (engine saw it)      10:00  10:08  10:02  10:04
arrival order: e0, e2, e3, e1        (e1 is late and out of order)
```

- **Event time** is a field in the record (Kafka `CreateTime`). It is the only clock that gives correct, **reproducible** business answers: a replay next month yields the same windows.
- **Ingestion time** (Kafka `LogAppendTime`) is cheap and monotonic per partition, but it measures the pipeline, not the business.
- **Processing time** is the engine's wall clock. It has no disorder, but results shift with lag, outages and replays.

Event time forces you to handle **disorder**: you must decide how long to wait for stragglers. That decision is the watermark.

### Windows

```
event time ->   0    5    10   15   20   25   30
tumbling(10):   [   W1    )[   W2    )[   W3    )      fixed, non-overlapping
hopping(10,5):  [   A     )
                     [   B     )
                          [   C     )                  each event lands in 10/5 = 2 windows
session(gap 5): x x  x         x             x x
                [ S1     )     [S2 )         [S3  )    per key; closes after 5 idle units
```

- **Tumbling** windows suit periodic metrics.
- **Hopping/sliding** windows suit moving averages. They multiply state by size/slide.
- **Session** windows are data-driven and merge when an event bridges a gap, so they are the hardest to bound.
- **Global window plus trigger** (Flink, Beam): one never-closing window per key, plus a trigger that decides when to emit. It is an escape hatch for custom logic, and you must evict state yourself.

### Watermarks and late data

A **watermark** is a heuristic assertion: "no more events with event time < T are expected." When it passes a window's end, the engine finalizes the window and frees its state.
- **Spark**: `watermark = max(event_time seen) − delay`, recomputed per micro-batch. With multiple inputs, the global watermark is the **minimum** by default, so a lagging stream never causes drops.
- **Flink**: watermarks are generated per source partition, and an operator takes the min of its inputs.

**It is the completeness-vs-latency dial (P7):**
- An **aggressive** delay gives fast output and small state, but real late events are **dropped**.
- A **lax** delay gives more complete results, but output is later and **state grows** with delay × event rate.

Pick the delay from the measured lateness distribution (for example, p99.9 of `processing_time − event_time`), not by guessing.

| Late-event policy | Mechanism | Where |
|---|---|---|
| Drop | Default. Spark won't drop data within the threshold and *may* drop data beyond it | Spark, Flink |
| Allowed lateness | Keep window state longer; late events re-fire an updated result | Flink, Beam |
| Side output | Route too-late events to a repair or audit table | Flink `sideOutputLateData` |
| Emit updates | Downstream sink upserts corrections | Spark update mode, Flink upsert/retract |
| Batch reconcile | Fast provisional stream, then a scheduled recompute from the log overwrites it | Any |

**Spark output modes:**
- **append**: each row is emitted once and is final. A window is emitted only after the watermark passes its end, so this mode needs a watermark and adds latency.
- **update**: emits only the rows that changed. Lowest latency; the sink must upsert.
- **complete**: rewrites the whole result each trigger. State is unbounded, so use it only for small aggregates.

### State

State is what operators remember between records: aggregates, open windows, join buffers, dedup keys. It is **partitioned by key**, so it scales out, and it survives failure only through checkpoints.

- **Storage.** JVM-heap maps hit GC pauses and memory limits. **RocksDB** (embedded LSM key-value store on local disk) is the standard choice for large state in Spark, Flink and Kafka Streams; on Databricks it is the default provider from DBR 17.3. Flink 2.0 added opt-in **disaggregated state** (ForSt: object storage is primary, local disk is a cache, with async state access), which decouples state size from local disk and speeds up rescaling.
- **Size and TTL.** State ≈ keys × bytes per key × retention. Bound it with watermarks (window and join state expire), or with TTL for keyed state that never closes on its own (Flink `StateTtlConfig` / `table.exec.state.ttl`; Spark `transformWithState` TTL).
- **Custom stateful logic.** Spark `transformWithState` (DBR 16.2+; Apache Spark 4.0) replaces `mapGroupsWithState`. It has multiple typed state variables, TTL and timers. Flink's equivalent is `KeyedProcessFunction`.

**Joins, cheapest to most expensive:**
- **Stream–static**: each micro-batch joins against the table's current snapshot. Stateless and cheap, but *not time-consistent*: a replay sees today's dimension values.
- **Temporal join** (Flink `FOR SYSTEM_TIME AS OF`): joins against the dimension version *as of event time*. Replay-correct, but the dimension's history must be kept in state.
- **Stream–stream**: neither side is complete, so the engine buffers **both** sides until no match is possible. Bounding that requires watermarks on both inputs *and* an event-time range condition. Spark makes both mandatory for outer joins, and the outer-join null rows are emitted only once the watermark passes.

**Dedup:** `dropDuplicates(key)` alone remembers every key forever, so bound it:
- Key on a stable, producer-assigned event id.
- In-stream, add event time plus a watermark, or use `dropDuplicatesWithinWatermark` (Spark 3.5+). Set the delay longer than the maximum gap between duplicates.
- At the sink, a keyed MERGE is the correctness backstop.

### Fault tolerance: checkpoints and savepoints

**Flink: asynchronous barrier snapshots** (Chandy–Lamport-style):

```
source --[e5 e4 |B7| e3 e2]--> map --> window(keyed state) --> sink
1. Sources inject barrier 7 and record their Kafka offsets.
2. A multi-input operator aligns: it waits for B7 on every input.
3. It snapshots state to durable storage asynchronously, then forwards B7.
4. All sinks ack B7, and checkpoint 7 is complete.
Recovery = restore state from 7 and rewind sources to 7's offsets.
```

Under backpressure, barriers queue behind data and checkpoints time out. **Unaligned checkpoints** let barriers overtake buffers and store the in-flight data in the snapshot. **Savepoints** are user-triggered, portable snapshots for upgrades and rescaling. They map state to operators by **operator UID**, so set `uid()` explicitly.

**Spark: per-micro-batch write-ahead log.** The driver writes batch N's offset range to `offsets/N`, runs the batch, commits the state-store version, then writes `commits/N`. After a crash, an uncommitted N re-runs with *identical offsets*. That is safe only with deterministic logic and an idempotent sink.

**Why code changes break checkpoints.** The checkpoint encodes the state layout. These changes generally make old state unreadable or wrong:
- grouping keys
- adding or removing a stateful operator
- state or aggregation schema
- the shuffle partition count
- the source subscription

Stateless edits are usually safe. Otherwise, migrate state or start a fresh checkpoint and backfill. Flink 1.x savepoints are not guaranteed to restore on 2.x.

### End-to-end exactly-once

Retries make duplicate *delivery* unavoidable. What engines achieve is an **exactly-once effect** ("effectively once"), and it needs three legs:

1. **Replayable source (P5):** Kafka offsets, the Delta log, a file listing.
2. **Deterministic processing with checkpointed state:** offsets and state are snapshotted together. Avoid `now()`, random values, or mutable lookups inside the replayed path.
3. **Idempotent or transactional sink (P6):**
   - **Delta**: the streaming sink records (query id, batch id) in the commit, so a re-run batch is skipped. For `foreachBatch`, use `txnAppId`/`txnVersion` or MERGE.
   - **Iceberg**: Flink and Connect sinks commit files when a checkpoint completes, and record the checkpoint id in the snapshot.
   - **Kafka**: Flink uses two-phase commit (pre-commit at the barrier, commit on checkpoint completion). Kafka Streams uses `exactly_once_v2` ([§10](10-kafka.md)). Readers need `read_committed` and see data only at checkpoint cadence.
   - **MERGE on a business key**: the most portable option. Duplicates collapse.
   - **Offset stored with the data**: Snowpipe Streaming channels keep an offset token, and the client resumes from the last committed token.

**What breaks it:** side effects outside the transaction, such as emails, webhooks, payment calls, or a `foreach` writing to an external API. They re-fire on every replay. To fix it, give each effect an idempotency key that the receiver dedups on, or use an outbox: write *intents* idempotently to a table or topic, and have a separate sender mark each one done.

### Engines

- **Spark Structured Streaming.** Micro-batch: each trigger is a small batch job, using the same DataFrame/SQL API as batch. It is the native engine under Auto Loader and Lakeflow on Databricks.

  **Real-time mode (GA on Databricks since Mar 2026)** is a new trigger, available on Databricks from DBR 16.4 LTS and in Apache Spark 4.1 (Dec 2025). How it works:
  - batches are long-running (the interval, default 5 min, is the *checkpoint* cadence)
  - all stages are scheduled at once
  - a streaming shuffle passes records between stages as they are produced
  - vendor-claimed latency goes as low as ~5 ms

  Limits on Databricks:
  - update mode only
  - classic compute (dedicated, or standard for Python only; no serverless) with autoscaling and Photon off
  - Kafka/Kinesis/Event Hubs sources only
  - no Delta source or sink, no session windows, no stream–stream outer joins (inner joins need DBR 18 LTS+ and extra config)
  - no event-time timers in `transformWithState`

  The OSS 4.1 version is Scala and stateless-only; Spark 4.2 (Jul 2026) adds the PySpark trigger. Legacy `Trigger.Continuous` is not supported on Databricks.
- **Apache Flink.** Pipelined, record-at-a-time, with the richest event-time and state model (timers, allowed lateness, side outputs, temporal joins, CEP) and good large-state handling. The cost is more operational work. Flink 2.0 (Mar 2025) removed the DataSet and Scala APIs and made Java 17 the default. Flink 2.1 added `ML_PREDICT`, and 2.2 and 2.3 (Jun 2026) improved **materialized tables** (a query plus a freshness target, and the engine maintains it). Managed options: Confluent Cloud for Apache Flink, Amazon Managed Service for Apache Flink, Ververica.
- **Kafka Streams.** A JVM **library** with no cluster. Its state lives in local RocksDB backed by Kafka **changelog topics**.
- **SQL-first.** ksqlDB and Flink SQL (on Confluent Cloud, Flink is recommended for new workloads; ksqlDB stays supported for existing apps). **Streaming databases** (RisingWave, Materialize) serve always-fresh queryable views over the Postgres protocol (P10).

### Landing streams in the lakehouse and warehouse

**Databricks** ([§07](07-databricks-gap-fill.md)):
- **Sources.** Read Kafka directly (`format("kafka")`), or use **Auto Loader** for files.
- **Lakeflow pipelines** (formerly DLT; they extend Apache Spark Declarative Pipelines) declare **streaming tables** and materialized views. Pipelines run *triggered* or *continuous*, and real-time mode in pipelines is (Preview).
- **Triggers are the cost dial:**
  - default or `processingTime`: always-on.
  - `availableNow`: drain the backlog, then stop. It is the only kind serverless supports, besides the deprecated `Once`.
  - `realTime`: (GA), classic compute only.
- **Small-file flood.** Each micro-batch commits at least one file per writing task. Mitigate with:
  - longer triggers
  - optimized writes
  - auto-compaction or `OPTIMIZE`
  - **liquid clustering** instead of high-cardinality partitioning ([§13](13-performance-cost.md))

**Snowflake** ([§06](06-snowflake-features.md)):

| Path | Mechanism | Typical latency |
|---|---|---|
| Kafka connector v3, Snowpipe (file) mode | Buffer to files, stage, Snowpipe load | ~1–several min (flush default 120 s, min 10 s) |
| Kafka connector v3, Snowpipe Streaming *classic* | Row inserts into channels; `buffer.flush.time` default 10 s, min 1 s | seconds |
| **Kafka connector v4 (GA Apr 2026)** | Rewrite on the high-performance architecture: server-side validation, schema evolution and COPY-style transforms in a `PIPE` object; exactly-once, ordered; per-GB pricing; up to 10 GB/s per table (vendor figure) | 5–10 s |
| **Snowpipe Streaming high-performance SDK / REST** (GA AWS Sep 2025, Azure and GCP Nov 2025) | Your app writes rows to channels; new Java/Python SDK or REST API required | < 10 s typical |
| **Openflow** (GA May 2025; its Kafka connector is GA) | Managed, NiFi-based integration; an alternative to self-run Kafka Connect | connector-dependent |

Downstream:
- **Dynamic Tables**: declarative, minimum `TARGET_LAG` 60 s, best-effort.
- **Streams + triggered Tasks**: `WHEN SYSTEM$STREAM_HAS_DATA`, skipped when the stream is empty. They run at most every 30 s by default; `USER_TASK_MINIMUM_TRIGGER_INTERVAL_IN_SECONDS` lowers that to 10 s.

A curated Snowflake table is realistically **≥ ~1 min** fresh; a raw landing table is seconds.

**Stream-to-table bridge:** Confluent **Tableflow** materializes topics as Iceberg or Delta tables (Delta + Unity Catalog GA Oct 2025), so both engines can read one copy (P11, [§10](10-kafka.md)).

### Batch vs micro-batch vs true streaming

| Mode | Freshness | Compute | Ops burden | Example |
|---|---|---|---|---|
| Scheduled batch | hours | runs, then stops | lowest | nightly ELT |
| Incremental batch (`availableNow`, scheduled tasks) | 15 min–hours | runs, then stops; streaming code path | low | hourly bronze→silver |
| Always-on micro-batch | seconds–minutes | 24×7 cluster | medium: state, checkpoints, on-call | ops dashboards, CDC mirrors |
| Record-at-a-time | ms–sub-second | 24×7, tuned | highest | fraud scoring, alerting |

**When real-time isn't worth it:** the consumer is an hourly dashboard or a human, or the source lands in batch. Default to **streaming as incremental batch**: a streaming query run on a schedule with `availableNow`, so you can turn the dial later without a rewrite (P7). The hidden costs of real-time are always-on compute, state operations, on-call, and harder schema changes and reprocessing.

### Failure modes

- **Unbounded state** and **checkpoint incompatibility** are covered above and in the interview questions.
- **Watermark misconfig**: too tight drops data silently; too loose explodes state. One **idle Flink partition** stalls the min watermark (fix with `withIdleness`).
- **Backpressure**: a slow sink leads to consumer lag, Flink checkpoint timeouts and Spark batch overruns. Cap intake (`maxOffsetsPerTrigger`) or scale the sink.
- **Skewed keys** pin one task. Fix with salting plus two-phase aggregation.
- **Late-data surprises**: totals change after they've been published.
- The **small-file flood** and **duplicate side effects** on replay round out the list.

## Comparison

| | Spark Structured Streaming | Apache Flink | Kafka Streams | Streaming DB (RisingWave, Materialize) |
|---|---|---|---|---|
| Execution | Micro-batch; real-time mode (GA) | Pipelined, record-at-a-time | Record-at-a-time in app threads | Incremental view maintenance |
| Typical latency | ~1 s to minutes; ms in real-time mode | ms to sub-second | ms | sub-second to seconds |
| State | Per-partition RocksDB store, checkpointed per batch | Keyed state (heap/RocksDB/ForSt), barrier snapshots | Local RocksDB + changelog topics | Managed internally |
| Event-time depth | Watermarks, windows, `transformWithState` timers | Richest: lateness, side outputs, temporal joins, CEP | Windows, grace periods | SQL semantics |
| Ops model | Job on cluster; fully managed on Databricks | Cluster or managed service | Library inside a service | Database service |
| Languages | Python, Scala, Java, SQL | Java, Python, SQL | Java/JVM | SQL |
| I/O | Broad; lakehouse-native | Broad connectors | Kafka → Kafka only | Kafka/CDC in, SQL out |
| Best fit | Lakehouse ETL, unified batch and stream | Low-latency, event-time-heavy, large state | Kafka-native microservices | Always-fresh queryable views |

## Interview questions

**Q: Event time vs processing time: why does it matter?**

Event time is when it happened. Processing time is when the engine saw it, and the gap includes offline devices, outages and backlogs. Business aggregates must use event time, because processing-time results change with lag and won't reproduce on replay or backfill. The price is disorder: you must decide how long to wait for late events, which is the watermark. Processing time is fine for metrics about the pipeline itself.

**Q: Explain watermarks to a non-streaming engineer.**

It's the engine saying "I'm fairly sure I've now seen everything up to 10:05," usually computed as the latest event time minus an allowed delay. When it passes a window's end, the engine publishes that result and frees its memory. Tight means fast but drops stragglers; loose means complete but slower, with more memory. It's a latency-vs-completeness dial (P7), set from the measured lateness distribution.

**Q: How do you handle late-arriving events?**

First, price lateness with the business. Then pick a policy: drop, allowed lateness with re-fired corrections, a side output to a repair table, or updates into an upserting sink. For financial numbers, I pair a fast provisional stream with a scheduled batch reconciliation from the log. Always count late drops; silent drops are the real failure.

**Q: What makes a stream–stream join expensive?**

Neither side is ever complete, so both are buffered in state until no future match is possible: roughly rate × retention on each side. You bound it with watermarks on both inputs plus an event-time range condition; outer joins require both, and they emit unmatched rows only after the watermark passes. If "current dimension value" is enough, use a stream–static join; if "as of event time" matters, use a temporal join. Both are far cheaper.

**Q: How does a streaming job recover without losing or double-counting?**

It snapshots source offsets and operator state consistently. Flink sends barriers through the data and snapshots asynchronously. Spark logs planned offsets before each batch and the commit after it, so an incomplete batch re-runs with identical input. That gives exactly-once *state*. Exactly-once *output* additionally needs an idempotent or transactional sink.

**Q: How do you get exactly-once from Kafka to a Delta table and to a Snowflake table?**

Exactly-once needs a replayable source, deterministic checkpointed processing, and an idempotent or transactional sink. For Delta, the streaming sink records the batch id in the commit, so replays are skipped; in `foreachBatch` I use `txnAppId`/`txnVersion` or a MERGE. For Snowflake, the Kafka connector v4 on Snowpipe Streaming tracks committed offsets per channel and resumes from them, which is "store the offset with the data." I still MERGE on event id downstream, because producer-side duplicates upstream of Kafka aren't caught by any sink protocol.

**Q: Flink vs Spark Structured Streaming: when each?**

On a Databricks lakehouse, Spark is my default: one API for batch and stream, Delta-native exactly-once, managed operations, and seconds-to-minutes latency. Flink wins when you need sub-second latency *and* heavy event-time logic: large keyed state, timers, temporal joins, CEP, Kafka-to-Kafka services. Spark's real-time mode (GA on Databricks) narrows the latency gap but still excludes stream–stream outer joins, session windows and Delta sinks. Flink's cost is a second platform, so I'd prefer a managed service.

**Q: Kafka Streams vs Flink?**

Kafka Streams is a library: deployed like any microservice, scaled by instances up to the partition count, with state made durable in Kafka changelog topics. It fits Kafka-to-Kafka logic owned by an application team. Flink is a separate engine with broader connectors, richer event time and better large-state rescaling, so I'd use it for shared platform pipelines and non-Kafka sinks. One watch-out for Kafka Streams: restoring a lost instance's state can be slow without standby replicas.

**Q: Design: stream from Kafka into both Databricks and Snowflake.**

Kafka is the shared log (P5) with Schema Registry contracts (P8), and each platform consumes independently. Databricks: a Lakeflow streaming table into bronze Delta. Snowflake: Kafka connector v4 (~5–10 s) into a raw table, then Dynamic Tables. Alternatively, land once in an open table (Tableflow, or a bronze Iceberg/Delta table that Snowflake reads) to avoid two copies (P11). The trade-offs are two catalogs to govern, duplicate cost, and reconciliation: I'd run daily per-hour count and checksum comparisons.

**Q: When would you push back on a real-time requirement?**

When no one acts faster than the data refreshes: an hourly dashboard, a daily decision, a human reviewer, or a batch-only source. I ask what happens in the first minute and what a 15-minute delay costs. If it costs nothing, I propose the same streaming code run as `availableNow` on a schedule. That is near-batch cost, with no 24×7 compute or pager, and keeps the upgrade path open (P7). I'd still go real-time for fraud, outages or in-session personalization.

**Q: How do you reprocess or backfill a streaming pipeline?**

Treat the raw log as truth and make the sink idempotent (P5, P6). If Kafka retention covers the range, run the new version with a fresh checkpoint into a *new* table, validate, then swap (blue/green). If it doesn't, backfill from bronze Delta/Iceberg with the same code in batch or `availableNow` mode, then hand off at a known offset. During fast replays, partitions racing ahead in event time inflate state; Flink watermark alignment or `maxOffsetsPerTrigger` keeps it bounded.

**Q: You changed a streaming query and it won't restart from its checkpoint. Why, and what now?**

The checkpoint encodes state layout: operators, keys, state schema, and Spark's shuffle partition count. Changing any of those invalidates it; stateless edits usually don't. Either migrate state (Flink savepoints with stable UIDs; limited state schema evolution in `transformWithState`) or start fresh and backfill. Prevention: version checkpoint paths with the code and ask "is this state-compatible?" in review.

**Q: A job's state keeps growing until it falls over. What do you check?**

Find what never expires: an aggregation without a watermark, `dropDuplicates` without event time, an unbounded stream–stream join, `complete` mode, or keyed state without TTL. Then look for key explosion (a request id used as a grouping key) and a frozen watermark (an idle Flink partition, or a lagging input under Spark's min policy). Fix the logic before tuning RocksDB.

**Q: Streaming into Delta created millions of small files. What do you do?**

It's mechanical: each micro-batch commits at least one file per writing task. Lengthen the trigger if freshness allows, reduce write parallelism, or enable optimized writes. Then add auto-compaction or scheduled `OPTIMIZE` (or predictive optimization), and use liquid clustering instead of high-cardinality partitions. Keep compaction decoupled from ingestion so neither blocks the other.

## Takeaways

- Batch is bounded streaming. The hard parts are time, state and failure.
- Use event time for business logic and processing time only for pipeline health.
- A watermark is a guess about completeness, trading latency and state against dropped data (P7).
- Every stateful operator needs an expiry story: watermark, time-bounded join, or TTL.
- Exactly-once = replayable source + deterministic checkpointed processing + idempotent or transactional sink. External side effects need idempotency keys.
- Checkpoints encode state layout. Stateful changes need migration or a backfill.
- Spark for lakehouse-native pipelines, Flink for low-latency event-time-heavy logic, Kafka Streams for Kafka-native services.
- Snowflake: Kafka connector v4 / Snowpipe Streaming land data in ~5–10 s. Dynamic Tables add at least 60 s.
- Default to streaming-as-incremental-batch and buy real-time only when delay destroys value.

## Sources

- https://docs.databricks.com/aws/en/structured-streaming/real-time
- https://docs.databricks.com/aws/en/structured-streaming/real-time/reference
- https://docs.databricks.com/aws/en/ldp/real-time
- https://docs.databricks.com/aws/en/structured-streaming/triggers
- https://docs.databricks.com/structured-streaming/watermarks.html
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
- https://docs.databricks.com/aws/en/structured-streaming/real-time/concepts
- https://docs.databricks.com/aws/en/structured-streaming/real-time/setup
- https://www.databricks.com/blog/announcing-general-availability-real-time-mode-apache-spark-structured-streaming-databricks
- https://docs.databricks.com/aws/en/release-notes/product/2026/may
- https://docs.databricks.com/aws/en/structured-streaming/rocksdb-state-store
- https://docs.databricks.com/aws/en/ldp/
- https://spark.apache.org/releases/spark-release-3-5-0.html
- https://spark.apache.org/releases/spark-release-4-0-0.html
- https://spark.apache.org/releases/spark-release-4-1-0.html
- https://spark.apache.org/releases/spark-release-4-2-0.html
- https://docs.snowflake.com/en/user-guide/kafka-connector-install
- https://github.com/snowflakedb/snowflake-kafka-connector/blob/v3.1.0/src/main/java/com/snowflake/kafka/connector/SnowflakeSinkConnectorConfig.java
- https://docs.snowflake.com/en/user-guide/data-integration/openflow/connectors/kafka/about
- https://flink.apache.org/2025/03/24/apache-flink-2.0.0-a-new-era-of-real-time-data-processing/
- https://flink.apache.org/2026/06/25/apache-flink-2.3.0-release-announcement/
- https://docs.confluent.io/cloud/current/topics/tableflow/overview.html
- https://docs.confluent.io/cloud/current/ksqldb/overview.html
