# 09. Batch Pipelines, CDC, and Orchestration

> As of October 2026. Tags: **(GA)** generally available · **(Preview)** public/private preview · **(⚠ verify)** not confirmed against official docs.

## Why this exists

Source systems change constantly, and analytics needs a copy that is correct, fresh enough, and cheap to keep current. Recopying everything is correct, but its cost scales with table size, not with change. Moving only the changes is cheap, but you must *know* what changed, and every detection method has blind spots. Pipelines also fail halfway through, and a pipeline you can't safely re-run is a pipeline you can't operate. The discipline therefore has three parts: detect change (by watermark or by log), apply it deterministically, and make every step idempotent so that a retry, a backfill and a recovery are the same operation (P1, P6). Orchestration puts the steps in order. It doesn't do the work.

## Core concepts

### ETL vs ELT: where the transform runs

ETL transformed data on a separate engine *before* loading, because warehouse compute was scarce and fixed (MPP appliances). ELT lands raw data and transforms it in place with SQL. The default flipped with storage/compute separation (P2): object storage is nearly free, and compute is rented by the second. Keeping raw data became cheap, and once you keep it, every transform becomes a replayable function of raw (P5, P6). When logic changes, you rebuild instead of re-extracting.

ETL still wins when **PII/PHI must never land** (residency, consent; once raw lands, erasure must also reach clones, time travel and fail-safe), when **transforms are heavy and not SQL** (binary formats, documents, ML features), or when **raw data is huge and low value** (telemetry you filter or aggregate first). The common compromise is **EtLT**: a light, stateless transform in flight (mask, cast, drop) and the modeling in the platform.

### Full vs incremental loads

A **full load** (truncate-and-reload or snapshot-and-swap) is correct by construction. It captures deletes and needs no state, but costs O(table). Use it for small dimensions, for sources with no reliable change marker, and as periodic reconciliation. An **incremental load** costs O(changes) but needs a change-detection mechanism plus stored state.

### Watermarks and their failure modes

A high-water mark stores `max(updated_at)` or `max(id)` from the last *successful* run. The next run reads rows above it, and the watermark advances only after the target commits.

```sql
SELECT * FROM src.orders
WHERE updated_at >  :last_watermark - INTERVAL '2 hours'  -- lookback overlap
  AND updated_at <= :run_upper_bound                       -- fixed at run start, trails now()
```

How it silently misses rows:
- **Late commits / long transactions.** A row is stamped `T1` but commits at `T3`, after you already read past `T1`. Timestamp order is not commit order. In Postgres, `now()` is the *transaction start* time, which makes this worse.
- **Clock skew** between the writers that stamp the timestamp.
- **Hard deletes** leave nothing to find.
- **`updated_at` not maintained** by bulk SQL, scripts or ORM bypasses.
- **Monotonic IDs** catch inserts only, and cached or per-node sequences commit out of order.
- **Precision/timezone truncation**, `>` vs `>=` on ties, and an upper bound that moves mid-run.

Mitigations: a **lookback window**, which is safe only because the apply step is idempotent; a fixed upper bound that trails `now()`; partition-level count/hash reconciliation; and log-based CDC when deletes matter.

### CDC: three ways to detect change

- **Query-based (polling)** is watermarking. It misses deletes and intermediate states (three updates between polls look like one), and it loads the source.
- **Trigger-based**: triggers write every change to a shadow table. It captures everything, but adds work *inside every source transaction's commit path*, and DBAs often refuse it.
- **Log-based** reads the database's own durability/replication log: the Postgres WAL via logical decoding, the MySQL row-format binlog, SQL Server CDC, or Oracle redo. That log already exists, so reading it costs the source little, and it yields every committed change in commit order, deletes and before-images included. The costs are privileged access, an initial snapshot handed off to the stream, schema-change handling, and **log retention**. A stalled Postgres replication slot keeps WAL until the disk fills, and a purged binlog forces a re-snapshot. Debezium is the common open-source engine (see [§10](10-kafka.md)).

### Applying CDC correctly

Each event carries an op (c/u/d), a key, the row image, and a **source position** (LSN, SCN or binlog offset). To apply them, land events append-only (raw is the replayable log, P5), deduplicate to the latest event per key **ordered by source position, not arrival time**, then MERGE with a position guard so that replays are no-ops:

```sql
MERGE INTO silver.customer t
USING (SELECT * FROM bronze.customer_cdc_batch
       QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY src_lsn DESC) = 1) s
ON t.customer_id = s.customer_id
WHEN MATCHED AND s.op = 'd'  AND s.src_lsn > t.src_lsn THEN DELETE   -- or SET is_deleted = true
WHEN MATCHED AND s.op <> 'd' AND s.src_lsn > t.src_lsn THEN UPDATE SET ...
WHEN NOT MATCHED AND s.op <> 'd' THEN INSERT ...;
```

**Soft deletes** keep history and let downstream tables see the delete. **Hard deletes** are required for erasure, and erasure must also purge history. SCD Type 2 from CDC closes the current version and opens a new one per change (see [§03](03-data-modeling.md)). Declarative options: Lakeflow **AUTO CDC** (which replaces `APPLY CHANGES`, same syntax) orders events with `SEQUENCE BY`. On Snowflake, use Streams + MERGE or Dynamic Tables.

### Idempotency (P6)

A run is idempotent when running it twice with the same parameters leaves the same state. You get there with:
- **Deterministic inputs.** Parameterize by the run's *logical interval* (`data_interval_start/end`), never `now()`.
- **Deterministic writes.** Choose a write pattern from the table below. Plain append duplicates rows on retry.
- **Stable keys.** MERGE on business keys. Surrogates minted per run (UUID, IDENTITY) change on rerun, so use deterministic hash keys or a key lookup.
- **Audit columns** (`_run_id`, `_src_lsn`) so you can delete exactly what a bad run wrote.
- **Atomic commits.** Table formats make writes all-or-nothing (P3). For multi-table consistency, build in staging and swap.
- **External side effects** (emails, API calls, topic publishes) sit outside table transactions. Use dedup keys or an outbox.

### Backfills

A backfill is *the normal job run over historical intervals*. If it needs separate code, the pipeline isn't idempotent. To run one safely, chunk by day or month with bounded concurrency so it can be resumed, read from a replica or export instead of the OLTP primary, and run on **isolated compute** so production SLAs don't starve and the cost can be attributed. **Zero-copy clone** the target, backfill into the clone, diff against prod (counts, sums, key sets), then swap. **Time travel** is the undo (Snowflake `AT/BEFORE`, `UNDROP`; Delta `RESTORE`), but only within retention and before `VACUUM`. Then reprocess the affected partitions in downstream incremental models.

### Late-arriving data

- **Late facts** (event time well before processing time). Each run recomputes a **reprocessing window** of the last N days by overwriting partitions. Size N from the measured lateness distribution and backfill the tail. A wider window costs more (P7). The streaming analogue is watermarks ([§11](11-stream-processing.md)).
- **Late-arriving dimensions** (a fact references a key that isn't in the dimension yet). Insert an **inferred member**: a placeholder row with the business key, "Unknown" attributes, `is_inferred = true`, and a real surrogate key, so the fact links immediately. When the real record arrives, *overwrite that row in place* (Type 1, even in an SCD2 dimension), so no facts need re-keying. The alternatives, a `-1` unknown member (re-key later) or quarantining facts, either force fact updates or delay the data.

### Orchestration: a control plane, not a data plane

An orchestrator owns the dependency graph, scheduling, retries with backoff, timeouts, concurrency, backfills, run parameters, alerting and lineage. It should **not** do heavy compute (pandas in workers) or pass data through its metadata store. Push compute down to the warehouse or Spark. Otherwise scheduler health becomes coupled to data volume.

**Task-based vs asset-based.** Airflow models *tasks* ("run X, then Y"). Dagster models *assets* ("this table derives from those"), which makes lineage, freshness and partial rematerialization first-class.

**Schedule-driven vs event-driven.** Cron is predictable, but it either wastes runs or adds latency waiting for upstream data. Event triggers (file arrival, table update, asset events, stream-has-data) run when the data is ready. They bring bursts and duplicate events, and they need **absence alerting**, because nothing fires when nothing arrives.

The main tools (compared below):
- **Apache Airflow 3** **(GA, April 2025)**: a Task SDK/Execution API decouples workers from the metadata DB, which enables remote execution. It adds DAG versioning, assets (renamed from datasets) with external-event scheduling, scheduler-managed backfills from the UI or API, and a new UI. SubDAGs and SLAs were removed.
- **Dagster**: assets, partitions, asset checks, strong local testing.
- **Lakeflow Jobs** (formerly Databricks Workflows): file-arrival, table-update and continuous triggers, if/else and for-each, and **repair runs** that re-run only the failed tasks.
- **Snowflake Tasks**: SQL task graphs (root, children, finalizer) on a warehouse or serverless (`TARGET_COMPLETION_INTERVAL`). **Triggered tasks** fire on `WHEN SYSTEM$STREAM_HAS_DATA(...)` with no schedule.

### dbt vs native declarative transforms

**dbt** compiles SQL+Jinja models into DDL/DML that runs on the engine. It provides a DAG from `ref()`, tests (generic, singular, unit), docs, lineage, environments, incremental strategies (`merge`, `delete+insert`, `insert_overwrite`, `microbatch`) and snapshots for SCD2. Its value is portability plus a software-engineering workflow. Current state:
- The Fivetran and dbt Labs merger **closed June 1, 2026**.
- The **Fusion** engine (a Rust rewrite that parses and type-checks SQL locally) is GA on the dbt platform for Snowflake and in preview for other adapters **(⚠ verify per adapter)**.
- **dbt Core v2.0 (alpha)** open-sources the Fusion runtime under Apache 2.0.
- **dbt Projects on Snowflake** **(GA, Nov 2025)** runs dbt natively inside Snowflake.
- Fivetran also owns SQLMesh (Tobiko Data, Sept 2025). How the two converge is **(⚠ verify)**.

**Snowflake Dynamic Tables**: you declare a query and a `TARGET_LAG`. Snowflake maintains the dependency graph and refreshes incrementally where the query allows. `TARGET_LAG = DOWNSTREAM` refreshes an intermediate table only when its consumers need it, and `FROZEN WHERE` (formerly `IMMUTABLE WHERE`) skips historical regions. Failure mode: a query that can't refresh incrementally falls back to an expensive full refresh. Pin `REFRESH_MODE` so that fails loudly.

**Lakeflow Spark Declarative Pipelines** (formerly DLT): streaming tables, materialized views, expectations and AUTO CDC. The core declarative API is in Apache Spark 4.1. AUTO CDC is Databricks-only.

The trade (P10, P11): native declarative transforms give fewer moving parts, engine-owned incrementality and lower latency, in exchange for lock-in. A common hybrid is dbt for authoring, with latency-sensitive models materialized as `dynamic_table` (Snowflake) or `streaming_table`/`materialized_view` (Databricks).

### Build vs buy ingestion

- **Fivetran**: managed SaaS, priced on monthly active rows (MAR), metered per connection since 2025. Reports of a per-connection minimum and of deletes counting toward MAR from 2026 are **(⚠ verify)**.
- **Airbyte**: open source plus cloud, with capacity-based tiers **(⚠ verify tier mapping)**.
- **Lakeflow Connect**: managed SaaS and database connectors billed as Databricks compute. Query-based database connectors went **GA in May 2026**.
- **Snowflake Openflow**: managed Apache NiFi. **(GA)** on AWS (BYOC) and Snowflake-managed on AWS/Azure.

How to decide:
- **Commoditization.** SaaS APIs churn, paginate, rate-limit and drift, so maintaining them is toil. Buy.
- **Volume and churn.** MAR pricing punishes high-update tables, so build those with Kafka/Debezium or file ingestion (Auto Loader, Snowpipe).
- **SLA.** Sync frequencies have floors. If you need seconds, go streaming.
- **Control.** Can data transit a vendor cloud?
- **Team capacity.** Every custom connector is a product with on-call.

**Buy the commodity, build the differentiator**, and land everything in open formats so the vendor stays replaceable (P11).

## Comparison

**Change-detection methods**

| | Query-based | Trigger-based | Log-based |
|---|---|---|---|
| Deletes / intermediate states | Missed | Captured | Captured |
| Source impact | Query load | Write-path overhead | Low (reads existing log) |
| Ordering | Timestamp (unreliable) | Trigger order | Commit order (LSN) |
| Main risk | Silent misses | DBA resistance, schema coupling | Log retention, slot bloat, re-snapshot |

**Write patterns**

| Pattern | Idempotent? | Best for | Watch out |
|---|---|---|---|
| Partition overwrite | Yes, by construction | Facts by event date; reprocessing windows | Run must own the whole partition |
| MERGE on business key | Yes, with ordering guard | Entities, dimensions, CDC | Duplicate source keys break the MERGE |
| Append | No | Immutable events | Needs a dedup key or delete-by-`_run_id` first |

**Orchestrators**

| | Airflow 3 | Dagster | Lakeflow Jobs | Snowflake Tasks |
|---|---|---|---|---|
| Model | Tasks (+ assets) | Assets | Tasks over Databricks objects | SQL task graphs |
| Scope | Any system | Any system | Databricks-centric | Snowflake-only |
| Event triggers | Assets, external events | Sensors, asset events | File arrival, table update | Stream-triggered |
| Ops burden | High if self-hosted | Medium | None | None |
| Pick when | Heterogeneous estate | Asset lineage/testing culture | Work lives in Databricks | Work lives in Snowflake |

**Transform layers**

| | dbt | Snowflake Dynamic Tables | Lakeflow SDP |
|---|---|---|---|
| Incremental logic | You choose the strategy | Engine decides | Engine decides; AUTO CDC |
| Tests/docs/lineage | Rich, built in | Catalog lineage; tests elsewhere | Expectations, UC lineage |
| Portability | High | Snowflake-only | Core API in Spark 4.1; key features Databricks-only |

## Interview questions

**Q: ETL vs ELT: when each?**

ELT is the default because cheap object storage and separately billed elastic compute (P2) make it cheap to keep raw data and rebuild from it without re-extracting. Choose ETL, or at least a small in-flight transform, when regulated data must never land raw, when transforms are heavy and not SQL, or when raw volume is huge and low value. A common mistake is landing PII raw "temporarily": clones, time travel and fail-safe keep copies alive long after the delete.

**Q: How do you design an idempotent pipeline?**

Parameterize every run by a logical interval rather than `now()`, so the same parameters select the same input. Write with partition overwrite (facts) or MERGE on business keys with an ordering guard (entities, CDC). Never use bare append. Stamp `_run_id` and source positions so one run's output can be removed, and rely on atomic table commits so a crash leaves nothing partial. The test: rerun yesterday's job twice and diff the outputs.

**Q: Log-based vs query-based CDC?**

Query-based CDC polls a change column, so it can't see deletes or intermediate updates, it loads the source, and it is exposed to late-commit and clock-skew misses. Log-based CDC reads the WAL/binlog the database already writes, so it gets every committed change in commit order at low source cost. You pay with privileged access, snapshot handoff, schema-change handling and log-retention risk (a stalled consumer can fill the source disk). Use query-based for append-mostly sources where deletes don't matter, and log-based for anything correctness-critical.

**Q: Your incremental load silently misses rows. What are the likely causes?**

The usual cause is long transactions: rows stamped before your watermark but committed after you read past it. Others: clock skew across writers, `updated_at` not maintained by bulk updates, hard deletes, sequence IDs committing out of order, `>` vs `>=` or precision truncation, and an upper bound that moves during the run. Fix it with a lookback overlap plus idempotent MERGE, a fixed upper bound that trails `now()`, and count/hash reconciliation per partition. If exactness matters, move to log-based CDC.

**Q: How do you apply a CDC stream to a table correctly?**

Land the events append-only. Dedup to the latest event per key ordered by LSN, not arrival time. MERGE only events newer than the target's stored position, so replays and overlaps are no-ops. Choose soft vs hard deletes deliberately: soft propagates downstream, hard is required for erasure. Failure modes: duplicate keys within a batch making the MERGE nondeterministic, and ordering by ingest time after a retry.

**Q: How do you handle late-arriving data?**

For late facts, overwrite a rolling reprocessing window of N days, sized from the measured lateness distribution, and backfill the long tail. A wider window costs more (P7). For late dimensions, insert an inferred member with a real surrogate key so facts link immediately, then overwrite it in place when the true record arrives. Silently dropping or holding facts is the failure mode: revenue disappears until someone notices.

**Q: How would you backfill two years of data safely?**

Use the production job itself, parameterized over historical intervals and chunked by month with bounded concurrency, so it can be resumed and doesn't hammer the source. Read from a replica, and run on isolated compute to protect production SLAs and attribute the cost. Write into a zero-copy clone, validate counts, sums and key sets against prod, then swap. Keep time travel as the rollback, and reprocess downstream incremental models. If the backfill needs different code from the daily job, fix idempotency first.

**Q: Airflow vs Lakeflow Jobs vs Snowflake Tasks?**

Choose by where the work lives and how heterogeneous the estate is (P10). Airflow 3 is the neutral cross-system control plane (remote execution, DAG versioning, asset events), but self-hosting it is real operational load. Lakeflow Jobs and Snowflake Tasks are zero-infrastructure and natively aware of their platform's objects and triggers, but weak outside it. A common split: native schedulers inside each platform, with Airflow or Dagster only where pipelines cross systems.

**Q: Schedule-driven or event-driven orchestration?**

Schedules are predictable, but they trade wasted runs against latency because you're guessing when upstream data will be ready. Event triggers run exactly when data lands. They bring bursts, duplicate events and the need for absence alerting. Use events for data-readiness dependencies and schedules for business deadlines, and monitor freshness either way.

**Q: dbt vs Dynamic Tables / Lakeflow Spark Declarative Pipelines?**

dbt gives portability, CI-friendly tests, docs and environments, and a vendor-neutral DAG. Native declarative tables let the engine own incremental computation and scheduling, with fewer moving parts and lower latency, in exchange for lock-in (P10, P11). The native risk is a silent full-refresh fallback, so pin the refresh mode. Pragmatically: author in dbt and materialize latency-sensitive models as dynamic or streaming tables, and check an adapter's Fusion status before standardizing on Fusion.

**Q: Build vs buy ingestion?**

Buy connectors for commoditized SaaS and standard databases. API churn and schema drift are toil with no differentiation. Build, or use streaming CDC, where volume or churn makes row-based pricing exceed engineering cost, where latency needs are below the tool's sync floor, or where data can't transit a vendor. Either way, land in open formats and keep raw so the vendor can be replaced.

## Takeaways
- Detect change, apply it deterministically, make every step idempotent: retries, backfills and recovery become one operation.
- Watermarks miss late commits, skew and deletes. A lookback plus idempotent MERGE plus reconciliation is the minimum.
- Log-based CDC is the correctness default. Its risk is log retention, not source load.
- Order CDC by source position, never arrival time.
- A backfill is the normal job over old intervals: clone, validate, swap, with time travel as the undo.
- Inferred members keep late dimensions from dropping facts.
- Orchestrators coordinate. Engines compute.
- Buy the commodity, build the differentiator, land open.

## Sources
- https://airflow.apache.org/blog/airflow-three-point-oh-is-here/
- https://airflow.apache.org/docs/apache-airflow/3.0.0/release_notes.html
- https://www.fivetran.com/press/fivetran-dbt-labs-complete-merger-to-create-the-data-infrastructure-for-trusted-ai-agents
- https://docs.getdbt.com/docs/fusion/fusion-readiness
- https://docs.snowflake.com/en/release-notes/2025/other/2025-11-06-dbt-projects-on-snowflake-ga
- https://www.fivetran.com/press/fivetran-acquires-tobiko-data-to-power-the-next-generation-of-advanced-ai-ready-data-transformation
- https://docs.databricks.com/aws/en/ldp/cdc
- https://docs.databricks.com/aws/en/ldp/where-is-dlt
- https://docs.snowflake.com/user-guide/dynamic-tables/best-practices
- https://docs.snowflake.com/en/user-guide/tasks-graphs
- https://docs.snowflake.com/en/user-guide/tasks-triggered
- https://docs.databricks.com/aws/en/jobs/triggers
- https://docs.databricks.com/aws/en/release-notes/product/
- https://docs.snowflake.com/en/user-guide/data-integration/openflow/connectors/about-openflow-connectors
- https://snowflake.com/en/product/features/openflow/
- https://hevodata.com/learn/fivetran-mar-pricing/
- https://bigdatawire.com/this-just-in/airbyte-introduces-capacity-based-pricing-for-predictable-data-movement-costs
