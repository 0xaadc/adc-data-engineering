# 06. Snowflake II: Data Features

> As of October 2026. Tags: **(GA)** generally available · **(Preview)** public/private preview · **(⚠ verify)** not confirmed against official docs.

## Why this exists

Snowflake stores every table as immutable columnar micro-partitions in object storage, plus a metadata service that records which micro-partitions make up each table version ([§05](05-snowflake-architecture.md)). Most of the "data features" here follow cheaply from that design (P2, P3). If you never overwrite a file, then old versions (Time Travel), copies (cloning), change sets (Streams) and cross-account access (Sharing) are metadata operations, not data movement. The other features exist because a warehouse on its own is not a platform. Data has to arrive (COPY, Snowpipe, Openflow), be transformed incrementally (Tasks, Dynamic Tables), run non-SQL code (Snowpark, containers), work with open formats (Iceberg), and increasingly serve operational and AI workloads (hybrid tables, Postgres, Cortex). Snowflake's bet is that one managed, SQL-first service with governance built in beats assembling those parts yourself (P10).

## Core concepts

### The one idea underneath: a version is a list of pointers

```
metadata service: table T
 v1: [mp1][mp2][mp3]
 v2: [mp1][mp2'][mp3]          UPDATE rewrote mp2 as mp2' (copy-on-write)
 v3: [mp1][mp2'][mp3][mp4]     INSERT added mp4
        mp2 is no longer active: kept for Time Travel -> Fail-safe -> purged

 clone C of T@v3   = [mp1][mp2'][mp3][mp4]   pointers only, zero bytes copied
 stream S @ v1     = diff(v1, v3) -> mp2 vs mp2', plus mp4
 share to acct B   = B's warehouse reads mp1..mp4 in place
```

Keep this picture in mind. Each feature below is a different question asked of the same version history.

### Time Travel

**Mechanism.** DML never edits a micro-partition. It writes new ones and commits a new table version. Time Travel keeps the superseded micro-partitions and version metadata for `DATA_RETENTION_TIME_IN_DAYS` and lets a query resolve an older version (P3).

- **Syntax:** `AT(TIMESTAMP => ...)`, `AT(OFFSET => -3600)` (seconds back) and `BEFORE(STATEMENT => '<query_id>')`, which is the "undo that bad statement" form. `UNDROP TABLE|SCHEMA|DATABASE` restores a dropped object, and fails if an object with the same name now exists. To restore a table, write `CREATE TABLE t_fix CLONE t BEFORE(STATEMENT => '...')` and then swap it in.
- **Retention (know these exactly):** the default is 1 day. Standard Edition allows at most 1 day. On Enterprise and above, permanent objects allow 0 to 90 days. Transient and temporary objects allow 0 or 1. Setting 0 disables Time Travel.
- **Cost:** you pay storage for every superseded micro-partition until both Time Travel and Fail-safe expire. Updating one row rewrites the whole micro-partition that holds it, so high-churn tables (wide MERGEs, nightly `CREATE OR REPLACE`) can carry several times their active size in history. Check `TABLE_STORAGE_METRICS`, which reports `ACTIVE_BYTES`, `TIME_TRAVEL_BYTES`, `FAILSAFE_BYTES` and `RETAINED_FOR_CLONE_BYTES`.
- **Databricks:** Delta `VERSION AS OF` and `RESTORE`. The difference is who controls the horizon. In Delta, `VACUUM` and file-retention settings decide how far back you can go. In Snowflake a retention parameter decides, and the service purges old data itself, so there is no VACUUM to forget or misconfigure.

### Fail-safe

When Time Travel expires on a **permanent** table, its history enters a **7-day, non-configurable Fail-safe** period. You cannot query it or reach it with SQL. Only Snowflake Support can recover from it, on a best-effort basis that takes hours to days. It is a backstop against catastrophic loss, not an operational tool, and you pay storage for it. Transient and temporary tables have no Fail-safe.
**Databricks:** no equivalent. The nearest thing is versioning or soft delete on the cloud storage bucket.

### Table types: a cost and recovery dial

| Type | Lifetime | Time Travel | Fail-safe | Use for |
|---|---|---|---|---|
| Permanent | Until dropped | 0–1 day (Standard), 0–90 days (Enterprise+) | 7 days | Systems of record, raw replay sources |
| Transient | Until dropped | 0 or 1 day | None | Staging, intermediate and rebuildable tables, dev |
| Temporary | Session | 0 or 1 day | None | Scratch work inside one session |

Rule of thumb: if upstream data can rebuild it, make it transient. Creating a database or schema as transient makes its tables transient too.

### Zero-copy cloning

`CREATE {DATABASE|SCHEMA|TABLE} x CLONE y [AT|BEFORE(...)]` copies **metadata** (the micro-partition pointers), not bytes. It takes time in proportion to the number of objects and partitions, not the data volume. Once created, source and clone are independent. Each side's new writes produce micro-partitions that only that side owns, and you are billed only for that divergence. A shared micro-partition stays alive while any clone still points to it (`RETAINED_FOR_CLONE_BYTES`). So a long-lived clone of a high-churn table quietly pins old data and keeps you paying for it.

**Uses:**
- **Dev and test:** full-size production replicas in seconds.
- **Safe backfills:** clone the target, backfill the clone, validate, then `ALTER TABLE ... SWAP WITH` (an atomic metadata swap, P3/P6).
- **Point-in-time debugging:** `CLONE ... AT(TIMESTAMP => ...)` reproduces yesterday's state.

**Gotchas:**
- Grants: a cloned database or schema does not inherit grants on the container itself, but child objects keep theirs. A table clone gets no grants unless you add `COPY GRANTS`.
- Tasks in a cloned container start suspended.
- Unconsumed rows in a cloned stream are inaccessible in the clone (⚠ verify).
- Pipes that reference internal stages are not cloned, and internal named stages need an explicit option (⚠ verify).
- Load metadata is not cloned, so a COPY into the clone can reload files the source already loaded (⚠ verify).
- A clone is not disaster recovery. It lives in the same account and region, on the same storage.

**Databricks:** `SHALLOW CLONE` (with `DEEP CLONE` as the copying variant). Snowflake reference-counts storage, so its clones never break. A Delta shallow clone can break if `VACUUM` on the source removes files the clone points to.

### Streams: change capture as a stored offset

A stream is **not a queue and holds no data**. It is an **offset**: a pointer to one table version, plus the ability to compute the difference between that version and now. Creating a stream turns on change tracking on the source, which adds hidden row-ID metadata so Snowflake can tell which rows changed between versions. A query on the stream returns the changed rows plus `METADATA$ACTION`, `METADATA$ISUPDATE` and `METADATA$ROW_ID`.

- **Standard:** net inserts, updates and deletes. "Net" means a row inserted and then deleted inside the window never shows up. An update appears as a DELETE plus an INSERT, both flagged `ISUPDATE`.
- **Append-only:** inserts only. It is cheaper because it never has to reconcile deletes. The right choice for landing tables.
- **Insert-only:** for external tables and externally managed Iceberg tables, where Snowflake can only see that new files have arrived.

**How the offset advances (P6).** A plain `SELECT` does not move the offset. It moves only when a **DML statement that reads the stream commits** (`INSERT ... SELECT`, `MERGE`). Inside an explicit transaction, every statement sees the same stream snapshot (repeatable read) and the offset advances at `COMMIT`. A rollback leaves it where it was. "Consume and write" is therefore atomic: a failed run that is retried sees the same delta. This is exactly-once-*ish*. No delta is lost or consumed twice, but your MERGE should still be keyed on a business key, because upstream systems resend data. If several pipelines read the same table, each needs its own stream, because each stream holds one offset (P5).

**Staleness.** The offset must stay inside the source's retention window, otherwise the versions needed to compute the delta are gone and the stream goes **stale**. A stale stream is unusable: you recreate it and reconcile with a full comparison. To soften this, Snowflake temporarily extends the retention of a table with an unconsumed stream, up to `MAX_DATA_EXTENSION_TIME_IN_DAYS` (default 14 days, ⚠ verify). Watch `STALE_AFTER`. The classic failure is a task paused over a long weekend on a table with 1-day retention.

**Databricks:** Change Data Feed (`readChangeFeed`). In Delta the offset lives in the *consumer's* streaming checkpoint. In Snowflake it lives in a *database object*, which you can grant, clone and monitor.

### Tasks

Tasks are a built-in scheduler for one SQL statement or stored-procedure call.

- **Compute:** either a user-managed warehouse (`WAREHOUSE = ...`) or **serverless**, where you omit the warehouse and Snowflake sizes compute and bills per second. Serverless wins for short, frequent jobs, where a warehouse's minimum billing and idle time would dominate the cost ([§05](05-snowflake-architecture.md)).
- **Task graphs:** a root task carries the schedule (interval or CRON), children declare `AFTER <parent>`, and an optional finalizer task handles cleanup. The graph must be acyclic.
- **`WHEN` conditions:** a condition such as `SYSTEM$STREAM_HAS_DATA('s')` is evaluated in the cloud services layer without starting a warehouse, so a poll that finds nothing costs almost nothing.
- **Triggered tasks (GA):** omit `SCHEDULE` and the task runs when its stream has data. It runs at most every 30 seconds by default, tunable down to 10 seconds. A serverless triggered task needs `TARGET_COMPLETION_INTERVAL`.
- **Failure mode:** tasks are created **suspended**, so a task that was deployed but never `RESUME`d never runs.

**Databricks:** Lakeflow Jobs, including file-arrival and table-update triggers.

**Streams + Tasks = the imperative incremental pipeline.** You own the merge logic, deduplication and error handling:

```sql
CREATE STREAM raw_orders_s ON TABLE raw_orders APPEND_ONLY = TRUE;
CREATE TASK merge_orders WAREHOUSE = etl_wh SCHEDULE = '5 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('raw_orders_s')
AS MERGE INTO orders t
   USING (SELECT * FROM raw_orders_s
          QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) = 1) s
   ON t.order_id = s.order_id
   WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET ...
   WHEN NOT MATCHED THEN INSERT ...;
ALTER TASK merge_orders RESUME;
```

### Dynamic Tables

You declare a `SELECT` and a **`TARGET_LAG`**, and Snowflake decides when and how to refresh. Snowflake builds a dependency graph from the queries. Each refresh reads what changed in the base tables since the last refresh (the same change-tracking machinery streams use). It then either merges a computed delta (**INCREMENTAL**) or recomputes the whole result (**FULL**), on the warehouse you assign.

- **`TARGET_LAG`:** a maximum staleness, for example `'15 minutes'`. The minimum is 1 minute, and lag is measured against the base tables at the root of the graph. **`DOWNSTREAM`** means "refresh only when a consumer needs it". Set it on intermediate tables so that only the leaf table's SLA drives the schedule.
- **`REFRESH_MODE`:** `AUTO` chooses once, at creation, and never revisits the choice. `INCREMENTAL` makes creation fail if the query can't be maintained incrementally. `FULL` always recomputes.
- **What blocks incremental:** non-deterministic functions (with `AUTO` they force FULL; with `INCREMENTAL` creation fails), unsupported SQL constructs (check the docs' supported-constructs list), and, in practice, change volumes so high that a full recompute is cheaper.
- **Versus materialized views:** Snowflake MVs (Enterprise+) cover a **single table with no joins**. A serverless service maintains them and they are always current. Use one to speed up a single hot filter or aggregate. Dynamic tables handle joins and multi-step pipelines at a chosen lag.
- **When not to use them:** when you need procedural logic, side effects, or custom SCD2 and merge semantics; when you need sub-minute latency; when a query silently falls back to FULL on a large table and cost explodes; or when a trivially cheap append-only stream plus task would do.

**Databricks:** materialized views and streaming tables in Lakeflow Declarative Pipelines (formerly DLT; current product name ⚠ verify).

### Loading: COPY, Snowpipe, Snowpipe Streaming

All three come down to one problem: knowing what has already been loaded (P6).

- **`COPY INTO` (batch):** runs on your warehouse and loads files in parallel. That is why the guidance is files of roughly **100–250 MB compressed**: too few large files limits parallelism, and too many tiny files adds overhead. The target table keeps **load metadata for 64 days** (file name, checksum, status), so rerunning COPY skips files it has already loaded. That makes it idempotent by default. `FORCE = TRUE` overrides this and creates duplicates. Files that have fallen outside the 64-day window need `LOAD_UNCERTAIN_FILES`.
- **Snowpipe (continuous files):** a `PIPE` object wraps a COPY statement. With `AUTO_INGEST = TRUE` it subscribes to cloud event notifications (S3 via SQS, Azure Event Grid, GCS Pub/Sub). The REST API is the alternative trigger. Compute is serverless, latency is typically about a minute (order of magnitude), and load order is not guaranteed. Deduplication metadata lives **in the pipe for 14 days**, so recreating the pipe erases it, and a careless redeploy reloads files. For trickle sources, aim for about one file per minute rather than many tiny files.
- **Snowpipe Streaming (rows, no files):** an SDK client opens **channels** on a table and appends rows, each batch tagged with an **offset token**. After a restart the client asks Snowflake for the last committed token on each channel and resumes from there. That gives exactly-once delivery when the source can be replayed by offset, as Kafka can (P5/P6). The **high-performance architecture** (GA: AWS September 2025, Azure and GCP November 2025) adds a server-side `PIPE` object as the entry point (COPY-style transforms and validation), Rust-core Java and Python SDKs plus a REST API, and throughput-based pricing. Order of magnitude: about 10 GB/s per table and **5–10 seconds** from ingest to query. Classic-architecture clients still exist (deprecation timeline ⚠ verify).
- **Kafka connector:** older versions either buffered records into files for Snowpipe or used Snowpipe Streaming. Version 4.0 (GA April 2026) is built on high-performance Snowpipe Streaming, with Kafka offsets carried as offset tokens. See [§11](11-stream-processing.md) and [§10](10-kafka.md).
- **Databricks:** Databricks also has `COPY INTO`. Auto Loader is the Snowpipe equivalent. Structured Streaming from Kafka (or Zerobus Ingest, ⚠ verify status) covers what Snowpipe Streaming does.

### External tables vs Iceberg tables

An **external table** is read-only metadata over files in your bucket. It is schema-on-read through a `VALUE` VARIANT column, supports no DML and prunes poorly. Iceberg tables have largely replaced it. An **Iceberg table** keeps data and metadata in Iceberg format in *your* storage (through an `EXTERNAL VOLUME`), so your cloud provider bills the storage, not Snowflake.
- **Snowflake-managed** (`CATALOG = 'SNOWFLAKE'`): Snowflake is both writer and catalog, with full DML and clustering. External engines can read **(GA, February 2026)** and write **(GA, May 2026)** these tables through Horizon Catalog's Iceberg REST API, which integrates Apache Polaris.
- **Externally managed:** a `CATALOG INTEGRATION` points to Glue, any Iceberg REST catalog (Polaris, Unity Catalog, S3 Tables) or raw metadata files. Snowflake reads them, and writes to tables in REST catalogs are **(GA)**.
- **Catalog-linked database (GA, October 2025):** links a whole remote REST catalog, so Snowflake discovers and syncs its namespaces and tables automatically instead of needing one `CREATE ICEBERG TABLE` per table.
- **Snowflake Open Catalog** is managed Apache Polaris. It no longer accepts new customers, and Snowflake points new customers to Horizon Catalog instead.

**Databricks:** Unity Catalog with Delta/UniForm and the Iceberg REST endpoint. Details in [§04](04-table-formats-catalogs.md).

### Secure Data Sharing

A **share** is a metadata object: grants on databases, schemas, tables, secure views and secure UDFs, plus a list of consumer accounts. The consumer creates a **read-only database from the share**. When it queries, *its own warehouse* reads the provider's micro-partitions in place. Within a region all accounts sit on the same storage and metadata layer, so nothing is copied. The provider pays for storage, the consumer pays for compute, and new data appears as soon as the provider commits. Revoking access takes effect immediately (P2, P3).

- **Reader accounts:** for consumers who don't have Snowflake. The provider creates and pays for the account, compute included. It is read-only and can only see that provider's data.
- **Listings:** a wrapper that adds descriptions, terms and usage analytics. A listing is either private (to named accounts) or public on the **Marketplace**, and can also package Native Apps.
- **Cross-region and cross-cloud:** storage isn't shared across regions, so the data must be physically copied. Either replicate the database to an account in the target region and share from there, or use **Cross-Cloud Auto-Fulfillment** on a listing, which replicates for you (at object level if you choose). Either way you pay replication, egress and target-region storage costs.
- **Data Clean Rooms:** two parties analyze overlapping data without exposing raw rows, because only approved query templates run. Built on sharing and Native Apps (current packaging ⚠ verify).
- **Delta Sharing contrast:** Delta Sharing is an *open protocol*. The server hands the recipient short-lived pre-signed URLs to Parquet files, so any client (pandas, Spark, BI tools) can read them, and there is open-sharing reach beyond Databricks. Snowflake sharing is *closed*: both ends need Snowflake, or the consumer needs a reader account. In return, row and masking policies apply live, no files are exposed, and in-region sharing is truly zero-copy. Openness buys reach. Closure buys tighter control and integration (P11).

### Snowpark, containers, notebooks

- **Snowpark DataFrames (Python, Java, Scala):** lazy. The client builds a plan, compiles it to **SQL** and runs it on the warehouse, so there is no cluster and no data export. When SQL can't express the logic, use **UDFs, UDTFs, UDAFs and vectorized UDFs**, or **stored procedures**. These run in a sandboxed runtime on the warehouse nodes with Anaconda-packaged libraries. Snowpark-optimized warehouses add memory for heavy work. **pandas on Snowflake** translates the pandas API to SQL (GA ⚠ verify). **Snowpark Connect for Apache Spark** runs PySpark code on Snowflake's engine through Spark Connect (status ⚠ verify).
- **Why it exists:** bring code to the data instead of copying data to Spark clusters. That avoids a data-movement hop (P1) and keeps one governance perimeter (P9). It also lets Snowflake compete for Python and ML workloads (P10).
- **Limits:** it is not a general distributed runtime. There are no RDDs, no arbitrary cluster libraries and no Structured Streaming. Parallelism is whatever the warehouse provides, and you end up debugging generated SQL.
- **Snowpark Container Services (SPCS):** runs OCI containers (long-running services and jobs) on compute pools, including GPUs, inside Snowflake's security boundary. Use it for apps, model serving and custom engines. **Databricks:** Databricks Apps and Model Serving.
- **Notebooks:** Notebooks in Workspaces **(GA, February 2026)** run on Container Runtime (built on SPCS) with CPU or GPU pools. The older warehouse-based notebooks are now legacy. **Databricks:** notebooks on serverless compute.

### Cortex AI

- **AI Functions:** LLM calls as SQL operators, running inside Snowflake's boundary and controlled by grants. AI_CLASSIFY, AI_EMBED, AI_SIMILARITY, AI_TRANSCRIBE, AI_EXTRACT, AI_SENTIMENT and AI_TRANSLATE are **(GA)**. For AI_COMPLETE, AI_FILTER and AI_AGG, status is ⚠ verify.
- **Cortex Search:** a managed hybrid (vector plus keyword) retrieval service over a table, used for RAG. **Cortex Analyst:** text-to-SQL grounded in semantic views (status ⚠ verify for both).
- **Cortex Agents (GA, November 2025):** orchestrates Analyst, Search and other tools. **Snowflake Intelligence (GA, November 2025):** an agent interface for business users, built on top of Cortex Agents.
- **Databricks:** AI Functions and `ai_query`, Vector Search, Genie and Agent Bricks. See [§18](18-ai-on-data-platform.md).

### Horizon (governance)

**Snowflake Horizon** is the governance umbrella. It covers column **masking policies**, **row access policies**, **object tags** with tag-based masking, classification, **ACCESS_HISTORY** (who read or wrote which columns), lineage, data-quality metric functions and the Trust Center. It also includes Horizon Catalog, which exposes tables to external engines over the Iceberg REST API. Policies attach to the data object and are enforced wherever it is queried, shares included (P9). **Databricks:** Unity Catalog. See [§12](12-governance-security.md).

### Convergence: OLTP and ingestion

**Why (P1).** Every pipeline ends in the warehouse, and the expensive, fragile parts are getting data *in* from operational systems (third-party ELT and CDC tools) and *back out* to applications. Owning the operational store and the ingestion path removes a hop, captures that spend, and gives apps and AI agents low-latency reads and writes next to governed analytics.

- **Hybrid tables (Unistore):** a row store with a required primary key, enforced PK, FK and unique constraints, secondary indexes and row-level locking, built for point lookups and small transactions. The data is also kept in columnar form for analytics. **(GA on AWS since October 2024; Azure ⚠ verify.)** It is not a high-throughput OLTP engine. Think application state and lookups at moderate query rates.
- **Snowflake Postgres (GA, February 2026, selected AWS and Azure regions):** real managed PostgreSQL instances on dedicated VMs, coming out of the 2025 Crunchy Data acquisition. Choose it when the application needs genuine Postgres semantics and drivers. **Databricks:** Lakebase.
- **Openflow:** managed integration built on Apache NiFi, with connectors for SaaS sources, database CDC, Kafka and unstructured data. It can run in your own VPC (BYOC) or inside Snowflake on SPCS. **(GA May 2025 for BYOC; Snowflake-hosted deployments entered Preview in September 2025, current status ⚠ verify.)** **Databricks:** Lakeflow Connect.

### Replication, failover, business continuity

**Replication groups** copy databases, shares and account objects (users, roles, warehouses) to accounts in other regions or clouds on a schedule, and refreshes are incremental. **Failover groups** (Business Critical edition or higher) let you promote a secondary account to primary. **Client Redirect** gives clients a connection URL that you can repoint to the new primary. Recovery point objective (RPO) is roughly the replication interval. Recovery time objective (RTO) is the promote step plus the redirect. Resilience within a region is built in. **Databricks:** there is no native account failover. Teams combine deep clones, Delta Sharing or storage replication with redeployed workspaces. See [§14](14-reliability-dataops.md).

## Comparison

| Feature | Snowflake mechanism | Databricks equivalent | Key difference |
|---|---|---|---|
| Time Travel | Retained micro-partitions + version metadata | Delta time travel | Parameter + auto-purge vs `VACUUM` |
| Fail-safe | 7 days, support-only | None (bucket versioning) | Extra backstop, extra storage cost |
| Clone | Metadata pointer copy, ref-counted | Shallow/deep clone | Snowflake clones can't break from source cleanup |
| Streams | Offset object on a table | Change Data Feed + checkpoint | Offset is a DB object, not consumer state |
| Tasks | SQL scheduler, serverless option | Lakeflow Jobs | SQL-only, no external orchestrator |
| Dynamic Tables | Declarative, target lag | LDP materialized views / streaming tables | Lag-driven scheduling over a DAG |
| Snowpipe | Event-driven serverless COPY | Auto Loader | Dedupe metadata in the pipe (14 days) |
| Snowpipe Streaming | Row channels + offset tokens | Structured Streaming / Zerobus | Push API, no files |
| Iceberg tables | Snowflake or external catalog | UC + Delta/UniForm | Open format, catalog is the new lock-in (P11) |
| Sharing | In-region metadata grant | Delta Sharing | Closed + live policies vs open protocol |
| Snowpark | DataFrame → SQL pushdown | PySpark | No cluster; less low-level control |
| Hybrid / Postgres | Row store / managed Postgres | Lakebase | OLTP next to the warehouse |

| Loading path | Unit | Trigger | Compute | Typical latency | Dedupe mechanism |
|---|---|---|---|---|---|
| `COPY INTO` | Files | You (schedule/task) | Your warehouse | Batch interval | Table load metadata, 64 days |
| Snowpipe | Files | Cloud event / REST | Serverless | ~1 minute | Pipe metadata, 14 days |
| Snowpipe Streaming | Rows | Client SDK / REST | Serverless | ~5–10 seconds | Offset token per channel |

| Incremental transform | You write | Scheduling | Handles joins | Best for |
|---|---|---|---|---|
| Streams + Tasks | Imperative MERGE logic | Schedule / trigger | Yes (your code) | Custom semantics, SCD2, side effects |
| Dynamic Tables | A SELECT | Target lag over a DAG | Yes | Declarative multi-step transforms |
| Materialized view | A single-table SELECT | Always current | No | Accelerating one hot table |

## Interview questions

**Q: Time Travel vs Fail-safe?**

Both come from keeping superseded immutable micro-partitions instead of deleting them (P3). Time Travel is user-facing: you can query, clone or undrop for 1 day by default, up to 90 days on Enterprise for permanent tables. Fail-safe follows it for a fixed 7 days, and only Snowflake Support can reach it, on a best-effort basis. It is a catastrophe backstop, not a recovery plan. Both cost storage, and that cost scales with churn, not size, so high-churn rebuildable tables should be transient with low retention.

**Q: When do you choose transient over permanent tables?**

When the data can be rebuilt from upstream: staging, intermediate models and dev copies. Transient tables skip Fail-safe and cap Time Travel at 1 day, which can cut storage several-fold on tables rewritten often. Keep raw landing data and systems of record permanent, because they are your replay source (P5). The failure mode is making raw transient and then discovering, after a bad delete, that there is no history to recover.

**Q: How does zero-copy cloning work and how would you use it?**

A clone copies the metadata list of micro-partition pointers, so it is near-instant and costs nothing until either side writes. Each write then creates new micro-partitions owned by that side alone. I use it for production-sized dev environments, for backfills (clone, backfill, validate, `SWAP WITH`), and with `AT(...)` for point-in-time debugging. Watch for grants not carrying over on table clones without `COPY GRANTS`, tasks starting suspended, and long-lived clones pinning old storage. It is also not DR, because it shares the same region and account.

**Q: How do Streams give you exactly-once-ish processing?**

A stream is a stored offset, not a buffer. Its offset advances only when a DML statement that reads it commits, and inside a transaction it is repeatable-read. Consuming the delta and writing the result are therefore one atomic step: a failure rolls both back, and a retry sees the same delta (P6). It is "ish" because upstream can still send duplicates, so the MERGE must be keyed and order-aware. Give each consumer its own stream.

**Q: What happens if a stream goes stale, and how do you prevent it?**

If the offset falls outside the source table's retention, the versions needed to compute the delta are purged, and the stream becomes unusable. Snowflake extends retention up to `MAX_DATA_EXTENSION_TIME_IN_DAYS` for unconsumed streams, but that is a buffer, not a guarantee. Prevent it by consuming regularly (triggered tasks), alerting on `STALE_AFTER`, and setting adequate retention on CDC sources. Recovery means recreating the stream and reconciling with a full diff, which is expensive, so treat staleness as an incident.

**Q: Streams + Tasks vs Dynamic Tables?**

Dynamic tables are declarative. You state the result and a freshness target, and Snowflake works out incremental refresh and scheduling across the DAG. Choose them by default for SQL transformations with joins and multiple steps. Streams + Tasks are imperative, so you own the merge, ordering and error handling. Choose them for custom SCD2, procedural logic, side effects or calls to external systems. The dynamic table risk is a query that can't be maintained incrementally silently refreshing in full on a large table, so check the refresh mode and `DYNAMIC_TABLE_REFRESH_HISTORY`.

**Q: Dynamic tables vs materialized views?**

Snowflake materialized views are single-table with no joins, always current, and maintained by a serverless service. They are an acceleration structure for one hot query pattern. Dynamic tables are pipeline building blocks: joins, aggregations and chains of tables, refreshed to a chosen lag on your warehouse. If you need joins, or you are building a pipeline, use dynamic tables. If you need transparent speed-up of one table's aggregate, use a materialized view.

**Q: Snowpipe vs Snowpipe Streaming vs COPY?**

All three are idempotent loaders that differ in unit and trigger. COPY loads files in batches on your warehouse and remembers loaded files for 64 days. Snowpipe is event-driven serverless COPY with about a minute of latency, deduped by pipe metadata for 14 days, and it suffers with tiny files. Snowpipe Streaming pushes rows over channels, tracks offset tokens for exactly-once delivery from replayable sources, and lands data in seconds. Pick the least fresh option the decision tolerates (P7): COPY for daily batches, Snowpipe when files already exist, Streaming for Kafka and event sources.

**Q: How do you get exactly-once delivery from Kafka into Snowflake?**

Use the Kafka connector (version 4 is built on high-performance Snowpipe Streaming). It maps partitions to channels and commits Kafka offsets as offset tokens. On restart it reads the last committed token and resumes from the next offset, so neither a crash nor a rebalance duplicates or loses records (P5/P6). Duplicates the producer itself sends are not covered, so keep a business-key dedupe downstream. Details in [§11](11-stream-processing.md).

**Q: How does Snowflake data sharing work without copying data?**

Within a region, provider and consumer accounts sit on the same storage and metadata layer. A share is just grants, so the consumer's warehouse reads the provider's micro-partitions in place (P2, P3). The provider pays storage, the consumer pays compute, and updates and revocations take effect instantly. Cross-region requires physical replication, either through database replication or listing auto-fulfillment, with egress and storage costs. Compared with Delta Sharing's open, file-URL protocol, Snowflake gives up reach in exchange for live policy enforcement and no exposed files.

**Q: When would you use Iceberg tables in Snowflake?**

When more than one engine needs to own or read the same data: Spark or Trino alongside Snowflake, a lakehouse already on Iceberg, or a requirement that data stay in your bucket in an open format (P11). Snowflake-managed Iceberg gives near-native features while external engines read and write through Horizon's Iceberg REST endpoint. Externally managed tables, or a catalog-linked database, let another catalog stay the system of record. Don't use Iceberg when Snowflake is the only engine: native tables get the deepest optimization and simplest operations, and the real lock-in decision becomes which catalog you trust.

**Q: What is Snowpark and why not just use Spark?**

Snowpark is a lazy DataFrame API that compiles to SQL and runs on Snowflake warehouses. Python UDFs and stored procedures cover what SQL can't express. Choosing it over Spark avoids exporting data, a second governance perimeter and cluster operations (P1, P9, P10). Spark remains better for low-level distributed control, streaming, arbitrary libraries and a lakehouse-first estate. If the data and the team already live in Snowflake, Snowpark removes a hop. If the platform is a lakehouse, it is the wrong center of gravity.

**Q: How would you build an incremental pipeline entirely in Snowflake?**

Land raw data with Snowpipe or Snowpipe Streaming into permanent, append-only raw tables, which are the replay source (P5). For bronze-to-silver logic with custom semantics, put an append-only stream on raw and a triggered or scheduled task that runs a keyed, deduplicating MERGE (P6). For declarative downstream models, use dynamic tables with `DOWNSTREAM` lag on intermediates and an SLA-based lag on the marts. Make staging transient, test changes on a clone, and monitor `TASK_HISTORY`, `DYNAMIC_TABLE_REFRESH_HISTORY` and stream staleness.

**Q: Why is a warehouse vendor shipping Postgres, hybrid tables and Openflow?**

Data originates in OLTP systems, and moving it between write-optimized and read-optimized stores is the core cost (P1). Owning both ends removes third-party ELT hops and gives AI apps a governed, low-latency store next to analytics. Hybrid tables are a constrained row store inside Snowflake. Snowflake Postgres is real Postgres when apps need its semantics. Openflow is NiFi-based ingestion. Be sceptical about using hybrid tables for high-throughput OLTP, and weigh the lock-in of putting every tier with one vendor.

## Takeaways
- Immutable micro-partitions + versioned metadata make Time Travel, cloning, streams and sharing metadata operations (P3).
- Time Travel: 1 day default, up to 90 days (Enterprise, permanent). Fail-safe: 7 days, support-only. Storage cost scales with churn.
- Clone for dev, backfills and debugging. It is not DR, and long-lived clones pin storage.
- A stream is an offset that advances only when a DML statement commits. Consume it before it goes stale.
- Use dynamic tables for declarative SQL pipelines, Streams + Tasks for custom semantics, and MVs for single-table acceleration.
- COPY (64-day load metadata) → Snowpipe (event-driven, ~1 minute, 14-day pipe metadata) → Snowpipe Streaming (rows, offset tokens, seconds).
- In-region sharing is zero-copy and closed. Delta Sharing is open. Cross-region always means replication.
- Snowflake Open Catalog accepts no new customers. Horizon Catalog is now Snowflake's Iceberg REST endpoint.
- Convergence (hybrid tables, Postgres, Openflow) is Snowflake moving up and down the P1 pipeline.

## Sources
- https://docs.snowflake.com/en/user-guide/data-time-travel
- https://docs.snowflake.com/ja/user-guide/tables-temp-transient
- https://docs.snowflake.com/en/user-guide/object-clone
- https://docs.snowflake.com/sql-reference/sql/create-clone
- https://docs.snowflake.com/en/sql-reference/account-usage/streams
- https://docs.snowflake.com/en/user-guide/tasks-triggered
- https://docs.snowflake.com/release-notes/2024/8_27
- https://docs.snowflake.com/sql-reference/sql/create-dynamic-table
- https://docs.snowflake.com/en/user-guide/dynamic-tables-refresh
- https://docs.snowflake.com/en/user-guide/dynamic-tables/migrate-streams-tasks
- https://docs.snowflake.com/user-guide/dynamic-tables-best-practices
- https://docs.snowflake.com/user-guide/data-load-considerations-load
- https://docs.snowflake.com/en/user-guide/data-load-considerations-prepare
- https://docs.snowflake.com/user-guide/data-load-snowpipe-manage
- https://docs.snowflake.com/user-guide/snowpipe-streaming-high-performance-overview
- https://docs.snowflake.com/en/release-notes/2025/other/2025-09-23-snowpipe-streaming-high-performance-architecture
- https://docs.snowflake.com/release-notes/2025/other/2025-11-10-snowpipe-streaming-gcp-ga
- https://docs.snowflake.com/en/connectors/kafkahp/about
- https://docs.snowflake.com/ko/release-notes/2026/other/2026-04-20-kafka-connector-v4-ga
- https://docs.snowflake.com/en/user-guide/tables-iceberg-catalog-linked-database
- https://docs.snowflake.com/release-notes/2025/other/2025-10-17-iceberg-external-writes-cld-ga
- https://docs.snowflake.com/en/user-guide/tables-iceberg-query-using-external-query-engine-snowflake-horizon
- https://docs.snowflake.com/release-notes/2026/other/2026-05-26-tables-iceberg-query-using-external-query-engine-snowflake-horizon-writes-ga
- https://docs.snowflake.com/release-notes/2026/other/2026-08-18-externally-managed-iceberg-tables-horizon-irc-preview
- https://docs.snowflake.com/en/user-guide/opencatalog/overview
- https://docs.snowflake.com/en/user-guide/data-sharing-reader-create.html
- https://docs.snowflake.com/user-guide/data-share-replication
- https://docs.snowflake.com/en/collaboration/provider-listings-auto-fulfillment
- https://docs.snowflake.com/release-notes/2026/other/2026-02-05-notebooks-in-workspaces
- https://docs.snowflake.com/user-guide/snowflake-cortex/llm-functions
- https://docs.snowflake.com/release-notes/2025/other/2025-11-04-cortex-aisql-operators-ga
- https://docs.snowflake.com/en/release-notes/2025/other/2025-11-04-cortex-agents
- https://docs.snowflake.com/release-notes/2025/other/2025-11-04-snowflake-intelligence
- https://docs.snowflake.com/release-notes/2024/other/2024-10-30-hybrid-tables-ga
- https://docs.snowflake.com/release-notes/2026/other/2026-02-24-snowflake-postgres-ga
- https://docs.snowflake.com/release-notes/2025/other/2025-05-30-openflow
- https://docs.snowflake.com/en/release-notes/openflow
- https://docs.snowflake.com/user-guide/replication-intro
