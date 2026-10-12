# 06. Snowflake II: Data Features

> As of October 2026. Tags: **(GA)** generally available · **(Preview)** public/private preview · **(⚠ verify)** not confirmed against official docs.

## Why this exists

Snowflake stores tables as immutable micro-partitions in object storage, plus a metadata service that records which micro-partitions make up each table version ([§05](05-snowflake-architecture.md)). Because files are never overwritten, old versions (Time Travel), copies (cloning), change sets (Streams) and cross-account access (Sharing) become metadata operations rather than data movement (P2, P3). The rest of the feature set fills in what a warehouse lacks on its own. Ingestion is covered by COPY, Snowpipe and Openflow, incremental transformation by Tasks and Dynamic Tables, non-SQL code by Snowpark and containers, open formats by Iceberg, and operational and AI serving by hybrid tables, Postgres and Cortex. Snowflake's bet is that one managed, SQL-first service with governance built in beats assembling those parts yourself (P10).

## Core concepts

### The one idea underneath: a version is a list of pointers

```
table T (metadata service)
 v1: [mp1][mp2][mp3]
 v2: [mp1][mp2'][mp3]        UPDATE rewrote mp2 -> mp2' (copy-on-write)
 v3: [mp1][mp2'][mp3][mp4]   INSERT added mp4
      mp2 inactive: kept for Time Travel -> Fail-safe -> purged

 clone C of T@v3 = same pointer list, zero bytes copied
 stream S @ v1   = diff(v1, v3)
 share to acct B = B's warehouse reads mp1..mp4 in place
```

### Time Travel

DML never edits a micro-partition. It writes new micro-partitions and commits a new version. Time Travel keeps the superseded micro-partitions for `DATA_RETENTION_TIME_IN_DAYS` and lets queries resolve an older version (P3).
- **Syntax:** `AT(TIMESTAMP => ...)`, `AT(OFFSET => -3600)` and `BEFORE(STATEMENT => '<query_id>')`, which undoes a bad statement. `UNDROP TABLE|SCHEMA|DATABASE` restores a dropped object, and fails if an object with that name exists again. For a restore, run `CREATE TABLE t_fix CLONE t BEFORE(...)` and then swap it in.
- **Retention (know exactly):** the default is 1 day and Standard Edition allows at most 1 day. Enterprise and above allow 0–90 days on permanent objects. Transient and temporary objects allow 0 or 1 day.
- **Cost:** you pay storage for superseded micro-partitions until retention and Fail-safe both expire. Updating one row rewrites its whole micro-partition, so high-churn tables (wide MERGEs, nightly `CREATE OR REPLACE`) can carry several times their active size in history. Check `TABLE_STORAGE_METRICS`, which reports `TIME_TRAVEL_BYTES`, `FAILSAFE_BYTES` and `RETAINED_FOR_CLONE_BYTES`.
- **Databricks:** Delta `VERSION AS OF` and `RESTORE`. In Delta, `VACUUM` settings set the horizon. In Snowflake a parameter sets it and the service purges, so there is no VACUUM to forget.

### Fail-safe and table types

When Time Travel expires on a **permanent** table, its history enters a **7-day Fail-safe** that you can't configure. You cannot query it. Only Snowflake Support can recover from it, on a best-effort basis that takes hours to days. It is a catastrophe backstop that costs storage, not an operational tool. **Databricks:** no equivalent beyond bucket versioning or soft delete.

| Type | Lifetime | Time Travel | Fail-safe | Use for |
|---|---|---|---|---|
| Permanent | Until dropped | ≤1 day (Standard), ≤90 days (Enterprise+) | 7 days | Systems of record, raw replay sources |
| Transient | Until dropped | 0 or 1 day | None | Staging and rebuildable models, dev |
| Temporary | Session | 0 or 1 day | None | Session scratch |

If upstream data can rebuild a table, make it transient. A transient database or schema makes its tables transient.

### Zero-copy cloning

`CREATE {DATABASE|SCHEMA|TABLE} x CLONE y [AT|BEFORE(...)]` copies the **pointer list**, not bytes, so it takes time in proportion to metadata, not data volume. After that, each side's writes create micro-partitions that only that side owns, and you pay only for the divergence. A shared micro-partition stays alive while any clone still references it, so a long-lived clone of a high-churn table quietly keeps old storage alive.
- **Uses:** production-sized dev and test environments. Safe backfills: clone, backfill, validate, then `ALTER TABLE ... SWAP WITH` (an atomic metadata swap, P6). Point-in-time debugging with `CLONE ... AT(TIMESTAMP => ...)`.
- **Gotchas:**
  - Grants: a cloned database or schema doesn't inherit grants on the container itself, though child objects keep theirs. A table clone gets no grants without `COPY GRANTS`.
  - Tasks in a clone start suspended.
  - A cloned stream's unconsumed rows are inaccessible.
  - Pipes on internal stages are not cloned, and internal named stages need `INCLUDE INTERNAL STAGES`.
  - Load metadata isn't cloned, so a COPY can reload files.
  - A clone is not DR: it shares the account, region and storage.
- **Databricks:** `SHALLOW CLONE`. Snowflake reference-counts storage, so a clone can't break. A Delta shallow clone breaks if `VACUUM` on the source removes files it points to.

### Streams

A stream holds **no data**. It is an **offset** (a table version) plus the ability to compute the difference between that version and now, using change tracking that is switched on for the source. Each returned row carries `METADATA$ACTION`, `METADATA$ISUPDATE` and `METADATA$ROW_ID`.
- **Standard:** net inserts, updates and deletes. A row inserted and deleted inside the window disappears. An update shows up as a DELETE plus an INSERT.
- **Append-only:** inserts only. Cheaper, because it never reconciles deletes, which makes it the right choice for landing tables.
- **Insert-only:** for external tables and externally managed Iceberg tables, where Snowflake only sees new files.

**The offset advances only when a DML statement that reads the stream commits.** A plain `SELECT` doesn't move it. Inside a transaction every statement sees the same snapshot, and the offset moves at `COMMIT`, so consuming the delta and writing the result succeed or fail together. That is exactly-once-*ish* (P6): no delta is lost or applied twice, but upstream can still resend data, so key your MERGE on a business key. Each consumer needs its own stream (P5).

**Staleness:** if the offset falls outside the source's retention, the history needed for the delta is gone and the stream is **stale**. You must recreate it and reconcile with a full comparison. Snowflake extends retention for unconsumed streams up to `MAX_DATA_EXTENSION_TIME_IN_DAYS` (default 14 days). Monitor `STALE_AFTER`.
**Databricks:** Change Data Feed. There the offset lives in the consumer's checkpoint. Here it is a database object you grant, clone and monitor.

### Tasks

Tasks schedule one SQL statement or procedure call, either on a user-managed warehouse or **serverless**. Serverless suits short, frequent jobs where warehouse minimum billing and idle time would dominate ([§05](05-snowflake-architecture.md)).
- **Task graphs:** a root task with an interval or CRON schedule, children declared with `AFTER`, and an optional finalizer task.
- **`WHEN SYSTEM$STREAM_HAS_DATA(...)`:** evaluated without starting a warehouse, so a poll that finds nothing is nearly free.
- **Triggered tasks (GA):** omit `SCHEDULE` and the task runs when the stream has data. That happens at most every 30 seconds by default, tunable down to 10 seconds. A serverless triggered task needs `TARGET_COMPLETION_INTERVAL`.
- **Failure mode:** tasks are created **suspended**. If nobody runs `ALTER TASK ... RESUME`, the task never runs.
- **Databricks:** Lakeflow Jobs.

Streams + Tasks is the imperative incremental pipeline, where you own the merge logic:

```sql
CREATE STREAM raw_orders_s ON TABLE raw_orders APPEND_ONLY = TRUE;
CREATE TASK merge_orders WAREHOUSE = etl_wh SCHEDULE = '5 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('raw_orders_s')
AS MERGE INTO orders t USING (
     SELECT * FROM raw_orders_s
     QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) = 1) s
   ON t.order_id = s.order_id
   WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET ...
   WHEN NOT MATCHED THEN INSERT ...;
ALTER TASK merge_orders RESUME;
```

### Dynamic Tables

You declare a `SELECT` and a **`TARGET_LAG`**, and Snowflake schedules the refreshes across the dependency graph. Each refresh reads base-table changes, using the same change tracking as streams. It then merges a computed delta (**INCREMENTAL**) or recomputes everything (**FULL**), on the warehouse you assign.
- **Lag:** the minimum is 1 minute, measured against the base tables at the root of the graph. **`DOWNSTREAM`** on an intermediate table means it refreshes only when consumers need it, so only the leaf table's SLA drives the schedule.
- **`REFRESH_MODE`:** `AUTO` chooses once, at creation, and never revisits. `INCREMENTAL` makes creation fail if the query can't be maintained incrementally.
- **What blocks incremental:** non-deterministic functions, unsupported constructs (see the docs' supported-SQL list), and change ratios so high that FULL is cheaper.
- **Versus materialized views:** Snowflake MVs (Enterprise+) are single-table with no joins and always current. Use one to speed up a single hot query, not to build a pipeline.
- **When not to use them:** when you need procedural logic, side effects or custom SCD2 semantics, when you need sub-minute latency, or when a query silently falls back to FULL on a large table.
- **Databricks:** materialized views and streaming tables in Lakeflow pipelines (formerly DLT; see [§07](07-databricks-gap-fill.md)).

### Loading: COPY, Snowpipe, Snowpipe Streaming

All three solve the same problem: knowing what has already been loaded (P6).
- **`COPY INTO`:** batch loading on your warehouse, parallel across files, which is why the guidance is files of **100–250 MB compressed**. The table keeps **load metadata for 64 days**, so a rerun skips files it already loaded. `FORCE = TRUE` overrides that and duplicates data.
- **Snowpipe:** a `PIPE` that wraps a COPY statement. It is triggered by cloud event notifications (`AUTO_INGEST`) or the REST API and runs serverless. Latency is about a minute and load order is not guaranteed. Dedupe metadata lives **in the pipe for 14 days**, so recreating a pipe erases it. Billing is per GB with no per-file charge, but file size still affects latency and throughput, so aim for 100–250 MB compressed or a new file about once per minute.
- **Snowpipe Streaming:** rows, no files. The SDK opens **channels** on a table, and each batch carries an **offset token**. After a restart the client reads the last committed token and resumes, which gives exactly-once delivery from sources that can be replayed by offset (P5). That is the **Named Channels** mode; **Elastic Channels (GA, September 2026)** let Snowflake manage channels but are at-least-once and unordered. The **high-performance architecture** (GA: AWS September 2025, Azure and GCP November 2025) adds a server-side `PIPE` for transforms and validation, a Rust-core SDK plus a REST API, and throughput pricing. Order of magnitude: the docs now cite up to 20 GB/s per table (10 GB/s at GA) and as little as 5 seconds from ingest to query. The classic architecture is under advance notice of deprecation: a formal announcement was planned for mid-2026, followed by an 18-month sunset.
- **Kafka connector:** version 4.0 (GA April 2026) is built on high-performance Snowpipe Streaming, with Kafka offsets carried as offset tokens. Earlier versions could also stage files for Snowpipe. See [§11](11-stream-processing.md).
- **Databricks:** COPY INTO and Auto Loader cover files. Structured Streaming or Zerobus Ingest (GA) cover rows.

### External tables vs Iceberg tables

**External tables** are read-only, schema-on-read metadata over files in your bucket, with weak pruning and no DML. **Iceberg tables** have largely replaced them. Data and metadata usually sit in your storage (an `EXTERNAL VOLUME`), billed by your cloud provider; Snowflake storage for Iceberg tables (GA, June 2026, AWS and Azure) is the managed alternative. A **Snowflake-managed** table (`CATALOG = 'SNOWFLAKE'`) gets full DML, and external engines can read **(GA, February 2026)** and write **(GA, May 2026)** it through Horizon Catalog's Iceberg REST API, which integrates Apache Polaris. An **externally managed** table uses a `CATALOG INTEGRATION` to Glue or any Iceberg REST catalog, with writes **(GA)** for REST catalogs. A **catalog-linked database (GA, October 2025)** mirrors a whole remote catalog automatically. **Snowflake Open Catalog** (managed Polaris) no longer accepts new customers, who are pointed to Horizon Catalog instead. **Databricks:** Unity Catalog with UniForm and its Iceberg REST endpoint. See [§04](04-table-formats-catalogs.md).

### Secure Data Sharing

A **share** is a set of grants (on databases, tables, secure views and secure UDFs) plus a list of consumer accounts. The consumer mounts a **read-only database** from it, and *its own warehouse* reads the provider's micro-partitions in place. In-region accounts share the storage and metadata layer, so nothing is copied, updates appear at commit, and revoking access is instant (P2, P3). The provider pays storage and the consumer pays compute.
- **Reader accounts:** for consumers without Snowflake. The provider creates and pays for them, compute included, and they can only read that provider's data.
- **Listings:** add terms and analytics. A listing can be private, or public on the **Marketplace**.
- **Cross-region and cross-cloud:** storage isn't shared, so the data must be replicated, either through database replication plus a share or through **Cross-Cloud Auto-Fulfillment** on a listing. You pay for egress and target storage.
- **Data Clean Rooms:** joins on overlapping data through approved query templates only, without exposing raw rows (GA; data providers need Enterprise Edition).
- **Contrast with Delta Sharing (open):** the Delta Sharing server issues short-lived pre-signed file URLs, so any client can read and reach extends beyond Databricks. Snowflake's closed model needs Snowflake (or a reader account) on both ends. In exchange, row and masking policies apply live and no files are exposed (P11).

### Snowpark, containers, notebooks

- **Snowpark (Python, Java, Scala):** a lazy DataFrame API that compiles to **SQL** and runs on a warehouse, so there is no cluster and no data export. Logic SQL can't express goes into UDFs, UDTFs, vectorized UDFs and stored procedures, which run sandboxed on warehouse nodes with Anaconda or PyPI packages (since June 2026 the shared PyPI repository is the default for new accounts and Python 3.14+). **pandas on Snowflake** translates the pandas API to SQL (GA, September 2024). **Snowpark Connect for Apache Spark** runs PySpark code on Snowflake (GA, November 2025).
- **Why it exists:** bring code to the data (P1), keep one governance perimeter (P9), and compete for Python and ML work (P10). **Limits:** it is not a general distributed runtime. There are no RDDs, no Structured Streaming and no arbitrary cluster libraries.
- **Snowpark Container Services:** runs OCI containers (services and jobs, GPUs available) on compute pools inside Snowflake's security boundary. **Databricks:** Apps and Model Serving.
- **Notebooks in Workspaces (GA, February 2026):** run on Container Runtime (built on SPCS). The warehouse-based notebooks are legacy, and new legacy notebooks can't be created since September 2026.

### Cortex AI

**AI Functions** are LLM calls as SQL operators, governed by grants. AI_COMPLETE, AI_CLASSIFY, AI_EMBED, AI_SIMILARITY, AI_TRANSCRIBE, AI_EXTRACT, AI_SENTIMENT and AI_TRANSLATE are **(GA)**, and AI_FILTER and AI_AGG joined them in January 2026. **Cortex Search** is managed hybrid vector-plus-keyword retrieval for RAG **(GA, October 2024)**. **Cortex Analyst** is text-to-SQL grounded in semantic views (GA date ⚠ verify); since August 2026 Snowflake recommends Cortex Agents, which uses Analyst as its structured-data tool, instead of calling Analyst directly. **Cortex Agents** and **Snowflake Intelligence**, the business-user agent interface, have both been **(GA)** since November 2025. **Databricks:** AI Functions, Vector Search, Genie and Agent Bricks. See [§18](18-ai-on-data-platform.md).

### Horizon (governance)

Horizon is the umbrella for masking policies, row access policies, object tags with tag-based masking, classification, `ACCESS_HISTORY`, lineage, data-quality metric functions and Horizon Catalog's Iceberg REST endpoint. Policies attach to the data object and are enforced wherever it is queried, shares included (P9). **Databricks:** Unity Catalog. See [§12](12-governance-security.md).

### Convergence: OLTP and ingestion

**Why (P1):** every pipeline ends in the warehouse, and the costly, fragile hops are getting data in from operational systems and back out to applications. Owning those hops removes third-party tools and gives AI apps low-latency state next to governed analytics.
- **Hybrid tables (Unistore):** a row store with a required primary key, enforced constraints, secondary indexes and row-level locking, built for point lookups and small transactions. **(GA on AWS since October 2024 and on Azure since October 2025.)** It is not a high-throughput OLTP engine.
- **Snowflake Postgres (GA, February 2026, selected AWS and Azure regions):** real managed PostgreSQL on dedicated VMs, building on the 2025 Crunchy Data acquisition. **Databricks:** Lakebase.
- **Openflow:** managed ingestion built on Apache NiFi, with connectors for SaaS, CDC, Kafka and unstructured data. **(GA May 2025 for BYOC; Snowflake Deployments on SPCS GA November 2025.)** **Databricks:** Lakeflow Connect.

### Replication and business continuity

**Replication groups** incrementally copy databases, shares and account objects to other regions or clouds. **Failover groups** (Business Critical and above) promote a secondary account. **Client Redirect** (also Business Critical) repoints clients through a connection URL. Database and share replication work on all editions; account-object replication needs Business Critical. RPO is roughly the replication interval, and RTO is promotion plus redirect. **Databricks:** managed disaster recovery (Preview since June 2026, gated) replicates Unity Catalog metadata, managed tables and workspace assets behind a stable URL; otherwise DR is DIY. See [§14](14-reliability-dataops.md).

## Comparison

| Snowflake feature | Databricks equivalent | Key difference |
|---|---|---|
| Time Travel + Fail-safe | Delta time travel | Parameter + auto-purge vs `VACUUM`; Fail-safe has no Delta equivalent |
| Zero-copy clone | Shallow / deep clone | Reference-counted, can't break from source cleanup |
| Streams | Change Data Feed | Offset is a database object, not consumer checkpoint state |
| Tasks | Lakeflow Jobs | SQL-native, serverless option |
| Dynamic Tables | LDP materialized views / streaming tables | Target-lag scheduling over a DAG |
| Snowpipe / Streaming | Auto Loader / Structured Streaming | Pipe-held dedupe; offset tokens |
| Secure Data Sharing | Delta Sharing | Closed, zero-copy, live policies vs open protocol |
| Snowpark | PySpark | SQL pushdown, no cluster, less control |
| Hybrid tables / Postgres | Lakebase | OLTP beside the warehouse |

| Loading | Unit | Compute | Latency | Dedupe |
|---|---|---|---|---|
| `COPY INTO` | Files | Your warehouse | Batch | Table metadata, 64 days |
| Snowpipe | Files | Serverless | ~1 minute | Pipe metadata, 14 days |
| Snowpipe Streaming | Rows | Serverless | ~5–10 seconds | Offset token per channel |

| Incremental transform | You write | Joins | Best for |
|---|---|---|---|
| Streams + Tasks | Imperative MERGE | Yes | Custom semantics, SCD2, side effects |
| Dynamic Tables | A SELECT + lag | Yes | Declarative multi-step SQL pipelines |
| Materialized view | Single-table SELECT | No | Accelerating one hot table |

## Interview questions

**Q: Time Travel vs Fail-safe?**

Both keep superseded immutable micro-partitions instead of deleting them (P3). Time Travel is user-facing (query, clone, undrop): 1 day by default, up to 90 days on Enterprise for permanent tables. Fail-safe follows it for a fixed 7 days, reachable only by Snowflake Support, and is a backstop, not a recovery plan. Both cost storage in proportion to churn, which is why rebuildable high-churn tables should be transient.

**Q: When do you choose transient over permanent tables?**

When upstream data can rebuild the table, as with staging, intermediate models and dev copies. Transient tables skip Fail-safe and cap Time Travel at 1 day, which can cut storage several-fold on frequently rewritten tables. Keep raw landing data and systems of record permanent, because they are your replay source (P5).

**Q: How does zero-copy cloning work and how would you use it?**

A clone copies the micro-partition pointer list, so it is near-instant and free until either side writes. Writes then create new micro-partitions owned only by the writer. I use clones for production-sized dev environments, for backfills (clone, backfill, validate, `SWAP WITH`), and for point-in-time debugging with `AT(...)`. Watch for missing grants on table clones, tasks starting suspended, and long-lived clones pinning storage. A clone is not DR.

**Q: How do Streams give you exactly-once-ish processing?**

A stream is a stored offset that advances only when a DML statement reading it commits, so consuming the delta and writing the result are atomic: a failure rolls both back and a retry sees the same delta (P6). It is "ish" because upstream systems can resend data, so the MERGE must still be keyed and order-aware.

**Q: What happens if a stream goes stale, and how do you prevent it?**

Once the offset falls outside the source's retention, the history needed for the delta is purged and the stream becomes unusable. Recovering means recreating it and running a full reconciliation. Prevent it by consuming regularly (triggered tasks), alerting on `STALE_AFTER`, and setting adequate retention on CDC sources. The retention extension Snowflake applies is a buffer, not a guarantee.

**Q: Streams + Tasks vs Dynamic Tables?**

Dynamic tables are declarative: you state the result and a freshness target, and Snowflake handles incremental refresh and scheduling across the DAG. They are my default for SQL transformations. Streams + Tasks are imperative, which is right for custom SCD2, procedural logic or side effects. The dynamic-table risk is a query that silently refreshes in FULL mode on a large table, so check its refresh mode and refresh history.

**Q: Dynamic tables vs materialized views?**

Snowflake MVs are single-table with no joins, always current and serverless-maintained, so they are an acceleration structure for one hot query. Dynamic tables are pipeline building blocks that support joins and chains, refreshed to a chosen lag on your warehouse.

**Q: Snowpipe vs Snowpipe Streaming vs COPY?**

All three are idempotent loaders that differ in unit and trigger. COPY loads batches of files on your warehouse and remembers them for 64 days. Snowpipe is event-driven serverless COPY with about a minute of latency and 14-day pipe dedupe. Snowpipe Streaming pushes rows with offset tokens and lands data in seconds. Buy only the freshness the decision needs (P7).

**Q: How do you get exactly-once delivery from Kafka into Snowflake?**

The Kafka connector (version 4, built on high-performance Snowpipe Streaming) maps partitions to channels and commits Kafka offsets as offset tokens. On restart it resumes after the last committed token, so neither crashes nor rebalances duplicate or drop records (P5/P6). Duplicates sent by the producer still need a business-key dedupe downstream.

**Q: How does Snowflake data sharing work without copying data?**

In-region accounts share the storage and metadata layer, so a share is only grants, and the consumer's warehouse reads the provider's micro-partitions in place (P2, P3). The provider pays storage and the consumer pays compute, and both updates and revocations take effect instantly. Cross-region sharing needs replication or listing auto-fulfillment. Compared with Delta Sharing's open file-URL protocol, it gives up reach in exchange for live policy enforcement.

**Q: When would you use Iceberg tables in Snowflake?**

When several engines must read or write the same data, or the data must stay in your bucket in an open format (P11). Snowflake-managed Iceberg tables keep near-native features while other engines use Horizon's Iceberg REST endpoint. Externally managed tables, or a catalog-linked database, let another catalog remain the system of record. If Snowflake is the only engine, native tables are simpler and better optimized.

**Q: What is Snowpark and why not just use Spark?**

Snowpark is a DataFrame API compiled to SQL and run on warehouses, with Python UDFs and stored procedures for what SQL can't express. It avoids data export, a second governance perimeter and cluster operations (P1, P9, P10). Spark remains better for low-level distributed control, streaming and lakehouse-centred estates.

**Q: How would you build an incremental pipeline entirely in Snowflake?**

Land data with Snowpipe or Snowpipe Streaming into permanent, append-only raw tables, which serve as the replay source (P5). For custom semantics, put an append-only stream on raw and have a triggered task run a keyed, deduplicating MERGE (P6). Build the downstream models as dynamic tables, with `DOWNSTREAM` lag on intermediates and SLA-based lag on the marts. Make staging transient, test changes on a clone, and monitor task history, refresh history and stream staleness.

**Q: Why is a warehouse vendor shipping Postgres, hybrid tables and Openflow?**

Data originates in OLTP systems, and moving it between write-optimized and read-optimized stores is the core cost (P1). Owning both ends removes ELT hops and gives AI apps governed low-latency state. Be sceptical of hybrid tables for high-throughput OLTP, and weigh the lock-in of putting every tier with one vendor.

## Takeaways
- Immutable micro-partitions + versioned metadata make Time Travel, cloning, streams and sharing metadata operations (P3).
- Time Travel: 1 day default, up to 90 days (Enterprise, permanent). Fail-safe: 7 days, support-only. Storage cost tracks churn.
- Clone for dev, backfills and debugging. It is not DR, and long-lived clones pin storage.
- A stream is an offset that moves only when a DML statement commits. Consume it before it goes stale.
- Use dynamic tables for declarative pipelines, Streams + Tasks for custom semantics, and MVs for single-table acceleration.
- COPY (64-day dedupe) → Snowpipe (~1 minute, 14-day pipe dedupe) → Snowpipe Streaming (rows, offset tokens, seconds).
- In-region sharing is zero-copy and closed. Delta Sharing is open. Cross-region always means replication.
- Open Catalog accepts no new customers. Horizon Catalog is now Snowflake's Iceberg REST endpoint.

## Sources
- https://docs.snowflake.com/en/user-guide/data-time-travel
- https://docs.snowflake.com/en/user-guide/tables-temp-transient
- https://docs.snowflake.com/en/user-guide/data-failsafe
- https://docs.snowflake.com/en/sql-reference/info-schema/table_storage_metrics
- https://docs.snowflake.com/en/sql-reference/parameters
- https://docs.snowflake.com/en/user-guide/object-clone
- https://docs.snowflake.com/sql-reference/sql/create-clone
- https://docs.snowflake.com/en/sql-reference/account-usage/streams
- https://docs.snowflake.com/en/user-guide/streams-intro
- https://docs.snowflake.com/en/user-guide/tasks-triggered
- https://docs.snowflake.com/en/sql-reference/sql/create-task
- https://docs.snowflake.com/release-notes/2024/8_27
- https://docs.snowflake.com/sql-reference/sql/create-dynamic-table
- https://docs.snowflake.com/en/user-guide/dynamic-tables-refresh
- https://docs.snowflake.com/en/user-guide/dynamic-tables/migrate-streams-tasks
- https://docs.snowflake.com/user-guide/dynamic-tables-best-practices
- https://docs.snowflake.com/en/user-guide/views-materialized
- https://docs.snowflake.com/user-guide/data-load-considerations-load
- https://docs.snowflake.com/en/user-guide/data-load-considerations-prepare
- https://docs.snowflake.com/en/user-guide/data-load-snowpipe-intro
- https://docs.snowflake.com/user-guide/data-load-snowpipe-manage
- https://docs.snowflake.com/user-guide/snowpipe-streaming-high-performance-overview
- https://docs.snowflake.com/en/user-guide/snowpipe-streaming/data-load-snowpipe-streaming-overview
- https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-classic-deprecation
- https://docs.snowflake.com/en/release-notes/2025/other/2025-09-23-snowpipe-streaming-high-performance-architecture
- https://docs.snowflake.com/en/release-notes/2025/other/2025-11-05-snowpipe-streaming-azure-ga
- https://docs.snowflake.com/release-notes/2025/other/2025-11-10-snowpipe-streaming-gcp-ga
- https://docs.snowflake.com/en/release-notes/2026/other/2026-09-15-snowpipe-streaming-elastic-channels-ga
- https://docs.snowflake.com/en/connectors/kafkahp/about
- https://docs.snowflake.com/en/release-notes/2026/other/2026-04-20-kafka-connector-v4-ga
- https://docs.databricks.com/aws/en/ingestion/zerobus-release-stages
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
