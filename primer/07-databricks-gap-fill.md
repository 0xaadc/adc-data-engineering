# 07. Databricks Gap-Fill: What's New or Renamed

> As of October 2026. Tags: **(GA)** generally available · **(Preview)** public/private preview · **(⚠ verify)** not confirmed against official docs.

## Why this exists

Between 2024 and 2026, Databricks renamed much of its product surface. It also shifted its defaults from "you tune it" to "the platform tunes it", through serverless compute, predictive optimization and liquid clustering. Interviewers use the current names. This file assumes you already know Spark, Delta, Unity Catalog (UC), jobs and clusters. It covers only what is new, renamed or less used, and for each item it explains why the feature exists and how it works.

## Core concepts

### Lakeflow: ingestion, transformation, orchestration

- **Lakeflow Connect: managed ingestion connectors.**
  - **Why it exists:** building and running your own SaaS and database connectors is undifferentiated work.
  - **How it works:** connectors read incrementally. Database sources use change data capture (CDC) from the transaction log (P5). Data lands as streaming tables on serverless compute.
  - **Availability:** varies by connector, from Beta to GA, so check each source.
- **Lakeflow pipelines**, formerly Delta Live Tables (DLT), and briefly Lakeflow (Spark) Declarative Pipelines.
  - **Why it exists:** hand-written streaming jobs tend to get checkpoints, ordering, retries and infrastructure wrong.
  - **How it works:** you declare datasets. The engine builds the dependency graph and runs it.
  - **Open-source core:** Databricks donated the core to Apache Spark as Apache Spark Declarative Pipelines (Spark 4.1+). Lakeflow pipelines extend it.
  - **New Python API:** `from pyspark import pipelines as dp`. `@dlt.table` becomes `@dp.table` for streaming tables or `@dp.materialized_view` for MVs, and `@dlt.view` becomes `@dp.temporary_view`. The old `dlt` module still works.
  - **Databricks-only extras:** expectations, and AUTO CDC (`dp.create_auto_cdc_flow`, formerly `APPLY CHANGES`).
- **Lakeflow Jobs**, formerly Databricks Jobs (Workflows), renamed Jun 2025. The orchestrator: task graphs, file-arrival and table-update triggers, for-each loops and conditional tasks.
- **Lakeflow Designer** **(GA)** Jun 2026. A visual, no-code canvas for building pipelines. It exists so analysts can build pipelines without leaving governance. The flows it produces are generated code, governed in UC.

**Streaming tables vs. materialized views (MVs)**

| | Streaming table | Materialized view |
|---|---|---|
| How it processes data | Each new source record once, tracked by a checkpoint | Keeps the whole query result correct |
| Upstream updates and deletes | Not reflected unless handled explicitly | Reflected on refresh |
| Cost | Cheap, incremental | Incremental where possible, full recompute where not |
| Typical use | Bronze ingestion | Joins, aggregations, silver/gold layers |

Choosing between them is a trade of correctness against cost (P6, P7).

**Expectations** are declarative data-quality rules on a dataset. Each rule chooses what happens to a failing row:
- `warn`: keep the row and record a metric;
- `drop`: remove the row;
- `fail`: stop the update.

Results land in the pipeline event log, where you can alert on them (P8).

### Layout and maintenance

- **Liquid clustering.**
  - **Problem with Hive-style partitioning:** the data layout is fixed at write time as directories. A wrong key produces small files or skew, and changing the key means rewriting the whole table.
  - **Problem with Z-order:** every `OPTIMIZE` rewrote whole partitions.
  - **How liquid clustering fixes both:** clustering keys live in table metadata. `OPTIMIZE` clusters only files that aren't clustered yet. `ALTER TABLE ... CLUSTER BY` changes keys without rewriting existing data.
  - **Constraint:** a table can't combine liquid clustering with partitioning or Z-order.
  - This is skipping work instead of doing it faster (P4).
- **Automatic liquid clustering (`CLUSTER BY AUTO`).** Databricks picks keys from the table's query history. It changes keys only when the predicted data-skipping savings exceed the cost of reclustering. Requirements: predictive optimization enabled, and Databricks Runtime (DBR) 15.4 LTS or later. **(GA)** since Jun 2025.
- **Predictive optimization.** Runs `OPTIMIZE`, `VACUUM` and `ANALYZE` automatically on UC managed tables, using telemetry to decide when. It runs on serverless compute that you pay for. It covers managed tables only, which is one reason Databricks pushes managed tables. On by default for all accounts: new ones since Nov 2024, existing ones rolled out from May 2025.
- **Deletion vectors.** Deletes become merge-on-read: `DELETE`, `UPDATE` and `MERGE` write a small bitmap marking removed rows instead of rewriting Parquet files. Readers apply the bitmap, and compaction later removes the rows physically. Writes get faster, and reads are slightly slower until compaction. Some external readers can't handle deletion vectors; for example, open-source sharing recipients need a recent connector (Spark connector 3.1+), and the table must be shared with history.

### Compute

- **Serverless compute** for notebooks, jobs, pipelines and SQL.
  - **How it works:** a Databricks-managed fleet. Startup takes seconds, the runtime is versionless and upgrades automatically, and VM cost is included in the DBU rate.
  - **What you give up:** choice of instance type, most Spark configs, and init-script customization.
  - **What you must set up:** network controls configured on the Databricks side.
  - **Cost tracking:** through system tables (P10).
- **SQL warehouse types:**
  - **Classic:** the base engine, with Photon.
  - **Pro:** adds newer engine features. Some features, such as Lakehouse Federation, require Pro or Serverless.
  - **Serverless:** Pro features plus fast startup and managed scaling. The default choice for BI concurrency.
- **Photon:** a vectorized C++ engine. It replaces JVM execution for the operators it supports and falls back to Spark for the rest, with no code changes.
  - **Helps:** scans, joins, aggregations, `MERGE` and writes on Delta or Parquet.
  - **Doesn't help:** Python or pandas UDFs, RDD code, tiny queries, or I/O-bound jobs.
  - **Cost:** on classic compute it bills at a higher DBU rate, so measure cost per job, not only runtime.

### Sharing and collaboration

- **Delta Sharing** (renamed **OpenSharing** in Jun 2026): an open REST protocol. The server authorizes the recipient and returns short-lived pre-signed URLs to Parquet files. The recipient reads straight from storage, so the provider runs no compute for the read. There are two modes:
  - **Databricks-to-Databricks:** UC to UC. You can share tables, views, volumes, models and notebooks.
  - **Open sharing:** the recipient authenticates with a bearer token or OIDC. Any client works: pandas, Spark, Power BI, or Snowflake. Snowflake's native consumption is **(GA)** as of Jul 2026 ([§08](08-snowflake-vs-databricks.md)).
  - **Token changes to know:** new tokens last at most one year. Long-lived tokens issued before Dec 8, 2025 expire on Dec 8, 2026 unless renewed. A move to recipient-specific URLs, planned for Jul 1, 2026, was postponed with no new date.
- **Clean Rooms:** each party shares data into an isolated serverless environment. Only approved notebooks run there, and only approved outputs leave. No party sees another's raw rows. Built on Delta Sharing. **(GA)** since Jan 2025.

### System tables

Platform telemetry is exposed as Delta tables in the `system` catalog. Key tables:
- `system.billing.usage` and `system.billing.list_prices`
- `system.access.audit`
- `system.access.table_lineage` and `system.access.column_lineage`
- `system.query.history`
- `system.compute.*` and `system.lakeflow.*`

They exist so that FinOps and audit can be plain SQL instead of API scraping. Usage joined to custom tags gives chargeback by team or project, and audit plus lineage answer "who touched what?" ([§13](13-performance-cost.md), [§12](12-governance-security.md)).

### Declarative Automation Bundles

This is the name since Mar 2026; bundles were formerly called Databricks Asset Bundles. The rename doesn't break anything: the `databricks bundle` CLI and existing configs are unchanged.

A bundle is a YAML project that declares jobs, pipelines, dashboards and apps next to their code, with dev, staging and prod targets. CI/CD deploys it. It exists because objects created by clicking in the UI drift between environments, can't be reviewed, and can't be promoted. A bundle makes deployment repeatable (P6).

### Platform expansion

- **Lakebase (GA, Jan 2026).** Managed Postgres built on Neon technology.
  - **Why it exists:** apps need OLTP next to the lakehouse without a reverse-ETL stack (P1).
  - **How it works:** separating storage from compute enables autoscaling, scale-to-zero, instant copy-on-write branches and point-in-time recovery.
  - **Versions:** Postgres 17 is the default; 16 to 18 are supported.
  - **APIs:** programmatic APIs reached GA in Aug 2026.
  - **Sync with Delta tables:** **synced tables** **(GA)** copy UC tables into Postgres for serving. **Lakehouse sync** **(Preview)** replicates Postgres tables back to Delta through CDC.
- **UC metric views.** A semantic layer: measures and dimensions are defined in YAML and registered in UC. Dashboards, Genie and external BI tools all reuse the same definitions. Entered **(Preview)** May 2025; now **(GA)**.
- **Lakehouse Federation.** Two modes ([§08](08-snowflake-vs-databricks.md)):
  - **Query federation:** read-only JDBC pushdown to an external database.
  - **Catalog federation:** UC reads a foreign catalog's files directly. Sources include Hive metastore, Glue and Snowflake Horizon Iceberg tables.
- **Databricks Apps.** Host data and AI web apps (Streamlit, Dash, Flask, Node and others) on serverless compute, with UC-governed data access. **(GA)** since May 2025.
- **AI/BI dashboards (formerly Lakeview) and Genie.** Genie turns natural-language questions into SQL within a curated space of tables, instructions and trusted queries. Its answer quality depends on metadata quality ([§18](18-ai-on-data-platform.md)). Genie spaces were renamed **Genie Agents** in Jul 2026.
- **Unity Catalog open source and Iceberg.** UC was open-sourced in 2024 and exposes an Iceberg REST catalog API.
  - **(GA):** managed Iceberg tables, foreign Iceberg tables, Iceberg v3 features, and sharing of managed Iceberg tables.
  - **(Preview):** managed Iceberg materialized views.
  - More in [§04](04-table-formats-catalogs.md).

## Comparison

| Old | Current |
|---|---|
| Delta Live Tables (DLT); briefly Lakeflow Declarative Pipelines / Lakeflow Spark Declarative Pipelines | Lakeflow pipelines; OSS core: Apache Spark Declarative Pipelines |
| `import dlt`, `@dlt.table`, `@dlt.view` | `from pyspark import pipelines as dp`, `@dp.table` / `@dp.materialized_view`, `@dp.temporary_view` |
| `APPLY CHANGES INTO` | AUTO CDC (`create_auto_cdc_flow`) |
| Databricks Jobs (Workflows) | Lakeflow Jobs |
| Databricks Asset Bundles | Declarative Automation Bundles (CLI unchanged) |
| Delta Sharing | OpenSharing (Jun 2026) |
| Lakeview dashboards | AI/BI dashboards |
| Genie spaces | Genie Agents (Jul 2026) |
| Hive partitioning + `ZORDER BY` | Liquid clustering, `CLUSTER BY AUTO` |
| Scheduled `OPTIMIZE`/`VACUUM` jobs | Predictive optimization |
| Hand-built or partner ingestion | Lakeflow Connect |
| UniForm as the only Iceberg path | Managed Iceberg tables in UC; UniForm now documented as "Iceberg reads" |

## Interview questions

**Q: Streaming table or materialized view: how do you choose?**

A streaming table processes each source record once using a checkpoint. That makes it cheap and right for append-only ingestion, but it won't reprocess an upstream correction. A materialized view keeps a query result correct as its inputs change, which suits joins, aggregations and sources with updates or deletes. The cost is a more expensive refresh when it can't be incremental. I use streaming tables at bronze and MVs above that. When a streaming source starts receiving updates, I switch that step to AUTO CDC or an MV.

**Q: Why liquid clustering over partitioning plus Z-order? When would you still partition?**

Partitioning fixes the data layout in directory names at write time. A wrong or high-cardinality key causes small files and skew, and changing it means rewriting the table. Z-order rewrote whole partitions on every `OPTIMIZE`. Liquid clustering keeps keys in metadata, clusters incrementally, and lets you change keys without a rewrite. I'd still partition only a very large table with a stable, low-cardinality key that every query filters on, such as a date used to drop old data.

**Q: When doesn't Photon help?**

Photon speeds up columnar operators: scans, joins, aggregations and writes. It does nothing for Python or pandas UDFs, RDD code, I/O-bound jobs or very short queries, either because it falls back to Spark or because the bottleneck is elsewhere. On classic compute it bills a higher DBU rate. So the real test is cost per job with Photon on and off.

**Q: Serverless or classic compute?**

Serverless removes cluster management, starts in seconds and upgrades itself, which makes it the right default for SQL, notebooks and most jobs (P10). Classic still wins when you need specific instance types or GPUs, Spark configs that serverless blocks, compute that must stay inside your network, or spot pricing for large steady jobs. The risk with serverless is cost visibility. Watch the billing system tables and set budget policies.

**Q: How do system tables support FinOps and governance?**

They expose usage, prices, audit logs, lineage and query history as queryable Delta tables. Joining usage to list prices and custom tags gives cost by team, job or project, which supports chargeback and catches idle compute. Audit and lineage tables show who accessed what and what sits downstream of a change. The prerequisite is tagging discipline, because untagged spend can't be attributed.

**Q: How does Delta Sharing work, and how would you share with a Snowflake account?**

The sharing server authorizes the recipient and returns short-lived pre-signed URLs. The recipient reads the files directly with its own compute, and the provider remains the single source of truth. Between workspaces I'd use Databricks-to-Databricks sharing. For Snowflake I'd use open sharing, which Snowflake now consumes natively as a catalog-linked database. The shared tables are read-only in Snowflake, and I'd plan for token expiry or use OIDC instead of bearer tokens.

**Q: Why use Declarative Automation Bundles instead of building in the UI?**

Jobs, pipelines and dashboards created by clicking drift between environments, can't be code-reviewed and are hard to recreate. A bundle declares them as YAML next to the code, so CI/CD deploys the same definition to dev, staging and prod with per-target parameters. That makes deployments repeatable (P6), and changes go through the same review as code.

**Q: Why put Postgres (Lakebase) inside a lakehouse, and when wouldn't you?**

Apps need fast single-row reads and writes, which columnar tables handle poorly (P1). A separate OLTP database plus reverse ETL adds a pipeline, a copy and another governance model. Lakebase separates storage from compute to get autoscaling, scale-to-zero and instant branches, all under platform governance. I wouldn't move a high-throughput core system of record there until its HA and operational track record meet our SLAs.

## Takeaways
- Current names: Lakeflow Connect, Lakeflow pipelines (`dp`), Lakeflow Jobs, Declarative Automation Bundles, OpenSharing.
- Streaming tables process each record once, for ingestion. Materialized views keep a result correct, for transformations.
- Liquid clustering keeps keys in metadata and clusters incrementally, which beats both directory partitions and full Z-order rewrites.
- Predictive optimization and automatic liquid clustering move maintenance onto the platform, but only for UC managed tables.
- Serverless trades control for operability. Photon helps columnar operators, not UDFs.
- OpenSharing (formerly Delta Sharing) grants access to files instead of copying them, and Snowflake now consumes shares natively.
- System tables turn FinOps and audit into SQL, provided resources are tagged.
- Lakebase and metric views extend the platform toward OLTP and a semantic layer.

## Sources
- https://docs.databricks.com/aws/en/dev-tools/bundles/faqs
- https://docs.databricks.com/aws/en/ldp/where-is-dlt
- https://docs.databricks.com/aws/en/ldp/developer/python-ref
- https://docs.databricks.com/aws/en/ldp/concepts
- https://docs.databricks.com/aws/en/delta/clustering
- https://docs.databricks.com/aws/en/release-notes/product/
- https://docs.databricks.com/aws/en/release-notes/product/2025/june
- https://docs.databricks.com/aws/en/release-notes/product/2025/november
- https://docs.databricks.com/aws/en/release-notes/product/2025/december
- https://docs.databricks.com/aws/en/release-notes/product/2026/march
- https://docs.databricks.com/aws/en/release-notes/product/2026/june
- https://docs.databricks.com/aws/en/release-notes/product/2026/july
- https://docs.databricks.com/aws/en/opensharing/
- https://docs.databricks.com/aws/en/admin/system-tables/
- https://docs.databricks.com/aws/en/oltp/projects/sync-tables
- https://docs.databricks.com/aws/en/oltp/projects/lakebase-cdf
- https://docs.databricks.com/aws/en/query-federation/catalog-federation
- https://docs.databricks.com/aws/en/delta/iceberg-reads
- https://learn.microsoft.com/en-us/azure/databricks/release-notes/product/
- https://docs.databricks.com/aws/en/release-notes/lakebase/
- https://docs.databricks.com/aws/en/release-notes/product/2025/may
- https://docs.databricks.com/aws/en/metric-views
- https://docs.databricks.com/aws/en/query-federation/database-federation
- https://docs.databricks.com/aws/en/external-access/iceberg
- https://docs.databricks.com/aws/en/opensharing/create-recipient-token
- https://docs.databricks.com/aws/en/release-notes/whats-coming
- https://docs.snowflake.com/en/release-notes/2026/other/2026-07-21-delta-sharing-horizon-catalog-ga
- https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-catalog-integration-delta-sharing
