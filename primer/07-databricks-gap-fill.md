# 07. Databricks Gap-Fill: What's New or Renamed

> As of October 2026. Tags: **(GA)** generally available · **(Preview)** public/private preview · **(⚠ verify)** not confirmed against official docs.

## Why this exists

Between 2024 and 2026, Databricks renamed much of its product surface (Lakeflow, bundles, pipelines). It also moved its defaults from "you tune it" to "the platform tunes it": serverless compute, predictive optimization and liquid clustering. Interviewers use the current names. This section assumes you already know Spark, Delta, Unity Catalog (UC), jobs and clusters. It covers only what is new, renamed or rarely used, with the reason each feature exists and how it works.

## Core concepts

### Lakeflow: ingestion, transformation, orchestration

Lakeflow is the umbrella name for Databricks' data engineering stack, which has four parts.

- **Lakeflow Connect.** *Why it exists:* most teams don't want to build connectors for SaaS apps and databases themselves. *How it works:* managed connectors (for example Salesforce, Workday, SQL Server) read incrementally. Database sources use change data capture (CDC) from the transaction log (P5). Each connector lands data as a streaming table on serverless compute. Availability differs per connector (⚠ verify per source).
- **Lakeflow Spark Declarative Pipelines (SDP).** This is the current name for what used to be Delta Live Tables (DLT). *Why it exists:* hand-written streaming jobs get checkpoints, ordering, retries and infrastructure wrong. *How it works:* you declare datasets, and the engine builds the dependency graph and handles checkpoints, retries and compute.
  - Databricks donated the core to Apache Spark, where it is called Spark Declarative Pipelines.
  - The Python module is now `pipelines`, imported as `dp`. `@dlt.table` becomes `@dp.table`, and `@dlt.view` becomes `@dp.temporary_view`. The old `dlt` module still works.
  - Some features exist only on Databricks: expectations, and AUTO CDC (`dp.create_auto_cdc_flow`, formerly `APPLY CHANGES`).
- **Lakeflow Jobs** (formerly Workflows). The orchestrator. It supports task graphs, file-arrival and table-update triggers, for-each loops and conditional tasks. Its run history is in system tables.
- **Lakeflow Designer** **(GA)** Jun 2026. A visual, no-code canvas for building data preparation flows. *Why it exists:* analysts need to build pipelines without leaving governance behind. *How it works:* the flows it generates are real code, governed in UC.

**Streaming tables vs. materialized views (MVs)** are the two core SDP dataset types:
- A **streaming table** processes each new source record once, tracked by a checkpoint. That makes it cheap and suited to append-only ingestion. It does not reprocess old records if upstream data changes.
- A **materialized view** stores a query result that the engine keeps correct as upstream data changes. It refreshes incrementally where it can and fully where it must. That makes it correct for joins, aggregations, updates and deletes, at a higher cost per refresh.
- Rule of thumb: streaming tables for bronze ingestion, MVs for transformations where correctness matters (P6, P7).

**Expectations** are declarative data-quality rules attached to a dataset. When a row fails one, you choose what happens:
- `warn`: keep the row and record a metric;
- `drop`: remove the row;
- `fail`: stop the update.

Results go to the pipeline event log, where you can alert on them (P8).

### Layout and maintenance

- **Liquid clustering.** *Why it exists:* Hive-style partitioning fixes the data layout at write time as directories. A wrong key gives you small files or skew, and changing the key means rewriting the whole table. Z-order helped, but each `OPTIMIZE` rewrote entire partitions. *How it works:*
  - clustering keys are stored in table metadata, not in directory names;
  - `OPTIMIZE` clusters only new or unclustered files (incremental);
  - `ALTER TABLE ... CLUSTER BY` changes the keys without rewriting existing data.

  Use it for high-cardinality filter columns, skewed data and access patterns that change over time. A table cannot be both liquid-clustered and partitioned or Z-ordered. The approach is skip work instead of doing it faster (P4).
- **Automatic liquid clustering (`CLUSTER BY AUTO`).** Databricks chooses the keys from the table's query history. It changes keys only when the predicted data-skipping savings exceed the cost of reclustering. Requirements: predictive optimization must be enabled, and Databricks Runtime (DBR) 15.4 LTS or later. Docs labeled it **(Preview)**; current status (⚠ verify GA).
- **Predictive optimization.** It runs `OPTIMIZE`, `VACUUM` and `ANALYZE` automatically on UC managed tables, deciding when based on telemetry. It runs on serverless compute that you pay for. It applies to managed tables only, which is one reason Databricks pushes managed tables. Default enablement for accounts (⚠ verify).
- **Deletion vectors.** These make deletes merge-on-read. `DELETE`, `UPDATE` and `MERGE` write a small bitmap that marks rows as removed instead of rewriting whole Parquet files. Readers apply the bitmap, and later compaction purges the rows physically. The tradeoff:
  - faster writes, slightly slower reads until compaction runs;
  - some external readers and interop paths can't handle them. For example, Delta Sharing to Snowflake was reported not to support them as of May 2026 (⚠ verify).

### Compute

- **Serverless compute** covers notebooks, jobs, pipelines and SQL. It runs on a fleet that Databricks manages in its own account. Startup takes seconds, the runtime is versionless and auto-upgraded, and VM cost is included in the DBU rate.
  - What you give up: choice of instance types, most Spark configs, and init-script-style customization.
  - What you need to set up: network controls (private connectivity and egress policies) on the Databricks side.
  - Track cost through system tables (P10).
- **SQL warehouse types:**
  - **Classic:** the base engine with Photon.
  - **Pro:** adds newer engine features, and is required (or Serverless is) for things like Lakehouse Federation.
  - **Serverless:** Pro features plus fast startup and managed scaling.

  Use Serverless for BI concurrency, and Classic or Pro only where network or policy rules require compute in your own account.
- **Photon** is a vectorized C++ execution engine. It replaces JVM execution for the operators it supports and falls back to Spark for the rest, with no code changes.
  - It **helps** with scans, joins, aggregations, `MERGE` and writes on Delta or Parquet.
  - It **doesn't help** with Python or pandas UDF-heavy code, RDD code, tiny queries, or I/O-bound jobs.
  - On classic compute it uses a higher DBU rate, so measure cost per job, not runtime alone.

### Sharing and collaboration

- **Delta Sharing** is an open REST protocol. The sharing server checks the recipient's access and returns short-lived pre-signed URLs to the Parquet files. The recipient reads directly from storage, so the provider runs no compute for reads. There are two modes:
  - **Databricks-to-Databricks:** UC to UC. It can share tables, views, volumes, models and notebooks, and identity is the recipient's metastore.
  - **Open sharing:** the recipient authenticates with a bearer token or OIDC federation and can use any client: pandas, Spark, Power BI, or Snowflake. Snowflake's native consumption reached **(GA)** Jul 2026; see [§08](08-snowflake-vs-databricks.md).

  Token-lifecycle changes:
  - tokens created on or after Jul 1, 2026 use recipient-specific URLs;
  - long-lived tokens issued before Dec 8, 2025 expire Dec 8, 2026 unless renewed.
- **Clean Rooms** let parties collaborate on data without seeing each other's raw records. Each party shares data into an isolated serverless environment, where only approved notebooks run and only approved outputs leave. The feature is built on Delta Sharing. Status (⚠ verify).

### Observability: system tables

Platform telemetry is exposed as Delta tables under the `system` catalog:
- `system.billing.usage` and `list_prices`;
- `system.access.audit`;
- `system.access.table_lineage` and `column_lineage`;
- `system.query.history`;
- the `system.compute.*` and `system.lakeflow.*` schemas.

These exist because FinOps and audit work should be plain SQL rather than API scraping. Joining usage to custom tags gives you chargeback (cost by team or project), and audit plus lineage answer "who touched what" ([§13](13-performance-cost.md), [§12](12-governance-security.md)). Exact table names per schema (⚠ verify).

### Deployment: Declarative Automation Bundles

**Declarative Automation Bundles** is the name since Mar 2026; they were previously called Databricks Asset Bundles. The rename doesn't break anything: the `databricks bundle` CLI and existing configs are unchanged, and "DAB" still applies.

A bundle is a YAML project that declares jobs, pipelines, dashboards, apps and similar resources next to their code, with targets such as dev, staging and prod. CI/CD deploys it with the CLI. *Why it exists:* workspace objects created by clicking in the UI drift, can't be reviewed and can't be promoted between environments. A bundle makes them versioned, reviewable and reproducible (P6).

### Platform expansion

- **Lakebase** **(GA)** Jan 2026. Managed Postgres, built on Neon technology. *Why it exists:* applications need fast single-row OLTP next to the lakehouse (P1), without a separate reverse-ETL stack. *How it works:* storage and compute are separated, which gives:
  - autoscaling and scale-to-zero;
  - instant copy-on-write branches (useful for dev and test databases);
  - point-in-time recovery.

  Postgres 17 is the default and versions 16–18 are supported. Programmatic APIs reached **(GA)** Aug 2026. Use it for app state, serving lookups and agent memory. Sync with Delta tables (⚠ verify feature name and status).
- **UC metric views.** A semantic layer: measures and dimensions defined in YAML and registered in UC. Dashboards, Genie, alerts and external BI tools all reuse the same definitions, so a metric is defined once. Entered **(Preview)** May 2025; GA status (⚠ verify).
- **Lakehouse Federation** has two modes ([§08](08-snowflake-vs-databricks.md)):
  - **Query federation:** read-only queries pushed down to the external database over JDBC.
  - **Catalog federation:** UC reads a foreign catalog's files directly, from Hive metastore, Glue or Snowflake Horizon Iceberg tables.
- **Databricks Apps.** Hosts data and AI web apps (Streamlit, Dash, Flask, Node and others) on serverless compute. Apps access data under UC governance, as a service principal or on behalf of the user. Status (⚠ verify).
- **AI/BI dashboards** (formerly Lakeview) **and Genie.** Genie answers natural-language questions by generating SQL inside a curated space of tables, instructions and trusted queries. Answer quality depends on metadata and metric definitions ([§18](18-ai-on-data-platform.md)).
- **UC open source and Iceberg.** UC was open-sourced in 2024 and exposes an Iceberg REST catalog API. The following are **(GA)**: managed Iceberg tables, foreign Iceberg tables governed by UC, Iceberg v3 features, and sharing of managed Iceberg tables. Managed Iceberg materialized views are **(Preview)** ([§04](04-table-formats-catalogs.md)).

## Comparison

**Renamed or replaced**

| Old | Current |
|---|---|
| Delta Live Tables (DLT), then "Lakeflow Declarative Pipelines" | Lakeflow Spark Declarative Pipelines (SDP); OSS core: Spark Declarative Pipelines |
| `import dlt`, `@dlt.table`, `@dlt.view` | `from pyspark import pipelines as dp`, `@dp.table`, `@dp.temporary_view` |
| `APPLY CHANGES INTO` | AUTO CDC (`create_auto_cdc_flow`) |
| Databricks Workflows | Lakeflow Jobs |
| Databricks Asset Bundles | Declarative Automation Bundles (CLI unchanged) |
| Lakeview dashboards | AI/BI dashboards |
| Hive partitioning + `ZORDER BY` | Liquid clustering, `CLUSTER BY AUTO` |
| Scheduled `OPTIMIZE`/`VACUUM` jobs | Predictive optimization |
| Hand-built or partner ingestion | Lakeflow Connect |
| UniForm as the only Iceberg path | Managed Iceberg tables in UC |

**Streaming table vs. materialized view**

| | Streaming table | Materialized view |
|---|---|---|
| Processes | Each new record once (checkpoint) | Whole query result, kept correct |
| Upstream updates/deletes | Not reflected unless handled explicitly | Reflected on refresh |
| Cost profile | Cheap, incremental | Incremental when possible, full recompute otherwise |
| Typical layer | Bronze ingestion | Silver/gold transforms, aggregates |

## Interview questions

**Q: Streaming table or materialized view: how do you choose?**

A streaming table processes each source record once, using a checkpoint. That makes it cheap and right for append-only ingestion, but a late correction upstream is not reprocessed. A materialized view keeps a query result correct as inputs change, which suits joins, aggregations and data with updates or deletes. It costs more per refresh when it can't update incrementally. I use streaming tables at bronze and MVs above that. If a streaming source starts receiving updates, I switch that step to AUTO CDC or an MV.

**Q: Why liquid clustering over partitioning plus Z-order? When would you still partition?**

Partitioning fixes the layout in directory names at write time. A wrong or high-cardinality key causes small files and skew, and changing it means rewriting the table. Z-order rewrote whole partitions on every `OPTIMIZE`. Liquid clustering keeps keys in metadata, clusters incrementally, and lets you change keys without a rewrite. I would still partition only very large tables with a low-cardinality key that every query filters on and that never changes, such as date when retention is managed by dropping old partitions.

**Q: When doesn't Photon help?**

Photon speeds up columnar operators: scans, joins, aggregations and writes. It does nothing for Python or pandas UDFs, RDD code, I/O- or network-bound jobs, or very short queries, because it falls back to Spark or the bottleneck is elsewhere. On classic compute it bills at a higher DBU rate. The test is cost per job with Photon on versus off, not runtime alone.

**Q: Serverless or classic compute?**

Serverless removes cluster management, starts in seconds and auto-upgrades the runtime, which is the right default for SQL, ad hoc notebooks and most jobs (P10). Classic still wins when you need specific instance types or GPUs, Spark configs that serverless doesn't allow, custom networking that must stay in your account, or spot pricing for large, steady jobs. The risk with serverless is cost visibility, so watch the billing system tables and set budget policies.

**Q: How do system tables support FinOps and governance?**

They expose usage, prices, audit logs, lineage and query history as queryable Delta tables. Joining `system.billing.usage` to list prices and custom tags gives cost by team, job or project, so you can do chargeback and catch idle clusters. Audit and lineage tables show who accessed which data and what is downstream of a change. The prerequisite is a tagging discipline: costs that aren't tagged can't be attributed.

**Q: How does Delta Sharing work, and how would you share with a Snowflake account?**

The sharing server authorizes the recipient and returns short-lived pre-signed URLs to the table's files. The recipient reads straight from storage with its own compute, and the provider just stays the source of truth. Between Databricks workspaces I'd use Databricks-to-Databricks sharing. For Snowflake I'd use open sharing, which Snowflake now consumes natively as a catalog-linked database. Check the table-feature constraints on the provider side (UniForm, deletion vectors) and plan for token expiry and rotation.

**Q: Why use Declarative Automation Bundles instead of building in the UI?**

Jobs, pipelines and dashboards created by clicking drift between environments, can't be code-reviewed, and can't be recreated after a mistake. A bundle declares them as YAML next to the code, so CI/CD deploys the same definition to dev, staging and prod, with parameters for each target. That makes deployments repeatable (P6) and puts changes through the same review as code.

**Q: Why put Postgres (Lakebase) inside a lakehouse, and when wouldn't you?**

Applications need fast single-row reads and writes, which columnar tables are bad at (P1). A separate OLTP database plus reverse ETL adds a pipeline, a copy and a second governance model. Lakebase separates storage and compute to get autoscaling, scale-to-zero and instant branches, and it sits under the same platform governance. I wouldn't move a high-throughput core system of record there until its HA, latency and operational track record meet our SLAs.

## Takeaways
- Current names: Lakeflow Connect, Lakeflow Spark Declarative Pipelines (`dp`), Lakeflow Jobs, Declarative Automation Bundles.
- Streaming tables: process each record once, for ingestion. MVs: keep a result correct, for transforms.
- Liquid clustering keeps keys in metadata and clusters incrementally, which beats both directory partitions and full Z-order rewrites.
- Predictive optimization and automatic liquid clustering move maintenance to the platform, but only for UC managed tables.
- Serverless trades control for operability; Photon helps columnar operators, not UDFs.
- Delta Sharing grants access to files instead of copying them, and Snowflake now consumes shares natively.
- System tables turn FinOps and audit into SQL, but only if resources are tagged.
- Lakebase and metric views extend the platform toward OLTP and a semantic layer.

## Sources
- https://docs.databricks.com/aws/en/release-notes/dev-tools/bundles
- https://docs.databricks.com/aws/en/dev-tools/bundles/faqs
- https://docs.databricks.com/aws/en/dlt/python-ref
- https://docs.databricks.com/gcp/en/dlt/concepts
- https://docs.databricks.com/aws/en/delta/clustering
- https://www.databricks.com/blog/announcing-automatic-liquid-clustering
- https://docs.databricks.com/aws/en/release-notes/product/
- https://learn.microsoft.com/en-us/azure/databricks/release-notes/product/
- https://docs.databricks.com/aws/en/release-notes/lakebase/
- https://docs.databricks.com/aws/release-notes/product/2025/may
- https://docs.databricks.com/aws/en/metric-views
- https://docs.databricks.com/aws/en/query-federation/database-federation
- https://docs.databricks.com/aws/en/query-federation/snowflake-catalog-federation
- https://docs.databricks.com/aws/en/external-access/iceberg
- https://docs.databricks.com/aws/en/delta-sharing/create-recipient-token
- https://docs.databricks.com/aws/en/release-notes/whats-coming
- https://docs.snowflake.com/en/release-notes/2026/other/2026-07-21-delta-sharing-horizon-catalog-ga
- https://www.databricks.com/blog/unity-catalog-and-next-era-apache-icebergtm
