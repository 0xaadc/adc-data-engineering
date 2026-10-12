# 08. Snowflake vs. Databricks

> As of October 2026. Tags: **(GA)** generally available · **(Preview)** public/private preview · **(⚠ verify)** not confirmed against official docs.

## Why this exists

Each platform started as the answer to a different problem, and its defaults still show where it came from.

**Snowflake (founded 2012 by database engineers from Oracle).** The problem was the on-prem MPP warehouse, which shipped storage and compute in the same box. More users meant buying a bigger box, a load job competed with the executive dashboard, and capacity had to be bought years ahead. Hadoop was cheap, but it wasn't a SQL database. Snowflake's bet was to rebuild the warehouse for the cloud:
- all data sits in object storage in a proprietary columnar format;
- many independent compute clusters run over that storage (P2);
- a central service layer owns metadata, optimization and security, so the customer manages almost nothing (P10, managed side).

Because Snowflake owns the format and the metadata end to end, its optimizer can prune aggressively (P4), and cloning and sharing become cheap metadata operations (P3).

**Databricks (founded 2013 by the creators of Apache Spark at UC Berkeley's AMPLab).** The problem was MapReduce, which was too slow for iterative ML and interactive data science on huge data-lake files. Databricks' bet was a general-purpose distributed engine that you program in code (Python, Scala, SQL) over open files the customer owns. Data lakes then proved unreliable: no transactions, no schema enforcement, and too many small files. That led to Delta Lake, a transaction log over Parquet (P3). Delta in turn led to the lakehouse, which gives warehouse guarantees on open formats (P11) with configurable compute (P10, control side).

**Then the two converged.** Snowflake added code, open formats and apps. Databricks added a SQL warehouse, BI and managed governance. The interview question is no longer "warehouse or lake?" It is now:
- whose defaults fit your team;
- who owns the storage and the catalog;
- how much control you will trade for operability.

## Core concepts

### Two architectures: where the metadata lives

```
SNOWFLAKE                               DATABRICKS
+------------------------------+        +------------------------------+
| Cloud services (vendor):     |        | Control plane (vendor):      |
| metadata, optimizer, txns,   |        | workspace, jobs, Unity       |
| security                     |        | Catalog (metadata + policy)  |
+------------------------------+        +------------------------------+
| Virtual warehouses: one per  |        | Classic compute in YOUR      |
| workload, billed per second  |        | cloud account, or serverless |
+------------------------------+        +------------------------------+
| Vendor-managed storage       |        | YOUR bucket: Parquet +       |
| (micro-partitions), or       |        | Delta/Iceberg log            |
| Iceberg in your bucket       |        |                              |
+------------------------------+        +------------------------------+
```

The core difference between the two is **where table metadata lives**.

- **Snowflake native tables:** metadata lives in the cloud services layer. Only Snowflake can read these tables. In return, the optimizer always has exact per-partition statistics, and cloning, Time Travel and sharing are just metadata pointers.
- **Databricks tables:** the metadata is the Delta or Iceberg log, stored next to the data in your bucket. Any compatible engine can read it. The cost is that someone has to run maintenance: compaction, statistics and vacuum. The platform increasingly does this for you ([§07](07-databricks-gap-fill.md)).

Architecture details are in [§05](05-snowflake-architecture.md), and table formats in [§04](04-table-formats-catalogs.md).

### Convergence: what each side added

| Gap being closed | Snowflake added | Databricks added |
|---|---|---|
| Code beyond SQL | Snowpark (Python/Java/Scala DataFrames pushed down to warehouses), notebooks, Snowpark Container Services, Snowpark Connect for Apache Spark (⚠ verify status) | native |
| SQL warehouse and BI | native | Databricks SQL, serverless SQL warehouses, Photon, AI/BI dashboards and Genie |
| Open formats | Iceberg tables; Horizon Catalog Iceberg REST endpoint for external engines: reads **(GA)** Feb 2026, writes **(GA)** May 2026; Snowflake Open Catalog (managed Apache Polaris) | Delta UniForm (Iceberg reads); managed and foreign Iceberg tables in Unity Catalog **(GA)**; Iceberg v3 **(GA)** |
| Managed ingestion | Openflow (based on Apache NiFi): BYOC and Snowflake-hosted deployments **(GA)** | Lakeflow Connect |
| OLTP next to analytics | Snowflake Postgres **(GA)** Feb 2026 | Lakebase Postgres **(GA)** Jan 2026 |
| AI and agents | Cortex AI, Snowflake Intelligence **(GA)** Nov 2025, semantic views | AI/BI Genie, MLflow, model serving, metric views |
| Less tuning | Gen2 warehouses **(GA)**, Adaptive Compute **(GA)** Jun 2026 | Serverless compute, predictive optimization, liquid clustering |
| Governance | Horizon Catalog | Unity Catalog (open-sourced 2024) |

The features converged, but each platform's center of gravity did not:
- **Snowflake's default path** is managed storage, SQL, and one warehouse per workload.
- **Databricks' default path** is open tables in your bucket, code-first engineering, and compute you can configure.

Each platform can do the other's job. On either one, the non-default path usually has fewer features and less production use behind it.

### Why choose Snowflake (and when not to)

- **Near-zero operations.** Native tables have no files, clusters, runtimes or compaction jobs for you to manage. The platform owns data layout and statistics (P4, P10), so the team spends its time on modeling instead of tuning.
- **Concurrency isolation by design.** A warehouse is an independent compute cluster over shared storage (P2). Give ELT, BI and data science separate warehouses, and none of them can slow the others. A multi-cluster warehouse (Enterprise Edition) adds clusters when queries start to queue and removes them when load drops. A 9 a.m. dashboard spike therefore scales out instead of waiting in line.
- **Mature sharing and governance.** Storage is central, so Secure Data Sharing grants another account read access to your micro-partitions without copying them, and the consumer pays for its own compute. Sharing across regions or clouds requires replication. Marketplace, data clean rooms and Horizon policies (masking, row access, tags) all build on this.
- **Team fit.** BI-heavy organizations, SQL-skilled analysts and analytics engineers, and small platform teams.

**When not to choose it:**
- **Heavy custom Python, ML and distributed GPU training.** Snowpark and containers close part of the gap, but the Spark ecosystem is deeper and more proven.
- **Complex stateful streaming.** Snowpipe Streaming and Dynamic Tables cover ingestion and minute-level freshness. They don't cover arbitrary event-time stateful processing ([§11](11-stream-processing.md)).
- **Open-format-first mandates.** Iceberg tables close most of this gap, but some features remain native-only.
- **Very large, always-on compute.** Credits carry a premium over raw VM prices, and at 24/7 scale that premium compounds.

### Why choose Databricks (and when not to)

- **One copy in open formats.** Tables are Delta or Iceberg files in your bucket. Unity Catalog governs tables, volumes (files), functions and ML models together, and other engines can read the same data (P11).
- **Depth in code workloads.** One engine handles batch and Structured Streaming. Python is first-class, alongside notebooks, MLflow, GPU clusters and model serving. Data science and engineering work on the same data under the same governance.
- **Control when you want it.** Classic compute lets you choose instance types, spot capacity, autoscaling, runtime versions and Spark configs. Serverless is there when you don't want those choices.

**When not to choose it:**
- **Small SQL-only BI teams that want zero operations.** Serverless SQL warehouses close part of the gap, but the platform's surface area is still larger.
- **Organizations without engineering maturity.** More configuration options mean more ways to misconfigure: cluster sprawl, small files, unpinned dependencies, and all-purpose clusters left running.

### Running both: why, and how to split

Companies end up on both platforms through mergers, through teams choosing tools independently, or through a deliberate split by strength. Common splits:

1. **Databricks upstream, Snowflake downstream.** Ingestion, heavy engineering and ML run in Databricks. Curated gold tables are served to BI, SQL users and external sharing from Snowflake.
2. **Snowflake as the warehouse of record, Databricks for ML.** Data science reads Snowflake data, ideally as Iceberg, for feature engineering and training.
3. **Split by domain or business unit.** Each unit keeps its own platform, and the units meet at published data products.

Running both has real costs:
- two governance models, which drift apart over time (P9);
- duplicated data and pipelines;
- egress charges and latency between the platforms;
- two skill sets and two FinOps processes;
- "which number is right?" disputes when the same metric is computed on both sides.

Three rules keep this manageable:
- **One writer per table.**
- **One catalog of record per table.**
- **Explicit contracts at the boundary** between platforms (P8).

### Cost models

| | Snowflake | Databricks |
|---|---|---|
| Unit | Credit; the dollar price depends on edition, cloud, region and contract | DBU; the dollar price depends on compute type, tier, cloud and contract |
| Compute | Warehouse credits per hour by size. The rate doubles with each size step (know the doubling, not the price list). Billing is per second, with a 60-second minimum each time a warehouse starts or resumes | DBUs per hour by compute type (jobs, all-purpose, SQL, pipelines, serverless). **Classic:** you also pay your cloud provider's VM and disk bill. **Serverless:** infrastructure is included in the DBU rate |
| Background services | Serverless features (Snowpipe, automatic clustering, materialized-view maintenance, search optimization, serverless tasks, AI functions) are billed in credits | Serverless features (predictive optimization, serverless pipelines, model serving) are billed in DBUs |
| Metadata layer | Cloud services are billed only for the part that exceeds 10% of that day's warehouse credits | No separate charge |
| Storage | Native tables: Snowflake bills per TB-month of compressed data, and Time Travel and Fail-safe bytes count toward it. Iceberg tables: your cloud provider bills you | Your cloud provider bills you directly |
| Common leak | Large warehouses that never auto-suspend | All-purpose clusters with no auto-termination |

**Why comparisons depend on the workload.** Price per unit is not price per job. Cost is roughly rate × runtime + idle time, and runtime depends on:
- how efficiently each engine runs your query shapes;
- caching and data layout;
- how well concurrent queries are packed onto shared compute.

Vendor benchmarks pick workloads that favor their own engine.

**TCO includes people.** A platform that needs two fewer engineers can be more expensive on the invoice and still cheaper overall. The reverse also holds: a strong engineering team running tuned jobs on spot capacity can beat the managed premium. Cost governance is covered in [§13](13-performance-cost.md).

### Governance across two platforms

Unity Catalog and Horizon cover similar ground: RBAC, tags, column masking, row filters, lineage, audit and discovery. The hard part is that **policies don't travel with files**.
- Object storage can only grant or deny access to a whole file, so row- and column-level rules need a trusted engine to enforce them.
- A Snowflake masking policy isn't applied when Spark reads the same Iceberg files through another catalog. Likewise, a Unity Catalog row filter isn't applied inside Snowflake.

Catalogs bridge this gap with **credential vending**: the catalog authenticates the caller, then hands out short-lived storage credentials scoped to the table. That gives table-level control over any engine. Fine-grained rules still depend on the reading engine honoring them.

A workable pattern (P9):
- **One catalog of record per table.** It is where the table is written and where its policies live. The other platform reads through federation or a catalog link.
- **One identity source.** Use the same IdP and SCIM groups on both platforms, so a group name means the same people everywhere.
- **Tags as the shared vocabulary.** Producers apply tags such as PII, confidentiality and domain at write time. Policies are written once as code and deployed to both platforms.
- **Restricted data is served from one place.** A table that needs row-level security is exposed only through the engine that enforces the policy. The other side gets a masked or aggregated product.
- **Audit both platforms in one place.** Load Databricks system tables and Snowflake `ACCOUNT_USAGE` into a single store so that policy drift shows up.

Security mechanics are in [§12](12-governance-security.md).

### Interop: one copy, two engines

```
        single writer (owns commits AND maintenance)
                          |
       catalog of record: Unity Catalog  or  Horizon
                          |   Iceberg REST + vended credentials
              +-----------+------------+
              |                        |
     Databricks engine          Snowflake engine
     (read / write)             (catalog-linked database)
              \                        /
        Parquet + Iceberg/Delta metadata in ONE bucket
```

| Direction | Mechanism | Status |
|---|---|---|
| Databricks → Snowflake | Unity Catalog (UC) exposes managed Iceberg tables, or Delta tables with UniForm (Iceberg metadata), through its Iceberg REST endpoint. Snowflake connects through a catalog integration, then a **catalog-linked database** that discovers tables automatically, or individual externally managed Iceberg tables | Catalog-linked databases on external Iceberg REST catalogs **(GA)** Oct 2025. Snowflake writes to UC-managed Iceberg on Azure **(GA)** Apr 2026; other clouds (⚠ verify) |
| Databricks → Snowflake, as a share | **Delta Sharing:** Snowflake consumes a Delta share as a catalog-linked database in Horizon and converts its tables to Iceberg. Horizon policies apply | **(GA)** Jul 2026. Provider-side constraints such as UniForm required and deletion vectors unsupported (⚠ verify) |
| Delta files → Snowflake | **Delta Direct:** a catalog integration with `CATALOG_SOURCE = OBJECT_STORE` and `TABLE_FORMAT = DELTA` reads the Delta log straight from storage. It does not work from a Delta table definition held in UC or Glue | (⚠ verify) |
| Snowflake → Databricks | External engines read Snowflake-managed Iceberg tables through Horizon's Iceberg REST endpoint. Databricks **catalog federation** for Snowflake reads those Iceberg files directly from storage, on Databricks compute | Horizon external reads **(GA)** Feb 2026, writes **(GA)** May 2026. Databricks Snowflake catalog federation (⚠ verify) |
| Snowflake native tables → Databricks | Lakehouse Federation **query federation**: read-only, pushes the query down to Snowflake over JDBC. Snowflake compute does the work, so you pay both platforms | Suits low-volume or exploratory access, not bulk pipelines |
| Neutral catalog | Both engines use Snowflake Open Catalog (managed Apache Polaris) or another Iceberg REST catalog | Exists; check feature coverage per engine |

**Failure modes:**
- **Unclear catalog of record.** If two catalogs accept commits for the same table, each keeps its own "current snapshot" pointer (P3). One engine then reads stale data or silently overwrites the other's commit.
- **Concurrent writers.** Optimistic concurrency only works when all writers commit through the same catalog or log. Pick a single writer per table and publish to the other side.
- **Feature lag.** Engines support table features unevenly: deletion vectors, Iceberg v3 types, row lineage, variant columns. A feature enabled for one engine can make the table unreadable or slow in the other. Limit tables to the features both engines support, and test format upgrades on both.
- **Maintenance ownership.** Only the owning side should compact, expire snapshots or remove orphan files. If the other side expires a snapshot, readers that still use it break.
- **Metadata lag.** UniForm generates Iceberg metadata asynchronously after each Delta commit, so Iceberg readers can trail briefly.
- **Egress.** Cross-region or cross-cloud reads cost money and add latency. Keep the bucket in the same region as both platforms' compute.

### Where Microsoft Fabric fits

Fabric is Microsoft's SaaS analytics platform. It puts Spark, a SQL warehouse, real-time analytics, pipelines and Power BI on one storage layer and one bill.

- **OneLake:** one logical lake per tenant. Its native table format is Delta Parquet, and it exposes ADLS-compatible APIs.
- **Shortcuts:** pointers to data in ADLS, S3, GCS or elsewhere in OneLake. Nothing is copied.
- **Mirroring:** there are several variants.

  | Variant | What it does | Status |
  |---|---|---|
  | Snowflake mirroring | Replicates a Snowflake database into OneLake as Delta and keeps it in sync in near real time. This is a copy. Mirroring storage is free up to a quota tied to capacity size (for example, F64 includes 64 TB) | **(GA)** |
  | Mirrored Azure Databricks catalog | Mirrors Unity Catalog metadata into Fabric and reads the same tables through shortcuts. No data moves | **(GA)** |
  | Publishing a UC catalog to OneLake | Done from the Azure Databricks side | **(Preview)** |
  | OneLake–Snowflake interoperability | Snowflake-managed Iceberg tables can be stored in OneLake, and Fabric tables are exposed as Iceberg for Snowflake to read | **(GA)** |

- **Capacity pricing:** you buy an F SKU, which is a fixed number of capacity units (CUs) shared by every Fabric workload. Usage is smoothed over time, and sustained overuse is throttled. Spend is predictable, but one workload can starve others on the same capacity unless you split capacities.
- **Power BI Direct Lake:** the semantic model loads Delta columns from OneLake into memory on demand. That gives near-import speed without scheduled import refreshes. Some modes fall back to DirectQuery when a query can't use Direct Lake.

**Where it fits:** Microsoft-centric, Power BI-heavy organizations. A common role is as the BI and consumption layer over Snowflake or Databricks data, connected through mirroring or shortcuts.

**Where it doesn't:**
- multi-cloud estates;
- workloads that need strict per-workload compute isolation;
- teams already deep in another platform, for whom Fabric adds a third governance model.

Adopt it as a consumption layer first, before treating it as a replacement platform.

## Comparison

**Mechanism comparison**

| Dimension | Snowflake | Databricks |
|---|---|---|
| Original bet | Managed cloud SQL warehouse | Code-first engine on open files, which became the lakehouse |
| Default storage | Vendor-managed micro-partitions; Iceberg optional | Delta (or Iceberg) in your bucket |
| Metadata | Cloud services layer | Table log in storage, plus Unity Catalog |
| Isolation | Warehouse per workload; multi-cluster scale-out | Cluster or SQL warehouse per workload; serverless |
| Languages | SQL first; Python/Java/Scala through Snowpark | Python, SQL, Scala; SQL through Databricks SQL |
| Incremental and streaming | Snowpipe Streaming, Dynamic Tables, streams and tasks | Structured Streaming, Spark Declarative Pipelines |
| Tuning | Mostly automatic; optional clustering keys and search optimization | Liquid clustering, predictive optimization; more knobs |
| Governance | Horizon Catalog | Unity Catalog |
| Sharing | Secure Data Sharing (no copy, Snowflake-to-Snowflake), Marketplace | Delta Sharing (open protocol, any client), Marketplace |
| Billing | Credits | DBUs, plus cloud VMs on classic compute |

**Decision matrix**

| Workload / team / constraint | Lean | Mechanism behind it |
|---|---|---|
| SQL-skilled analysts, BI-heavy, small platform team | Snowflake | Least to operate; the platform owns layout and tuning (P10) |
| Spiky dashboard concurrency, many independent consumers | Snowflake (Databricks serverless SQL is close) | Multi-cluster warehouses scale out per workload (P2) |
| Heavy Python, ML training, GPUs, feature pipelines | Databricks | Spark plus ML runtimes, with UC governing models and data together |
| Complex stateful or event-time streaming | Databricks (or a dedicated stream processor, [§11](11-stream-processing.md)) | Structured Streaming state management |
| Minute-level incremental SQL ELT | Either | Dynamic Tables vs. declarative pipelines (P7) |
| Open-format mandate, many engines | Databricks, or either with Iceberg | Open by default vs. opt-in Iceberg tables (P11) |
| External data sharing and monetization | Snowflake; Delta Sharing for non-Snowflake recipients | No-copy sharing and a mature marketplace |
| Need control over instance types, spot, runtime | Databricks | Classic compute runs in your account |
| Limited engineering, zero-ops mandate | Snowflake | Fewer knobs means fewer ways to fail |
| OLTP app state next to analytics | Either | Snowflake Postgres / Lakebase |
| Multi-party privacy-safe collaboration | Either | Snowflake Data Clean Rooms / Databricks Clean Rooms |
| Azure- and Power BI-centric | Either, plus Fabric for BI | Direct Lake, mirroring, shortcuts |
| Very large always-on compute | Benchmark | Raw VM pricing vs. managed premium and people cost |

## Interview questions

**Q: Why Snowflake when Databricks exists?**

With Snowflake you are buying an operating model as much as an engine. Snowflake owns the storage format, metadata and tuning, so a small team gets fast, concurrent SQL with almost nothing to operate. Per-workload warehouses keep BI and ELT from slowing each other by design (P2, P10). Sharing is mature because storage is central: a share is a metadata grant, not a copy. I would not make it the primary platform for heavy custom ML or complex stateful streaming, and I would model the cost of large, always-on compute before committing.

**Q: Why Databricks when Snowflake exists?**

One copy of data in open formats can serve SQL, streaming, data science and ML under one catalog, and you keep control of compute (P11). That suits organizations whose hardest problems are engineering problems: messy ingestion, streaming, Python feature pipelines, model training and serving, and a mandate to avoid format lock-in. The price is surface area. Clusters, runtimes, file layout and cost all need engineering discipline, even though serverless removes much of it. For a small SQL-only BI team it is more platform than they need.

**Q: Your company runs both; how do you split workloads?**

Split by each platform's strength and by the direction data flows, not by team preference. A common split is Databricks for ingestion, engineering and ML, and Snowflake for curated BI serving and external sharing. The reverse makes sense when Snowflake is already the warehouse of record. The boundary is a set of published data products with contracts (P8): each table has one writer and one catalog of record, and the other side reads it through Iceberg instead of copying it. I would centralize identity, tagging and cost reporting across both. If the overlap keeps growing, consolidating onto one platform is a legitimate outcome, because running two costs two governance models, two skill sets and egress.

**Q: How would you govern data consistently across both?**

Policies don't travel with files: only a trusted engine enforces row and column rules. So each table gets one catalog of record that owns its policies (P9). On top of that:
- one IdP with SCIM-synced groups on both sides;
- a shared tag taxonomy applied at write time;
- policies defined as code and deployed to both Unity Catalog and Horizon.

Data that needs fine-grained protection is served only through the engine that enforces it, and the other platform gets a masked or aggregated product. Finally, collect both platforms' access logs in one place, because drift between the two policy sets is the usual failure.

**Q: How do you avoid two copies of the data?**

Store each shared table once in one bucket, as Iceberg or as Delta with UniForm Iceberg metadata. One catalog of record owns commits and maintenance. The other engine reads through Iceberg REST:
- Snowflake through catalog-linked databases pointed at Unity Catalog;
- Databricks through catalog federation against Horizon.

Delta Sharing into Snowflake is now also a no-copy option. The failure modes are writers committing through different catalogs, a table feature one engine can't read, and maintenance run by the wrong side. Sometimes a governed copy is the right answer: when the consumer needs native-only performance or features, or when the data would otherwise be read across regions.

**Q: Compare the cost models.**

Snowflake bills in credits: warehouse time per second (with a 60-second minimum per resume) at a rate that doubles per size step, plus serverless features, cloud services above 10% of daily warehouse use, and storage. Databricks bills DBUs by compute type. On classic compute you also pay your cloud provider for VMs; serverless folds the infrastructure into the DBU rate. Unit prices can't be compared directly, because cost is rate × runtime plus idle time, and runtime depends on engine efficiency for your queries and on concurrency packing. I would benchmark our top workloads on both platforms, count people in TCO, and watch for idle leaks.

**Q: Where does Fabric fit?**

Fabric is Microsoft's SaaS analytics platform on OneLake, with capacity-based pricing and native Power BI. In an Azure and Power BI shop it often fits best as the consumption layer:
- Azure Databricks tables mirrored as metadata through shortcuts;
- Snowflake data mirrored, or exposed as Iceberg;
- Power BI Direct Lake reading Delta without import refreshes.

As a full replacement, weigh the risk of workloads contending on shared capacity, uneven feature maturity, and the third governance model it adds. It is a poor fit for multi-cloud estates or for teams that need strict per-workload compute isolation.

**Q: What's converging, and does the choice still matter?**

Features are converging. Both platforms now have SQL warehouses, Python, notebooks, Iceberg, managed Postgres, ingestion connectors, semantic layers and AI assistants. Defaults and maturity are not converging: Snowflake's default path is managed storage plus SQL, Databricks' is open tables plus code, and each one's non-default path has less production use behind it. So the choice matters less for capability and more for operating model, team skills, cost on your workloads, and where the catalog of record lives (the new lock-in point, P11). Pick the platform whose default path is your main workload.

**Q: Dashboards slow down during nightly ELT. How does each platform fix that?**

Both fix it by giving ELT and BI separate compute over shared storage (P2). In Snowflake that means separate warehouses, with a multi-cluster warehouse for BI so peaks add clusters instead of queuing. In Databricks it means a dedicated serverless SQL warehouse for BI while ELT runs on job compute. If dashboards are still slow after isolating compute, the cause is usually the data: ELT rewriting the tables BI reads, which invalidates caches and data layout. Publish gold tables atomically, on a schedule.

**Q: Both platforms write to the same Iceberg table. What goes wrong?**

An Iceberg commit is an atomic swap of the current-metadata pointer in a catalog (P3). Two engines committing through the same catalog conflict and retry, like any two writers. Two engines committing through different catalogs each see a different "current" pointer, so one silently reads stale data or overwrites the other's snapshot. Maintenance from one side (compaction, snapshot expiry) can also delete files the other side still references. Make one platform the writer and owner of the table, and route the other platform's changes through it.

**Q: How would you run a fair cost and performance bake-off?**

Use your own top workloads by spend, not vendor benchmarks, and cover ELT, BI concurrency and at least one ML pipeline. Let each platform's experts tune their side (sizing, layout, caching), then measure cost per completed workload at the required SLA, including idle time. Add operating effort (people, on-call, upgrades) and migration cost to TCO. Re-run it periodically, because engines and price lists both change.

**Q: To serve Databricks data to Snowflake, would you use native Iceberg, UniForm or Delta Sharing?**

**Managed Iceberg in Unity Catalog** is cleanest when Snowflake is a first-class reader: one format, no translation. **UniForm** keeps Delta for Databricks-native features and adds Iceberg metadata asynchronously. Expect slight read lag, and limit the table to features Iceberg readers support. **Delta Sharing** fits consumers in another account or organization, because a share is a revocable, audited grant, and Snowflake now consumes shares natively. In all three cases there is one writer, and the consumer never writes back.

**Q: Where is the lock-in in each platform?**

Snowflake native tables are readable only by Snowflake. Iceberg tables remove the format lock-in, but proprietary features remain: some performance features, sharing and governance. Databricks data is open, so the lock-in moves to Unity Catalog policies and lineage, jobs, pipelines and runtime-specific features. In both cases the catalog, with its permissions, lineage and semantics, is the real lock-in point (P11). Estimate exit cost by what it takes to migrate policies and pipelines, not just bytes.

**Q: Both platforms now offer managed Postgres. Why, and when would you use it?**

Applications need fast single-row reads and writes, which a columnar engine is bad at (P1). Reverse ETL to a separate database adds pipelines, copies and governance gaps. Snowflake Postgres and Lakebase put OLTP next to the analytical data, with sync paths and shared governance. Use them for app state, serving lookups, features and agent memory. Don't move a high-volume core system of record onto them until their HA, scale and operations track record meets your SLAs. Lakebase's mechanism is covered in [§07](07-databricks-gap-fill.md).

## Takeaways
- Snowflake bet on managed SQL over vendor-owned storage; Databricks bet on code over open files. The defaults still follow from those bets.
- The core architectural difference is where table metadata lives: in a vendor service layer or in a log beside the data.
- Pick the platform whose default path is your main workload and whose operating model fits your team.
- When running both: one writer and one catalog of record per table, with contracts at the boundary.
- Price per unit is not price per job, and TCO includes people.
- Policies don't travel with files; fine-grained rules need a trusted engine, so serve restricted data from one place.
- Iceberg makes "one copy, two engines" real. Its failure modes are catalog ambiguity, feature lag and maintenance ownership.
- Fabric usually fits first as a Microsoft-centric consumption layer (OneLake, mirroring, Direct Lake), not as a replacement.
- The platforms have converged on features but not on defaults. The catalog is the new lock-in point.

## Sources
- https://docs.snowflake.com/en/user-guide/data-integration/openflow/about
- https://docs.snowflake.com/en/user-guide/data-integration/openflow/about-byoc
- https://docs.snowflake.com/en/release-notes/2026/other/2026-02-24-snowflake-postgres-ga
- https://docs.snowflake.com/en/user-guide/snowflake-postgres/about
- https://docs.snowflake.com/release-notes/2025/other/2025-10-17-iceberg-external-writes-cld-ga
- https://docs.snowflake.com/release-notes/2026/other/2026-04-06-iceberg-write-support-azure-unity-catalog
- https://docs.snowflake.com/release-notes/2026/other/2026-03-31-ctas-unity-catalog-external-volumes
- https://docs.snowflake.com/en/release-notes/2026/other/2026-02-06-tables-iceberg-query-using-external-query-engine-snowflake-horizon-ga
- https://docs.snowflake.com/release-notes/2026/other/2026-05-26-tables-iceberg-query-using-external-query-engine-snowflake-horizon-writes-ga
- https://docs.snowflake.com/release-notes/2026/other/2026-08-18-externally-managed-iceberg-tables-horizon-irc-preview
- https://docs.snowflake.com/en/user-guide/tables-iceberg-query-using-external-query-engine-snowflake-horizon
- https://docs.snowflake.com/en/release-notes/2026/other/2026-07-21-delta-sharing-horizon-catalog-ga
- https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-catalog-integration-object-storage
- https://docs.snowflake.com/en/user-guide/warehouses-considerations
- https://www.snowflake.com/legal-files/CreditConsumptionTable.pdf
- https://docs.snowflake.com/en/release-notes.html
- https://www.snowflake.com/en/blog/engineering/gen2-warehouses-snowflake-multicloud-ga/
- https://docs.snowflake.com/release-notes/2025/other/2025-11-04-snowflake-intelligence
- https://docs.databricks.com/aws/en/external-access/iceberg
- https://docs.databricks.com/aws/en/query-federation/snowflake-catalog-federation
- https://docs.databricks.com/aws/en/query-federation/database-federation
- https://docs.databricks.com/aws/en/release-notes/product/
- https://learn.microsoft.com/en-us/azure/databricks/release-notes/product/
- https://docs.databricks.com/aws/en/release-notes/lakebase/
- https://learn.microsoft.com/en-us/azure/databricks/partners/bi/fabric
- https://blog.fabric.microsoft.com/en/blog/announcing-the-general-availability-of-mirroring-for-snowflake-in-microsoft-fabric
- https://blog.fabric.microsoft.com/en-us/blog/microsoft-onelake-and-snowflake-interoperability-is-now-generally-available
- https://blog.fabric.microsoft.com/en/blog/databricks-unity-catalog-tables-available-in-microsoft-fabric
