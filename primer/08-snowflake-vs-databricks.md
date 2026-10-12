# 08. Snowflake vs. Databricks

> As of October 2026. Tags: **(GA)** generally available · **(Preview)** public/private preview · **(⚠ verify)** not confirmed against official docs.

## Why this exists

Each platform was a bet on a different problem, and its defaults still reflect that bet.

**Snowflake (2012, founded by database engineers from Oracle).** On-prem MPP warehouses bundled storage and compute in one box. Adding users meant buying a bigger box, and nightly loads competed with dashboards. Hadoop was cheap but wasn't a SQL database. The bet was a warehouse rebuilt for the cloud: data in object storage in a proprietary columnar format, many independent compute clusters over it (P2), and a vendor-run service layer that owns metadata, optimization and security, leaving the customer almost nothing to run (P10, managed side). Owning the format end to end lets the optimizer prune aggressively (P4) and makes cloning and sharing cheap metadata operations (P3).

**Databricks (2013, founded by the creators of Apache Spark at UC Berkeley's AMPLab).** MapReduce was too slow for iterative ML and interactive data science on lake files. The bet was a general-purpose distributed engine you program in code, running over open files the customer owns. Lakes then proved unreliable (no transactions, small-file chaos). That led to Delta Lake, a transaction log over Parquet (P3), and then to the lakehouse: warehouse guarantees on open formats (P11) with compute you configure yourself (P10, control side).

**Then the two converged.** The question is no longer "warehouse or lake?" It is whose defaults fit your team, who owns storage and the catalog, and how much control you will trade for operability.

## Core concepts

### Where the metadata lives

```
SNOWFLAKE                               DATABRICKS
+------------------------------+        +------------------------------+
| Cloud services (vendor):     |        | Control plane (vendor):      |
| metadata, optimizer, txns    |        | jobs, Unity Catalog          |
+------------------------------+        +------------------------------+
| Virtual warehouses, one per  |        | Classic compute in YOUR      |
| workload, billed per second  |        | account, or serverless       |
+------------------------------+        +------------------------------+
| Vendor-managed storage, or   |        | YOUR bucket: Parquet +       |
| Iceberg in your bucket       |        | Delta/Iceberg log            |
+------------------------------+        +------------------------------+
```

This is the core difference between the two platforms:
- **Snowflake native tables:** the metadata lives in the vendor's service layer. Only Snowflake can read the tables. In return, the optimizer always has exact statistics, and Time Travel, cloning and sharing are just pointer operations.
- **Databricks tables:** the metadata is a log stored next to the data, so any compatible engine can read it. The cost is that maintenance (compaction, statistics, vacuum) is a job that must run. Increasingly, the platform runs it for you ([§07](07-databricks-gap-fill.md)).

Details are in [§05](05-snowflake-architecture.md) and [§04](04-table-formats-catalogs.md).

### Convergence

| Gap being closed | Snowflake added | Databricks added |
|---|---|---|
| Code beyond SQL | Snowpark, notebooks, Snowpark Container Services, Snowpark Connect for Apache Spark **(GA)** | native |
| SQL warehouse and BI | native | Databricks SQL, serverless SQL warehouses, Photon, AI/BI dashboards and Genie |
| Open formats | Iceberg tables. Horizon Catalog Iceberg REST endpoint for external engines: reads **(GA)** Feb 2026, writes **(GA)** May 2026. Snowflake Open Catalog (managed Apache Polaris) | Delta UniForm (docs now call it "Iceberg reads"). Unity Catalog managed and foreign Iceberg tables and Iceberg v3 **(GA)** May 2026 |
| Managed ingestion | Openflow (Apache NiFi-based), BYOC and Snowflake-hosted **(GA)** | Lakeflow Connect |
| OLTP next to analytics | Snowflake Postgres **(GA)** Feb 2026 | Lakebase **(GA)** Jan 2026 |
| AI and semantics | Cortex AI, semantic views, Snowflake CoWork (formerly Snowflake Intelligence) **(GA)** Nov 2025 | AI/BI Genie, metric views, MLflow, model serving |
| Less tuning | Gen2 warehouses **(GA)**, Adaptive Compute **(GA)** Jun 2026 | Serverless compute, predictive optimization, liquid clustering |
| Governance | Horizon Catalog | Unity Catalog (open-sourced 2024) |

Features have converged. The center of gravity has not:
- **Snowflake's default path:** managed storage, SQL, and one warehouse per workload.
- **Databricks' default path:** open tables, code-first engineering, and compute you configure.

On either platform, the non-default path usually has fewer features and less production mileage.

### Why choose Snowflake, and when not to

- **Near-zero operations.** Native tables involve no files, clusters, runtimes or compaction jobs. The platform owns layout and statistics (P4, P10).
- **Concurrency isolation by design.** Each warehouse is an independent cluster over shared storage (P2), so ELT, BI and data science never contend. A multi-cluster warehouse (Enterprise Edition) adds clusters when queries start queuing, so a morning dashboard spike scales out instead of waiting.
- **Mature sharing and governance.** Storage is central, so Secure Data Sharing grants another account read access with no copy, and the consumer pays for its own compute. Cross-region sharing needs replication. Marketplace, clean rooms and Horizon policies all build on this.
- **Team fit.** BI-heavy organizations, SQL-skilled analysts, and small platform teams.

**When not to choose it:**
- Heavy custom Python, ML, or distributed GPU training. Snowpark and containers narrow the gap, but the Spark ecosystem is deeper.
- Complex stateful event-time streaming. Snowpipe Streaming and Dynamic Tables cover ingestion and minute-level freshness ([§11](11-stream-processing.md)).
- Open-format-first mandates. Iceberg narrows this, but some features remain native-only.
- Very large always-on compute, where the credit premium over raw VM cost compounds.

### Why choose Databricks, and when not to

- **One copy in open formats.** Delta or Iceberg in your bucket, with Unity Catalog governing tables, files, functions and models together. Other engines can read the same data (P11).
- **Depth in code workloads.** One engine for batch and Structured Streaming, first-class Python, notebooks, MLflow, GPUs and model serving, all on shared data under shared governance.
- **Control when you want it.** Instance types, spot capacity, runtimes and Spark configs on classic compute, or serverless when you don't want to choose.

**When not to choose it:**
- Small SQL-only BI teams that want zero operations. Serverless SQL narrows the gap, but the platform's surface area is larger.
- Organizations without engineering maturity. More knobs mean more ways to misconfigure: cluster sprawl, small files, idle all-purpose clusters.

### Running both

Companies end up on both platforms through mergers, through independent team choices, or through a deliberate split. Three splits are common:
1. **Databricks upstream, Snowflake downstream.** Ingestion, engineering and ML run in Databricks; curated gold tables serve BI and sharing from Snowflake.
2. **Snowflake as the warehouse of record, Databricks for ML.** Data science reads Snowflake data, ideally as Iceberg.
3. **By domain or business unit.** Each keeps its platform, and they meet at published data products.

Running both has real costs:
- two governance models that drift apart (P9);
- duplicated pipelines and data;
- egress between platforms;
- two skill sets and two FinOps processes;
- "which number is right?" disputes.

The rules that keep it manageable:
- one writer per table;
- one catalog of record per table;
- explicit contracts at the boundary (P8).

### Cost models

| | Snowflake | Databricks |
|---|---|---|
| Unit | Credit. Price varies by edition, cloud, region and contract | DBU. Price varies by compute type, tier, cloud and contract |
| Compute | Warehouse credits/hour by size, doubling per size step (know the doubling, not the price list). Per-second billing with a 60-second minimum on each start or resume | DBUs/hour by compute type (jobs, all-purpose, SQL, pipelines). **Classic:** plus your cloud provider's VM bill. **Serverless:** infrastructure included in the DBU rate |
| Background services | Serverless features (Snowpipe, automatic clustering, MV maintenance, search optimization, AI functions) billed in credits | Serverless features (predictive optimization, pipelines, model serving) billed in DBUs |
| Metadata layer | Cloud services billed only above 10% of that day's warehouse credits | No separate charge |
| Storage | Native tables: Snowflake bills per compressed TB-month, and Time Travel and Fail-safe bytes count. Iceberg: your cloud bills you | Your cloud bills you |
| Common leak | Warehouses that never auto-suspend | All-purpose clusters that never terminate |

**Price per unit isn't price per job.** Cost ≈ rate × runtime + idle time. Runtime depends on how efficiently each engine handles your query shapes, on caching and layout, and on concurrency packing. That is why vendor benchmarks, which pick favorable workloads, don't settle anything.

**TCO includes people.** A platform that needs two fewer engineers can cost more on the invoice and less overall. Equally, a strong team running tuned jobs on spot capacity can beat a managed premium. See [§13](13-performance-cost.md).

### Governance across two platforms

Unity Catalog and Horizon both offer RBAC, tags, masking, row filters, lineage and audit. The core problem is that **policies don't travel with files**.
- Object storage authorizes whole files, so row- and column-level rules need a trusted engine to enforce them.
- A Snowflake masking policy isn't applied when Spark reads the same Iceberg files through another catalog, and a Unity Catalog row filter isn't applied in Snowflake.

Catalogs bridge part of this with **credential vending**. The catalog authenticates the caller, then hands out short-lived storage credentials scoped to the table. That gives table-level control over any engine, but fine-grained rules only hold if the reading engine honors them.

A workable pattern (P9):
- **One catalog of record per table.** It owns the table's policies; the other side reads through federation or a catalog link.
- **One identity source.** The same IdP and SCIM groups on both platforms.
- **One tag taxonomy.** Tags are applied at write time, and policies are kept as code and deployed to both platforms.
- **One serving path for restricted data.** It goes only through the engine that enforces the policy; the other side gets a masked or aggregated product.
- **Merged audit.** Combine Databricks system tables and Snowflake `ACCOUNT_USAGE` in one place to catch drift.

See [§12](12-governance-security.md).

### Interop: one copy, two engines

```
      single writer: owns commits AND maintenance
                        |
   catalog of record: Unity Catalog  or  Horizon
                        |  Iceberg REST + vended credentials
            +-----------+-----------+
     Databricks engine        Snowflake engine
            +-----------+-----------+
     Parquet + Iceberg/Delta metadata in ONE bucket
```

| Direction | Mechanism | Status |
|---|---|---|
| Databricks → Snowflake | Unity Catalog exposes managed Iceberg tables, or Delta tables with UniForm metadata, through its Iceberg REST endpoint. Snowflake connects via a catalog integration plus a **catalog-linked database**, which discovers tables automatically | Catalog-linked databases on external Iceberg REST catalogs **(GA)** Oct 2025. Snowflake writes to UC-managed Iceberg **(GA)** on AWS, and on Azure since Apr 2026; Google Cloud (⚠ verify) |
| Databricks → Snowflake, shared | Snowflake consumes a **Delta share** (Databricks renamed Delta Sharing to OpenSharing in Jun 2026) as a catalog-linked database in Horizon, converting its tables to Iceberg, with Horizon policies applied | **(GA)** Jul 2026. Read-only in Snowflake, Delta tables only, and needs vended credentials or an external volume |
| Delta files → Snowflake | **Delta Direct**: a catalog integration with `CATALOG_SOURCE = OBJECT_STORE` and `TABLE_FORMAT = DELTA` reads the Delta log from storage. It can't use a Delta definition held in UC or Glue | **(GA)**. Reads deletion vectors since Mar 2026 |
| Snowflake → Databricks | External engines read Snowflake-managed Iceberg tables through Horizon's Iceberg REST endpoint. Databricks **catalog federation** reads those files directly, on Databricks compute | Horizon reads **(GA)** Feb 2026, writes **(GA)** May 2026. Databricks Snowflake catalog federation **(GA)** |
| Snowflake native tables → Databricks | Lakehouse Federation **query federation**: read-only JDBC pushdown, so Snowflake compute runs the query and you pay both platforms | Fits exploratory or low-volume access, not bulk pipelines |
| Neutral catalog | Both engines use Snowflake Open Catalog (managed Polaris) or another Iceberg REST catalog | Check per-engine feature coverage |

**Failure modes:**
- **Catalog ambiguity.** If two catalogs both accept commits, each holds its own current-snapshot pointer (P3). One engine reads stale data or silently overwrites the other's commit.
- **Concurrent writers.** Optimistic concurrency only works through a single catalog, so pick one writer per table.
- **Feature lag.** Deletion vectors, Iceberg v3 types and variant columns are supported unevenly across engines. Limit tables to the features both engines support, and test format upgrades on both.
- **Maintenance ownership.** Only the owner compacts tables or expires snapshots. If the other side does it, the owner's readers break.
- **Metadata lag.** UniForm writes Iceberg metadata asynchronously after each Delta commit, so Iceberg readers can trail briefly.
- **Egress.** Keep the bucket in the same region as both platforms' compute.

### Where Microsoft Fabric fits

Fabric is Microsoft's SaaS analytics platform: Spark, a SQL warehouse, real-time analytics, pipelines and Power BI on one storage layer and one bill.

- **OneLake** is one logical lake per tenant, with Delta Parquet as the native format and ADLS-compatible APIs. **Shortcuts** point to data in ADLS, S3, GCS or elsewhere in OneLake, with no copy.
- **Mirroring** comes in several forms:
  - **Snowflake mirroring (GA)** replicates Snowflake native tables into OneLake as Delta, kept in sync in near real time. This is a copy; Iceberg tables are mirrored as metadata through shortcuts instead. Mirroring storage is free up to a quota tied to capacity size (for example, F64 includes 64 TB).
  - **Mirrored Azure Databricks catalog (GA)** mirrors Unity Catalog metadata and reads the same tables through shortcuts, so no data moves. Publishing a UC catalog to OneLake from the Azure Databricks side is **(Preview)**.
  - **OneLake–Snowflake interoperability (GA)** lets Snowflake store its managed Iceberg tables in OneLake and read Fabric tables as Iceberg.
- **Capacity pricing.** An F SKU is a fixed pool of capacity units (CUs) shared by every Fabric workload. Usage is smoothed over time, and sustained overuse is throttled. Spend is predictable, but one workload can starve another unless you split capacities.
- **Power BI Direct Lake.** Semantic models load Delta columns from OneLake into memory on demand. That gives near-import speed without scheduled import refreshes, with DirectQuery fallback in some modes.

**Where it fits:** Microsoft-centric, Power BI-heavy organizations, often as the BI and consumption layer over Snowflake or Databricks data. **Where it doesn't:** multi-cloud estates, workloads needing strict compute isolation, and teams already deep in another platform, for whom Fabric adds a third governance model.

## Comparison

**Decision matrix**

| Workload / team / constraint | Lean | Mechanism behind it |
|---|---|---|
| SQL-skilled analysts, BI-heavy, small platform team | Snowflake | Platform owns layout and tuning (P10) |
| Spiky dashboard concurrency, many consumers | Snowflake (Databricks serverless SQL is close) | Multi-cluster scale-out per workload (P2) |
| Heavy Python, ML training, GPUs, feature pipelines | Databricks | Spark and ML runtimes, with models governed in UC |
| Complex stateful or event-time streaming | Databricks, or a dedicated processor ([§11](11-stream-processing.md)) | Structured Streaming state |
| Minute-level incremental SQL ELT | Either | Dynamic Tables vs. declarative pipelines (P7) |
| Open-format mandate, many engines | Databricks, or either with Iceberg | Open by default vs. opt-in (P11) |
| External data sharing and monetization | Snowflake; Delta Sharing for non-Snowflake recipients | No-copy sharing, mature marketplace |
| Need control of instances, spot, runtime | Databricks | Classic compute in your account |
| Limited engineering, zero-ops mandate | Snowflake | Fewer knobs, fewer failure modes |
| OLTP app state next to analytics | Either | Snowflake Postgres / Lakebase |
| Multi-party privacy-safe collaboration | Either | Snowflake Data Clean Rooms / Databricks Clean Rooms |
| Azure- and Power BI-centric | Either, plus Fabric for BI | Direct Lake, mirroring, shortcuts |
| Very large always-on compute | Benchmark | Raw VM price vs. managed premium and people cost |

## Interview questions

**Q: Why Snowflake when Databricks exists?**

You buy an operating model as much as an engine. Snowflake owns the format, metadata and tuning, so a small team gets fast, concurrent SQL with almost nothing to run, and per-workload warehouses isolate BI from ELT by design (P2, P10). Sharing is mature because storage is central, so a share is a grant rather than a copy. I wouldn't make it the primary platform for heavy custom ML or complex stateful streaming, and I'd model the cost of large always-on compute first.

**Q: Why Databricks when Snowflake exists?**

One open copy of data serves SQL, streaming, data science and ML under one catalog, and you keep control of compute (P11). It suits organizations whose hard problems are engineering problems: messy ingestion, streaming, Python feature pipelines, model training and serving, and a no-lock-in mandate. The price is surface area. Clusters, runtimes, layout and cost need engineering discipline, even with serverless. For a small SQL-only BI team it's more platform than they need.

**Q: Your company runs both; how do you split workloads?**

Split by strength and by the direction data flows, not by team preference. Commonly that's Databricks for ingestion, engineering and ML, and Snowflake for curated BI serving and external sharing, or the reverse where Snowflake is already the warehouse of record. The boundary is a set of contracted data products (P8), each with one writer and one catalog of record, read across through Iceberg instead of copied. Centralize identity, tags and cost reporting. If the overlap keeps growing, consolidation is a legitimate outcome, because running both costs two governance models and two skill sets.

**Q: How would you govern data consistently across both?**

Policies don't travel with files, so each table gets one catalog of record that owns and enforces its policies (P9). Use one IdP with SCIM-synced groups and a shared tag taxonomy applied at write time, and keep policies as code deployed to both Unity Catalog and Horizon. Serve fine-grained-protected data only through the engine that enforces it, and give the other platform a masked or aggregated product. Merge both platforms' audit logs in one place, because silent drift between two policy sets is the usual failure.

**Q: How do you avoid two copies of the data?**

Store each shared table once, as Iceberg or as Delta with UniForm, with one catalog of record that owns commits and maintenance. The other engine reads through Iceberg REST: Snowflake through a catalog-linked database pointed at Unity Catalog, or Databricks through catalog federation against Horizon. Delta Sharing into Snowflake is also a no-copy path. The failure modes are writers in two catalogs, table features one engine can't read, and maintenance run by the wrong side. Sometimes a governed copy is still right, for example when the consumer needs native-only performance or the alternative is cross-region reads.

**Q: Compare the cost models.**

Snowflake bills credits for warehouse time per second (60-second minimum per resume) at a rate that doubles per size step, plus serverless features, cloud services above 10% of daily warehouse use, and storage. Databricks bills DBUs by compute type; classic compute adds your cloud VM bill, while serverless bundles infrastructure into the DBU rate. Unit prices don't compare directly, because cost is rate × runtime plus idle time, and runtime depends on the engine and how well concurrency is packed. I'd benchmark our top workloads on both, include people in TCO, and hunt for idle leaks.

**Q: Where does Fabric fit?**

Fabric is Microsoft's SaaS analytics platform on OneLake, with capacity pricing and native Power BI. In an Azure and Power BI shop it usually fits first as the consumption layer: Azure Databricks tables mirrored as metadata through shortcuts, Snowflake data mirrored or read as Iceberg, and Direct Lake serving Power BI without import refreshes. As a full replacement, weigh contention on shared capacity, uneven feature maturity, and the third governance model it adds. It's a poor fit for multi-cloud estates or teams that need strict per-workload compute isolation.

**Q: What's converging, and does the choice still matter?**

Features are converging: both have SQL warehouses, Python, notebooks, Iceberg, managed Postgres, ingestion connectors, semantic layers and AI assistants. Defaults and maturity aren't. Snowflake's default path is managed storage and SQL, Databricks' is open tables and code, and each one's non-default path is younger. So the choice now turns on operating model, team skills, cost on your workloads, and where the catalog of record lives, which is the new lock-in point (P11). Pick the platform whose default path is your main workload.

**Q: Dashboards slow down during nightly ELT. How does each platform fix that?**

Both fix it by giving each workload its own compute over shared storage (P2). In Snowflake that's separate warehouses, with a multi-cluster warehouse for BI so peaks add clusters instead of queuing. In Databricks it's a dedicated serverless SQL warehouse for BI while ELT runs on job compute. If dashboards are still slow after that, the cause is usually the data: ELT is rewriting the tables BI reads, which invalidates caches and layout. Publish gold tables atomically, on a schedule.

**Q: Both platforms write to the same Iceberg table. What goes wrong?**

An Iceberg commit atomically swaps the current-metadata pointer in a catalog (P3). Writers using one shared catalog just conflict and retry. Writers using different catalogs each see a different "current" pointer, so one silently reads stale data or overwrites the other's snapshot. Maintenance run from one side can also delete files the other side still references. Make one platform the writer and owner, and route the other platform's changes through it.

**Q: How would you run a fair cost and performance bake-off?**

Use your own top workloads by spend, not vendor benchmarks, covering ELT, BI concurrency and at least one ML pipeline. Let each platform's experts tune their side, then measure cost per completed workload at the required SLA, including idle time. Add operating effort and migration cost to get TCO. Re-run periodically, because engines and price lists change.

**Q: To serve Databricks data to Snowflake, would you use managed Iceberg, UniForm or Delta Sharing?**

Managed Iceberg in Unity Catalog is cleanest when Snowflake is a first-class reader, because there's one format and no translation. UniForm keeps Delta for Databricks-native features and adds Iceberg metadata asynchronously, so expect brief lag and limit the table to features Iceberg readers support. Delta Sharing fits a consumer in another account or organization, since a share is a revocable, audited grant, and Snowflake now consumes shares natively. In every case there's one writer, and the consumer doesn't write back.

**Q: Where is the lock-in in each platform?**

Snowflake native tables are readable only by Snowflake. Iceberg tables remove the format lock-in, but proprietary features such as sharing, governance and some performance features remain. Databricks data is open, so its lock-in sits in Unity Catalog policies and lineage, jobs, pipelines and runtime-specific features. On both, the catalog with its permissions, lineage and semantics is the real lock-in point (P11). Estimate exit cost by what it takes to migrate policies and pipelines, not bytes.

**Q: Both platforms now offer managed Postgres. Why, and when would you use it?**

Applications need fast single-row reads and writes, which columnar engines handle poorly (P1). Reverse ETL to a separate database adds pipelines, copies and governance gaps. Snowflake Postgres and Lakebase put OLTP next to the analytical data, with sync paths and shared governance. That suits app state, serving lookups and agent memory. I wouldn't move a high-volume core system of record onto either until its HA and operational track record meets our SLAs.

## Takeaways
- Snowflake bet on managed SQL over vendor-owned storage; Databricks bet on code over open files. Their defaults still follow those bets.
- The core architectural difference is where table metadata lives: in a vendor service layer, or in a log beside the data.
- Choose the platform whose default path is your main workload and whose operating model fits your team.
- If you run both: one writer and one catalog of record per table, with contracts at the boundary.
- Price per unit isn't price per job, and TCO includes people.
- Policies don't travel with files, so serve restricted data through the engine that enforces it.
- Iceberg makes "one copy, two engines" real. Its failure modes are catalog ambiguity, feature lag and maintenance ownership.
- Fabric fits first as a Microsoft-centric consumption layer.
- Features have converged; defaults haven't. The catalog is the new lock-in point.

## Sources
- https://docs.snowflake.com/en/user-guide/data-integration/openflow/about
- https://docs.snowflake.com/en/user-guide/data-integration/openflow/about-byoc
- https://docs.snowflake.com/en/release-notes/2026/other/2026-02-24-snowflake-postgres-ga
- https://docs.snowflake.com/en/user-guide/snowflake-postgres/about
- https://docs.snowflake.com/release-notes/2025/other/2025-10-17-iceberg-external-writes-cld-ga
- https://docs.snowflake.com/release-notes/2026/other/2026-04-06-iceberg-write-support-azure-unity-catalog
- https://docs.snowflake.com/en/release-notes/2026/other/2026-02-06-tables-iceberg-query-using-external-query-engine-snowflake-horizon-ga
- https://docs.snowflake.com/release-notes/2026/other/2026-05-26-tables-iceberg-query-using-external-query-engine-snowflake-horizon-writes-ga
- https://docs.snowflake.com/en/user-guide/tables-iceberg-query-using-external-query-engine-snowflake-horizon
- https://docs.snowflake.com/en/release-notes/2026/other/2026-07-21-delta-sharing-horizon-catalog-ga
- https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-catalog-integration-object-storage
- https://docs.snowflake.com/en/user-guide/warehouses-considerations
- https://snowflake.com/legal-files/CreditConsumptionTable.pdf
- https://docs.snowflake.com/en/release-notes.html
- https://docs.snowflake.com/en/release-notes/llms.txt
- https://docs.snowflake.com/en/release-notes/2025/other/2025-05-05-gen2-standard-warehouses
- https://docs.snowflake.com/en/user-guide/warehouses-gen2
- https://docs.snowflake.com/en/user-guide/warehouses-adaptive
- https://docs.snowflake.com/en/release-notes/2026/other/2026-06-16-adaptive-compute-ga
- https://docs.snowflake.com/en/user-guide/cost-understanding-compute
- https://docs.snowflake.com/en/developer-guide/snowpark-connect/snowpark-connect-overview
- https://docs.snowflake.com/release-notes/2025/other/2025-11-04-snowflake-intelligence
- https://docs.snowflake.com/en/user-guide/snowflake-cortex/snowflake-cowork
- https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-catalog-integration-delta-sharing
- https://docs.snowflake.com/en/release-notes/2026/other/2026-03-02-iceberg-delta-deletion-vectors
- https://docs.snowflake.com/en/user-guide/tables-iceberg-externally-managed-writes
- https://docs.snowflake.com/en/release-notes/2026/other/2026-01-30-iceberg-microsoft-fabric-bidirectional-data-access-ga
- https://docs.databricks.com/aws/en/external-access/iceberg
- https://docs.databricks.com/aws/en/delta/iceberg-reads
- https://docs.databricks.com/aws/en/query-federation/snowflake-catalog-federation
- https://docs.databricks.com/aws/en/query-federation/catalog-federation
- https://docs.databricks.com/aws/en/query-federation/database-federation
- https://docs.databricks.com/aws/en/release-notes/product/
- https://docs.databricks.com/aws/en/release-notes/product/2026/june
- https://learn.microsoft.com/en-us/azure/databricks/release-notes/product/
- https://docs.databricks.com/aws/en/release-notes/lakebase/
- https://learn.microsoft.com/en-us/azure/databricks/partners/bi/fabric
- https://learn.microsoft.com/en-us/fabric/mirroring/snowflake
- https://learn.microsoft.com/en-us/fabric/mirroring/overview
- https://learn.microsoft.com/en-us/fabric/mirroring/azure-databricks
- https://learn.microsoft.com/en-us/fabric/onelake/onelake-iceberg-snowflake
- https://learn.microsoft.com/en-us/fabric/fundamentals/direct-lake-overview
