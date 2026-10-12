# 05. Snowflake I: Architecture, Performance, and Cost

> As of October 2026. Tags: **(GA)** generally available · **(Preview)** public/private preview · **(⚠ verify)** not confirmed against official docs.

## Why this exists

Around 2012 the analytic warehouse was a shared-nothing MPP system (Teradata, Netezza, and early Redshift in the cloud). Each node owned a slice of the disk, so storage and compute were welded together. That caused four chronic problems:

- **Resize means reshuffle.** Adding nodes meant redistributing data across them, which took hours or days. Clusters were sized for peak load and sat idle the rest of the time.
- **Concurrency contention.** ETL, BI and ad-hoc users shared the same nodes, so one heavy load slowed every dashboard. The only way to isolate them was to buy a second cluster and copy the data into it.
- **Ops burden.** Distribution keys, vacuuming, statistics, indexes, upgrades and capacity planning were all the DBA's job.
- **Semi-structured pain.** JSON had to be flattened by ETL before it could be queried.

Snowflake's bet is to store data once in cloud object storage (P2), let any number of independent compute clusters read the same bytes, and sell the whole thing as a fully managed SQL service with very few knobs (P10). Snowflake calls this design **multi-cluster, shared data**. Compute is elastic and disposable, storage is cheap and durable, and a central metadata service keeps the two consistent. The price is a proprietary storage format and less physical control than a lakehouse gives you (P11).

## Core concepts

### Databricks → Snowflake concept map

| Databricks | Snowflake | Note |
|---|---|---|
| Account | Organization | Groups accounts; billing and org-level admin |
| Workspace + regional metastore | Account | A Snowflake account lives in one cloud region and is the security boundary: users, roles and objects all live inside it |
| Unity Catalog `catalog.schema.table` | `database.schema.object`, governed through Horizon Catalog | Same three-level namespace. Governance is covered in [§12](12-governance-security.md) |
| Classic cluster / SQL warehouse | Virtual warehouse | Databricks SQL warehouses use the same T-shirt sizes and min/max cluster model, so this maps almost 1:1 |
| Serverless compute | Every warehouse | Snowflake runs the VMs in its own cloud account, so you never see them |
| DBU (plus a separate cloud VM bill on classic compute) | Credit | A credit already includes the infrastructure cost |
| Delta/Parquet files in your bucket | Micro-partitions in Snowflake-managed storage | Proprietary format. Iceberg tables are the open option ([§04](04-table-formats-catalogs.md)) |
| `_delta_log` | Cloud services metadata | A transactional service, not files you can read |
| Delta data-skipping stats (first N columns) | Per-micro-partition stats on every column | Maintained automatically |
| Z-order / liquid clustering | Clustering key + Automatic Clustering | Liquid clustering is the closer analogy: incremental, runs in the background, and the key can change |
| `OPTIMIZE`, predictive optimization | Nothing to run; Automatic Clustering, Snowflake Optima | File sizing and compaction are never your job |
| Bloom filter indexes | Search Optimization Service | |
| Photon | Built in | The engine is columnar and vectorized, with nothing to switch on |
| Disk cache | Warehouse cache (local SSD) | Lost when the compute goes away |
| SQL query result cache | Result cache in cloud services | Survives warehouse suspension |
| Delta time travel / shallow clone | Time Travel / zero-copy clone | See [§06](06-snowflake-features.md) |
| Spark on a cluster | Snowpark on a (Snowpark-optimized) warehouse | See [§06](06-snowflake-features.md) |

### Three layers

```
 clients ─▶ CLOUD SERVICES  (always on, multi-tenant, run by Snowflake)
            auth · RBAC · parse/optimize · metadata · transactions · result cache
                    │  plan + list of micro-partitions to read
        ┌───────────┼──────────────┬───────────────┐
        ▼           ▼              ▼               ▼
     WH_ETL (L)  WH_BI (M ×1–4)  WH_DS (Snowpark) …   VIRTUAL WAREHOUSES
        │           │              │                 (elastic, per-second, local SSD cache)
        └───────────┴──────┬───────┘
                           ▼
       STORAGE: immutable columnar micro-partitions in S3 / ADLS / GCS
```

- **Storage.** Loaded data is converted to Snowflake's compressed columnar format and kept in the cloud provider's object storage, which Snowflake manages. You pay per compressed TB per month and never touch the files.
- **Compute.** A virtual warehouse is a cluster of nodes with memory and local SSD. It reads micro-partitions from storage, caches them, and runs the query. Warehouses share no state with each other, so ETL on one cannot slow BI on another, yet all of them see the same committed data.
- **Cloud services.** An always-on fleet that holds the metadata and coordinates everything. For every query it:
  1. authenticates the session and checks RBAC;
  2. parses the SQL and resolves names against the metadata;
  3. checks the result cache;
  4. prunes micro-partitions using min/max metadata, at compile time (P4);
  5. builds a cost-based plan, then queues it and sends it to a warehouse;
  6. for DML, commits by atomically publishing a new metadata version (P3).

  DDL, `SHOW`, cloning and some aggregates need no warehouse at all.

This is why the design matters: storage scales with data, compute scales with workload, and isolating a workload means starting another warehouse, not copying data. Concurrency is handled by adding compute that reads the same bytes.

### Micro-partitions

Snowflake splits all table data into **micro-partitions** automatically. Each one holds 50–500 MB of uncompressed data (less on disk), stored column by column with each column compressed separately. Remember the order of magnitude (tens to hundreds of MB), not the exact range.

For each micro-partition, the metadata layer records per-column min/max, distinct-value counts, null counts and other statistics. That enables three things:

1. **Pruning (P4).** The optimizer compares a predicate against every partition's min/max *before* reading any data. Only partitions whose range could hold matching rows get scanned, and within those only the referenced columns are read.

   ```
   partition   P1            P2            P3            P4
   event_date  [09-01..09-08][09-08..09-15][09-29..10-03][10-02..10-09]
   WHERE event_date = '10-01'  →  scan P3; P1, P2, P4 pruned from metadata alone
   ```

2. **Immutability gives you transactions (P3).** A partition is never modified in place. An `UPDATE` that touches one row writes a new copy of that row's whole partition and then swaps the metadata pointer. Snapshot isolation, Time Travel and zero-copy cloning follow almost for free ([§06](06-snowflake-features.md)). The cost is write amplification: heavy DML on large tables rewrites many partitions and keeps the old versions billed as storage.
3. **Natural clustering.** Partitions are created in load order, so a table loaded daily is already well clustered on its load date and pruning on that column costs nothing. On other columns, or after many updates and out-of-order loads, partition ranges overlap and pruning gets worse.

**Clustering depth** measures how bad the overlap is. For a given column, it is the average number of micro-partitions whose value ranges overlap. A depth near 1 means each value lives in very few partitions. A high depth means a filter has to scan many. Inspect it with `SYSTEM$CLUSTERING_INFORMATION`.

### Clustering keys and Automatic Clustering

A **clustering key** names the columns (or expressions) whose rows Snowflake should keep together in the same micro-partitions. **Automatic Clustering** then reclusters in the background on Snowflake-managed serverless compute, incrementally rewriting overlapping partitions into better-sorted ones. You don't provide a warehouse. You pay credits for the rewrites, plus storage for the replaced partitions that Time Travel and Fail-safe keep.

A clustering key is worth it when:

- the table is large and has many micro-partitions (the docs say this typically means multiple TB);
- most queries filter, join or sort selectively on the same few columns;
- the table is queried much more often than it changes.

It is not worth it, or does harm, when:

- the table is small, or its natural load order already matches the filters;
- the key has very high cardinality (a raw timestamp or a UUID): upkeep is expensive and the gain is small. Use an expression such as `TO_DATE(ts)` instead;
- the key has very low cardinality (a boolean), which gives little pruning;
- the table churns heavily, so you pay to recluster constantly.

Use at most 3–4 columns, ordered from lowest to highest cardinality. Try it on one or two tables first and watch `AUTOMATIC_CLUSTERING_HISTORY`. The docs also describe a newer **Optima Clustering** billing model, priced on the volume of data ingested rather than compute hours **(⚠ verify status and rate)**.

### Other ways to skip work

- **Search Optimization Service (SOS)** (Enterprise). Builds a persistent *search access path*, an index-like structure maintained on serverless compute, for selective lookups:
  - equality or `IN` predicates that return a few rows;
  - substring and regex matches (`LIKE`, `RLIKE`);
  - semi-structured fields.

  Use it for needle-in-a-haystack queries on columns that clustering can't serve, since a table has only one sort order. Cost grows with the number of enabled columns, their distinct values and how much the table churns, so enable it column by column. **Snowflake Optima** (on Gen2 and Adaptive warehouses, all editions) builds indexes like these automatically for repeated point lookups, at no extra cost.
- **Query Acceleration Service (QAS)** (Enterprise). Offloads parts of eligible queries to shared serverless compute, so one outlier query doesn't force you to upsize the whole warehouse. Eligible work is mainly large scans with selective filters or aggregations, plus large `INSERT`/`COPY`. A *scale factor*, a multiple of the warehouse size, caps the extra compute (default 8). Find candidates with `SYSTEM$ESTIMATE_QUERY_ACCELERATION` or the `QUERY_ACCELERATION_ELIGIBLE` view. A pending behavior change would turn QAS on by default for new standard warehouses **(⚠ verify)**.
- **Materialized views** (Enterprise). Precomputed, automatically maintained results for repeated aggregations over one table. See [§06](06-snowflake-features.md).

### Virtual warehouses

**Sizing.** Sizes run from XS to 6XL. Each step up doubles the compute and the credits per hour: for Gen1 standard warehouses, XS = 1, S = 2, M = 4 … 4XL = 128, 6XL = 512. Remember "XS = 1, doubling each size" and work out the rest.

**Billing.** Warehouses bill per second, with a 60-second minimum each time one starts or resumes. A warehouse that resumes, runs for 5 seconds and suspends is billed for 60. A warehouse that keeps suspending and resuming pays that minimum again every time.

**Auto-suspend and auto-resume.** A warehouse suspends after `AUTO_SUSPEND` seconds with no activity (default 600) and resumes automatically, usually within seconds, when the next query arrives. A suspended warehouse costs nothing, but its cache is lost.

**Scale up vs scale out.**

- *Scale up* (a bigger size) makes one query faster, provided its work parallelizes: large scans, big joins and sorts, and especially queries that spill to disk. If a query scales linearly, double the size at half the runtime costs the same credits, so you get the lower latency for free. Scaling up does not help small queries, queuing or data skew.
- *Scale out* (a multi-cluster warehouse, Enterprise) adds clusters of the same size when queries start to queue. It fixes concurrency, not the speed of a single query.
  - Set `MIN_CLUSTER_COUNT` and `MAX_CLUSTER_COUNT`. Equal values run in maximized mode; different values run in auto-scale mode.
  - The maximum cluster count depends on size: up to 300 for XS–M, down to 10 for 4XL and above. Values above 10 must be set with SQL.
- *Scaling policy* (auto-scale mode only).
  - **Standard** (the default) starts a new cluster as soon as queries queue, which keeps waiting to a minimum.
  - **Economy** starts a cluster only when it estimates there is enough work to keep it busy (about 6 minutes **(⚠ verify)**), accepting some queuing to save credits.

Within one cluster, concurrency is limited by `MAX_CONCURRENCY_LEVEL` (default 8 **(⚠ verify)**). Queries beyond that limit wait in a queue.

**Warehouse types and generations.**

- **Gen2 standard warehouses** **(GA, May 2025)**: newer hardware plus engine improvements. They cost more credits per hour than Gen1 but finish most work faster. Create one with `GENERATION = '2'`. The 2026_03 behavior change makes Gen2 the default for new standard warehouses where it is available **(⚠ verify rollout)**. There is no 5XL or 6XL. Third-party sources put the rate at about 1.35× Gen1 on AWS and 1.25× on Azure **(⚠ verify against the Service Consumption Table)**.
- **Snowpark-optimized** **(GA)**: 16× the memory per node by default (`RESOURCE_CONSTRAINT = MEMORY_16X`). Use these for memory-hungry Python or Java UDFs, ML training and large Snowpark DataFrames. The 1 TB memory tiers are **(Preview)**.
- **Adaptive Compute** **(GA June 2026, select AWS regions, Enterprise+)**: replaces the fixed T-shirt size with a workload-aware engine and bills per query. You control it with `MAX_QUERY_PERFORMANCE_LEVEL` and `QUERY_THROUGHPUT_MULTIPLIER`. It is Snowflake's answer to the problem that sizing is still a manual guess.
- **Interactive warehouses** **(GA Dec 2025)**: built for sub-second, high-concurrency dashboards and APIs over interactive tables. The minimum billing period is 1 hour and the minimum auto-suspend is 24 hours, so they are for always-on serving, not batch.

### Caching: three layers

1. **Result cache** (in cloud services). An exact repeat of a query returns the stored result without running any warehouse.
   - **Retention.** A result is kept for 24 hours, and each reuse resets that clock, up to 31 days after the query first ran.
   - **Reuse conditions.** The query text must be identical and the underlying data unchanged. The query must not use non-reusable functions (`RANDOM`, `UUID_STRING`), external functions or hybrid tables. The role running it must have the required privileges.
   - Set `USE_CACHED_RESULT = FALSE` when benchmarking.
2. **Warehouse cache** (each warehouse's local SSD). Holds the column data of micro-partitions read from storage. It is dropped when the warehouse suspends or is resized down, which is the tradeoff behind the auto-suspend setting. Snowflake suggests about 10 minutes for BI warehouses and about 5 for ETL or dev, where the cache matters less.
3. **Metadata.** Row counts and min/max are kept per partition, so `COUNT(*)`, `MIN` and `MAX` over a table can be answered without scanning any data **(⚠ verify which types and predicates qualify)**.

### Credit model

- **Warehouse compute.** Credits per hour for the size × seconds running, with a 60-second minimum per resume. Usually most of the bill.
- **Serverless features.** Snowpipe, Automatic Clustering, SOS upkeep, MV refresh, serverless tasks and QAS are billed in credits at their own rates. They never sit idle, but they can grow without anyone noticing.
- **Cloud services.** Uses credits too, but you are billed only for the part above 10% of that day's warehouse compute. The check runs per UTC day, and serverless usage doesn't count toward the 10%. Normal workloads pay nothing here. Millions of tiny queries, heavy metadata calls or very complex compiles can push you over.
- **Storage.** A flat rate per compressed TB per month, averaged daily. It includes bytes held for Time Travel and for Fail-safe (7 days on permanent tables). On-demand prices are in the tens of dollars per TB-month **(⚠ verify for region and contract)**.
- **Data transfer.** Ingress is free. Egress across regions or clouds, or out to the internet, is charged: replication, cross-region sharing and unloads.

Editions decide which features you get, not how fast queries run. The credit price depends on edition, cloud, region and contract.

| Edition | Adds |
|---|---|
| Standard | Core engine, Time Travel up to 1 day, Fail-safe |
| Enterprise | Multi-cluster warehouses, Time Travel up to 90 days, SOS, QAS, MVs, row/column policies, tagging, Adaptive Compute |
| Business Critical | Tri-Secret Secure (customer-managed key), private connectivity, support for regulated data (e.g., HIPAA, PCI DSS) |
| Virtual Private Snowflake | Business Critical in a fully isolated environment, with its own metadata store and compute |

### Reading performance

Open the Query Profile and ask four questions:

1. **Pruning.** Compare partitions scanned with partitions total. A selective filter that scans 95% of the table means the layout doesn't match the filter. Fix it with a clustering key or SOS, or by rewriting the predicate: functions wrapped around the column, mismatched types and filters that apply only through a join can all defeat pruning.
2. **Spilling.** "Bytes spilled to local storage" means memory ran out and the warehouse fell back to its SSD. "Remote" means the SSD ran out too and the warehouse is writing to object storage, which is far slower. Fix it with a larger warehouse, smaller batches or a better join or aggregation. Remote spill is the classic reason to scale up. With QAS enabled, a small amount of remote spill is normal.
3. **Queuing.** Time spent queued (`QUEUED_OVERLOAD_TIME` in `QUERY_HISTORY`) points to concurrency, not size. Scale out or move workloads to separate warehouses.
4. **Operator hotspots.** Look for exploding joins (far more output rows than input rows), skew, or a huge sort with no `LIMIT`.

A useful lens is bytes scanned per useful row returned. Shrink the bytes first (P4), and only then decide whether to add compute.

### Failure modes

- **Oversized idle warehouses.** An XL with a 1-hour auto-suspend burns 16 credits an hour doing nothing.
- **Auto-suspend set badly.** Too aggressive (60 s on a BI warehouse) means a cold cache on every visit plus repeated resume minimums. Too loose means paying for idle time.
- **Clustering everything.** Background credits spent on tables that never benefit.
- **Shared warehouses.** Ad-hoc users on the ETL warehouse cause queuing and cost no one can attribute. Give each class of workload its own warehouse.
- **Runaway queries.** A cartesian join on a 4XL keeps running until the default statement timeout of 2 days **(⚠ verify)** unless you set `STATEMENT_TIMEOUT_IN_SECONDS`.
- **Serverless creep.** SOS or clustering left on high-churn tables.
- **Guardrails.** Use resource monitors (credit quotas that notify or suspend warehouses) and budgets. See [§13](13-performance-cost.md).

## Comparison

**Pick the lever by symptom**

| Symptom (Query Profile) | Lever | Mechanism | Billed as | Don't use when |
|---|---|---|---|---|
| Scans most partitions on a selective range filter | Clustering key | Re-sorts partitions so min/max ranges stop overlapping | Serverless reclustering credits + storage churn | Small, high-churn, or already naturally clustered tables |
| Point lookup / substring on high-cardinality column | Search Optimization | Search access path tells the engine which partitions hold the value | Serverless build/upkeep + storage | Queries return many rows; columns rarely queried |
| Rare huge scan on a warehouse sized for typical queries | QAS | Borrows serverless compute for scan/filter fragments | Serverless credits, capped by scale factor | Query is not eligible (check the estimate function first) |
| Spill to remote storage | Scale up | More memory and SSD per query | Warehouse credits (2× per size) | The issue is queuing or skew |
| Queued time, many concurrent users | Multi-cluster (scale out) | More same-size clusters | Warehouse credits per running cluster | Single queries are slow |
| Same expensive aggregate run all day | Materialized view / result cache | Precomputed or reused result | MV refresh credits / free | Base table changes constantly |

**Snowflake standard table vs Delta table**

| | Snowflake standard table | Delta table |
|---|---|---|
| Data files | Proprietary micro-partitions, Snowflake-managed storage | Parquet in your bucket |
| Transaction metadata | Cloud services metadata store | `_delta_log` JSON + checkpoints next to the data |
| File stats | Every column, automatic | First N columns by default, configurable |
| Layout maintenance | Automatic (sizing); clustering optional, serverless | `OPTIMIZE`/liquid clustering/predictive optimization |
| Who can read it | Snowflake engine only (export via SQL, sharing, unload) | Any Delta-capable engine |
| Old versions | Time Travel + 7-day Fail-safe, billed as storage | Retained until `VACUUM` |
| Open alternative | Snowflake-managed Iceberg tables ([§04](04-table-formats-catalogs.md)) | Native |

## Interview questions

**Q: Explain Snowflake's architecture and why it matters.**

There are three layers. Storage is immutable columnar micro-partitions in object storage. Compute is any number of isolated virtual warehouses. Cloud services is the always-on layer that handles metadata, optimization, transactions and security. Separating them means storage and compute scale and bill independently (P2). You isolate a workload by giving it its own warehouse instead of a copy of the data, and resizing takes seconds because no data has to be redistributed. The tradeoff is that the storage format and metadata are proprietary and managed: you give up physical control and openness (P10, P11) in exchange for almost no ops work. Iceberg tables are the hedge.

**Q: What is the cloud services layer actually doing?**

It is the brain and the transaction manager. On every query it authenticates, checks privileges, parses and optimizes, prunes micro-partitions from metadata, checks the result cache, queues and dispatches work to a warehouse, and commits DML by atomically publishing a new metadata version. That is why cached results, `COUNT(*)` and clones need no running warehouse, and why a commit is a metadata swap rather than a file rewrite. It uses credits too, but you pay only for the part above 10% of that day's warehouse compute. A workload of millions of trivial queries or metadata-heavy calls can make that line billable. It's also shared and managed, so you can't scale it yourself.

**Q: How does Snowflake make a query fast without indexes?**

Mostly by not reading data (P4). Per-partition min/max lets the optimizer prune at compile time, and columnar storage means only the referenced columns are read. The result cache and the warehouse cache skip repeated work, and vectorized execution in parallel across nodes handles the rest. When the filter column doesn't line up with load order, you add a clustering key, and for needle lookups SOS adds index-like access paths. So "no indexes" really means no user-maintained B-trees: the metadata is the index.

**Q: How is Snowflake's storage different from a Delta table?**

Both are immutable columnar files plus a metadata layer that gives ACID, time travel and clones (P3). Delta is Parquet in your bucket with a readable log that any Delta engine can use, and you maintain it with `OPTIMIZE`, `VACUUM` or predictive optimization. Snowflake keeps proprietary micro-partitions in storage it manages, with metadata in a transactional service and stats on every column. Sizing and compaction are automatic, but only Snowflake's engine can read the data directly. The tradeoff is operability and tight engine integration against open access and lock-in (P11). Snowflake-managed Iceberg tables are the middle ground.

**Q: Scale up vs scale out?**

Scaling up (a larger size) gives each query more nodes and memory. Use it when single queries are slow because of large scans, big joins or sorts, or spilling. If the work parallelizes, double the size at half the runtime costs the same credits. Scaling out (multi-cluster) adds clusters when queries queue because of concurrency, and no single query gets faster. Reading the profile tells you which: remote spill means scale up, queued time means scale out. Picking the wrong one wastes money and leaves the problem unsolved.

**Q: When would you define a clustering key and when not?**

Define one on a large, multi-TB table that is filtered repeatedly and selectively on columns its load order doesn't already sort, once clustering depth confirms that pruning is poor, and only if the table is read far more often than it changes. Skip it for small tables, high-churn tables, tables where natural load order already serves the filters, and raw high-cardinality keys (cluster on `TO_DATE(ts)`, not `ts`). Use at most 3–4 columns, ordered from low to high cardinality, then compare the credits spent against the drop in partitions scanned. A table has only one sort order, so serve secondary access patterns with SOS or an MV.

**Q: Clustering, Search Optimization, QAS, or a materialized view: how do you choose?**

They solve different problems. Clustering serves range or equality filters on one to three columns of a big table, used by many queries. SOS serves lookups of a few rows on high-cardinality columns, substrings and VARIANT paths that can't be the sort key. QAS absorbs occasional outlier scans on a warehouse sized for the typical query. An MV serves a repeated, expensive aggregation over one table. All four run on background or serverless compute, so each has to justify its cost on measured queries. "Turn everything on" is the anti-pattern.

**Q: What happens when a warehouse is suspended?**

The compute is released and billing stops, after that run's 60-second minimum. The local SSD cache is dropped. Data, metadata and the result cache are untouched, so cached results and metadata-only queries still return. The next query resumes the warehouse within seconds, but on a cold cache, so the first queries read more from remote storage and run slower. `AUTO_SUSPEND` is the dial: short saves idle credits but costs the cache and repeated minimums, long keeps the cache warm but pays for idle time. Match it to how far apart queries arrive.

**Q: How would you control Snowflake costs?**

Measure first: use `WAREHOUSE_METERING_HISTORY`, `QUERY_HISTORY` and the serverless history views, and attribute spend by warehouse and tag. Then fix the structure:

- one warehouse per workload;
- right-size by testing, picking the smallest size that meets the SLA without remote spill;
- tune auto-suspend to how queries arrive;
- cap multi-cluster maximums, and use the Economy policy where latency matters less.

Then cut work: use pruning-friendly layouts, build incrementally instead of doing full rebuilds, and avoid `SELECT *`. Add guardrails (resource monitors, budgets, statement timeouts) and review clustering and SOS spend against the benefit. Freshness costs money too (P7): a task scheduled every minute keeps a warehouse awake all day.

**Q: A dashboard query that used to take 2 seconds now takes 40. How do you diagnose it?**

Start with query history and split the time into queued, compiling and executing. If the time is queued, the problem is concurrency: scale out or isolate the workload. If it's executing, open the profile and check partitions scanned against total (pruning lost after out-of-order loads or a new filter), spill (data growth outgrew the size), and the most expensive operator (a join explosion after a data change). Also check whether the result cache stopped hitting, for example because a new frequent load keeps invalidating it, or whether a shorter auto-suspend left the cache cold. Fix the cause before upsizing.

**Q: When does the result cache not help?**

Any change to an underlying table invalidates the cached result, so tables loaded every few minutes rarely get hits. Query text must match exactly, so BI tools that inject different literals or comments miss. Non-reusable functions, external functions and hybrid tables are excluded, and the role must have privileges on the objects. Results live 24 hours from the last reuse, up to 31 days in total. Design around this: for dashboards on fresh data, lean on the warehouse cache, MVs or interactive tables, not the result cache.

**Q: How would you lay out warehouses for ETL, BI and data science?**

Isolation is cheap because every warehouse reads the same data, so separate them.

- **ETL**: sized for throughput, single cluster, short auto-suspend, scheduled.
- **BI**: multi-cluster auto-scale with the Standard policy, a moderate size, and a longer auto-suspend so the cache stays warm. Interactive warehouses suit sub-second, always-on serving.
- **Data science**: Snowpark-optimized warehouses for memory-heavy Python.
- **Ad-hoc**: small, with a cluster cap and a statement timeout.

Put a resource monitor on every warehouse and charge back by warehouse. Don't split so finely that dozens of warehouses each pay resume minimums and keep their own cold caches.

**Q: Gen2 and Adaptive Compute are new. How do they change sizing decisions?**

Gen2 runs faster hardware and engine improvements at a higher hourly rate, so the cost per job usually falls when runtime drops more than the rate rises. Benchmark it on your own workload, especially DML-heavy pipelines. Adaptive Compute removes the size decision and bills per query. That suits mixed, unpredictable workloads where hand-sizing is guesswork. The tradeoffs: you get less predictable spend per hour and less control, it's newer, and it needs Enterprise in a limited set of regions. Keep fixed-size warehouses where you need predictable budgets or have workloads you've already tuned.

**Q: What does micro-partition immutability cost you?**

Write amplification. Updating one row rewrites that row's whole micro-partition, so a MERGE-heavy CDC feed into a huge table rewrites many partitions. That costs compute, and the old versions stay billed as storage through Time Travel and the 7-day Fail-safe. Mitigations: batch changes, cluster on the merge key so updates touch few partitions, use transient tables for staging, and shorten retention on high-churn tables. For true row-level OLTP, use hybrid tables ([§06](06-snowflake-features.md)) or keep it in an OLTP database (P1).

## Takeaways
- Snowflake = storage in object storage, isolated elastic warehouses, and a cloud services layer that holds metadata and transactions; that separation is the whole point (P2).
- Micro-partitions (50–500 MB uncompressed) are immutable and columnar, with per-column stats: pruning, time travel and clones all come from this (P3, P4).
- The metadata is the index. Clustering keys fix range pruning on big tables; SOS fixes needle lookups; QAS absorbs outlier scans.
- Credits double per size, and billing is per second with a 60-second minimum per resume.
- Spill means scale up; queuing means scale out (multi-cluster, Enterprise).
- Suspend drops the warehouse cache; the result cache (24 h, up to 31 days) and metadata survive.
- Cloud services credits are billed only above 10% of daily warehouse compute.
- Most cost incidents are structural (shared, oversized, idle warehouses or always-on serverless features), not query-level.
- New compute options to know by name: Gen2 (GA), Snowpark-optimized (GA), Adaptive Compute (GA mid-2026, limited regions), interactive warehouses (GA).

## Sources
- https://docs.snowflake.com/en/user-guide/intro-key-concepts
- https://docs.snowflake.com/en/user-guide/tables-clustering-micropartitions
- https://docs.snowflake.com/en/user-guide/tables-clustering-keys
- https://docs.snowflake.com/en/user-guide/tables-auto-reclustering
- https://docs.snowflake.com/en/user-guide/search-optimization-service
- https://docs.snowflake.com/en/user-guide/search-optimization/cost-estimation
- https://docs.snowflake.com/en/user-guide/query-acceleration-service
- https://docs.snowflake.com/en/release-notes/bcr-bundles/2026_06/bcr-2373
- https://docs.snowflake.com/en/user-guide/snowflake-optima
- https://docs.snowflake.com/en/user-guide/warehouses-overview
- https://docs.snowflake.com/en/user-guide/warehouses-considerations
- https://docs.snowflake.com/en/user-guide/warehouses-multicluster
- https://docs.snowflake.com/en/release-notes/2025/other/2025-02-28-increased-max_cluster_count-limits
- https://docs.snowflake.com/en/user-guide/warehouses-gen2
- https://docs.snowflake.com/en/release-notes/2025/other/2025-05-05-gen2-standard-warehouses
- https://docs.snowflake.com/en/release-notes/bcr-bundles/2026_03/bcr-2250
- https://docs.snowflake.com/en/user-guide/warehouses-snowpark-optimized
- https://docs.snowflake.com/en/user-guide/warehouses-adaptive
- https://docs.snowflake.com/en/release-notes/2026/other/2026-06-16-adaptive-compute-ga
- https://docs.snowflake.com/en/user-guide/interactive
- https://docs.snowflake.com/en/release-notes/2025/other/2025-12-11-interactive-tables-ga
- https://docs.snowflake.com/en/user-guide/querying-persisted-results
- https://docs.snowflake.com/en/user-guide/performance-query-warehouse-cache
- https://docs.snowflake.com/en/user-guide/performance-query-warehouse-memory
- https://docs.snowflake.com/en/user-guide/ui-query-profile
- https://docs.snowflake.com/en/user-guide/cost-understanding-compute
- https://docs.snowflake.com/en/user-guide/cost-understanding-overall
- https://docs.snowflake.com/en/user-guide/resource-monitors
- https://docs.snowflake.com/en/user-guide/intro-editions
- https://docs.snowflake.com/en/user-guide/snowflake-horizon
