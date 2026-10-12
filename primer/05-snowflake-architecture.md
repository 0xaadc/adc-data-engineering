# 05. Snowflake I: Architecture, Performance, and Cost

> As of October 2026. Tags: **(GA)** generally available · **(Preview)** public/private preview · **(⚠ verify)** not confirmed against official docs.

## Why this exists

Around 2012 analytic warehouses were shared-nothing MPP systems (Teradata, Netezza, early Redshift). Each node owned a slice of the disk, so storage and compute were coupled. That caused four problems:

- **Resizing meant reshuffling.** Adding nodes meant redistributing data, which took hours or days, so clusters were sized for peak load and sat idle the rest of the time.
- **Concurrency contention.** ETL, BI and ad-hoc users shared the same nodes. The only way to isolate them was a second cluster holding a second copy of the data.
- **Ops burden.** Distribution keys, vacuum, statistics, indexes and upgrades were all the DBA's job.
- **Semi-structured pain.** JSON had to be flattened before it could be queried.

**Snowflake's bet:** store data once in cloud object storage, let any number of isolated compute clusters read the same bytes (P2), and sell it as a fully managed SQL service with very few knobs (P10). Snowflake calls this *multi-cluster, shared data*. The price is a proprietary storage format and less control over physical layout (P11).

## Core concepts

### Databricks → Snowflake concept map

| Databricks | Snowflake | Note |
|---|---|---|
| Account | Organization | Groups accounts and billing |
| Workspace + regional metastore | Account | Lives in one cloud region; it is the security boundary for users, roles and objects |
| Unity Catalog `catalog.schema.table` | `database.schema.object`, Horizon Catalog | Same three-level namespace ([§12](12-governance-security.md)) |
| SQL warehouse / cluster | Virtual warehouse | Databricks SQL copied the T-shirt sizes and min/max clusters, so this maps almost 1:1 |
| Serverless compute | Every warehouse | Runs in Snowflake's cloud account; you never see the VMs |
| DBU (+ VM bill on classic compute) | Credit | A credit includes the infrastructure |
| Delta/Parquet in your bucket | Micro-partitions in Snowflake-managed storage | Proprietary format. Iceberg is the open option ([§04](04-table-formats-catalogs.md)) |
| `_delta_log` | Cloud services metadata | A transactional service, not readable files |
| Z-order / liquid clustering | Clustering key + Automatic Clustering | Liquid clustering is the closer match (incremental, in the background) |
| `OPTIMIZE`, predictive optimization | Nothing to run | File sizing is automatic |
| Bloom filter index | Search Optimization Service | |
| Photon | Built in | The engine is vectorized and columnar, with no toggle |
| Disk cache | Warehouse cache (local SSD) | Lost when compute goes away |
| Query result cache | Result cache (cloud services) | Survives warehouse suspension |
| Time travel / shallow clone | Time Travel / zero-copy clone | [§06](06-snowflake-features.md) |

### Three layers

```
 clients ─▶ CLOUD SERVICES (always on, multi-tenant)
            auth · RBAC · optimizer · metadata · transactions · result cache
                  │ plan + list of micro-partitions to read
      ┌───────────┼──────────────┐
   WH_ETL (L)  WH_BI (M ×1–4)  WH_DS (Snowpark)     VIRTUAL WAREHOUSES
      └───────────┼──────────────┘                  (isolated, per-second, SSD cache)
                  ▼
   STORAGE: immutable columnar micro-partitions in S3 / ADLS / GCS
```

- **Storage.** Loaded data becomes compressed columnar files in object storage that Snowflake manages. You pay per compressed TB per month.
- **Compute.** A virtual warehouse is a cluster of nodes with memory and local SSD. Warehouses share no state with each other, so ETL can't slow BI. All of them see the same committed data.
- **Cloud services.** For every query this layer:
  1. authenticates and checks RBAC;
  2. parses and resolves names;
  3. checks the result cache;
  4. **prunes micro-partitions from metadata at compile time** (P4);
  5. builds a cost-based plan, then queues and dispatches it;
  6. commits DML by atomically publishing a new metadata version (P3).

  DDL, `SHOW` and cloning need no warehouse.

### Micro-partitions

Snowflake splits every table automatically into micro-partitions of **50–500 MB uncompressed** (less on disk). Each column is stored and compressed separately. Know the order of magnitude, not the exact range. The metadata keeps per-column min/max, distinct and null counts for every partition. That gives three properties:

1. **Pruning (P4).** Predicates are checked against min/max *before* any data is read. Only partitions that could match are scanned, and only the columns the query references.
   ```
   event_date  P1[09-01..09-08] P2[09-08..09-15] P3[09-29..10-03] P4[10-02..10-09]
   WHERE event_date = '10-01'  →  scan P3 only
   ```
2. **Immutability (P3).** An `UPDATE` to one row rewrites that row's whole partition and swaps the metadata pointer. Snapshots, Time Travel and zero-copy clones follow from this ([§06](06-snowflake-features.md)). So does write amplification on high-churn tables.
3. **Natural clustering.** Partitions are created in load order, so a table loaded daily is already clustered on load date. Filters on other columns, out-of-order loads and DML make partition ranges overlap.

**Clustering depth** is the average number of partitions whose ranges overlap for a column. Near 1 is good; high means filters scan many partitions. Check it with `SYSTEM$CLUSTERING_INFORMATION`.

### Clustering keys and Automatic Clustering

A **clustering key** names the columns or expressions whose rows should sit in the same partitions. **Automatic Clustering** then rewrites overlapping partitions in the background on serverless compute, so you supply no warehouse. You pay credits for the rewrites, plus storage for replaced partitions held by Time Travel and Fail-safe.

- **Worth it when** the table is large (the docs say typically multi-TB), most queries filter selectively on the same few columns, and reads far outnumber changes.
- **Not worth it when** the table is small, already naturally clustered, or high-churn.
- **Avoid very high-cardinality keys** (raw timestamps, UUIDs): upkeep is costly. Cluster on an expression like `TO_DATE(ts)` instead. Very low-cardinality keys, such as booleans, give little pruning.
- **Limits:** at most 3–4 columns, ordered from low to high cardinality. Pilot on one or two tables and track `AUTOMATIC_CLUSTERING_HISTORY`.

The docs also describe an **Optima Clustering** billing model that charges by data volume ingested instead of compute hours **(⚠ verify status and rate)**.

### Other ways to skip work

- **Search Optimization Service (SOS)** (Enterprise) builds a serverless-maintained *search access path*, a structure that works like an index. It serves equality and `IN` lookups that return few rows, substring and regex matches, and semi-structured fields. It helps columns that clustering can't serve, since a table has only one sort order. Cost grows with the enabled columns, their distinct values, and churn, so enable it per column. **Snowflake Optima** (on Gen2 and Adaptive warehouses) builds such indexes automatically for repeated point lookups at no extra cost.
- **Query Acceleration Service (QAS)** (Enterprise) offloads eligible fragments of a query to serverless compute: large scans with selective filters or aggregations, and big `INSERT`/`COPY`. An outlier query no longer forces you to upsize the whole warehouse. A *scale factor* caps the extra compute (default 8). Test eligibility with `SYSTEM$ESTIMATE_QUERY_ACCELERATION`. A pending behavior change would turn QAS on by default for new standard warehouses **(⚠ verify)**.
- **Materialized views** (Enterprise) are precomputed aggregates that stay up to date automatically ([§06](06-snowflake-features.md)).

### Virtual warehouses

- **Sizes.** Sizes run from XS to 6XL, and credits per hour double at each step. For Gen1 standard warehouses: XS = 1, S = 2, M = 4 … 4XL = 128. Remember "XS = 1, doubling" and work out the rest.
- **Billing.** Per second, with a **60-second minimum every time a warehouse starts or resumes**. Thrashing between suspend and resume pays that minimum again each time.
- **Auto-suspend and auto-resume.** A warehouse suspends after `AUTO_SUSPEND` idle seconds (default 600) and resumes within seconds when the next query arrives. While suspended it costs nothing, but its cache is dropped.
- **Scale up** (bigger size) helps single queries that are parallelizable or spilling. If a query scales linearly, 2× the size at half the runtime costs the same credits. It doesn't help queuing, skew, or tiny queries.
- **Scale out** (multi-cluster, Enterprise) adds same-size clusters when queries queue. It fixes concurrency, not the speed of a single query.
  - Equal `MIN_CLUSTER_COUNT` and `MAX_CLUSTER_COUNT` is maximized mode; different values give auto-scale mode.
  - The maximum cluster count depends on size: up to 300 for XS–M, falling to 10 for 4XL and above.
  - In auto-scale mode, the **Standard** policy (the default) starts clusters as soon as queries queue. **Economy** starts one only when it predicts enough work to keep it busy (about 6 minutes **(⚠ verify)**), trading some queuing for credits.
  - Per-cluster concurrency is capped by `MAX_CONCURRENCY_LEVEL` (default 8 **(⚠ verify)**).

Newer compute options:

- **Gen2 standard (GA, May 2025):** newer hardware and engine. It costs more credits per hour but is faster on most work. Set `GENERATION = '2'`. The 2026_03 behavior change makes it the default where available **(⚠ verify rollout)**. No 5XL or 6XL. The rate is about 1.35× Gen1 on AWS and 1.25× on Azure according to third-party sources **(⚠ verify against the Service Consumption Table)**.
- **Snowpark-optimized (GA):** 16× the memory per node by default (`RESOURCE_CONSTRAINT = MEMORY_16X`). For memory-heavy Python, UDFs and ML. The 1 TB tiers are **(Preview)**.
- **Adaptive Compute (GA June 2026, Enterprise+):** replaces the fixed size with workload-aware compute billed per query. You tune it with `MAX_QUERY_PERFORMANCE_LEVEL` and `QUERY_THROUGHPUT_MULTIPLIER`. The release note says select AWS regions **(⚠ verify current regions)**.
- **Interactive warehouses (GA Dec 2025):** sub-second, high-concurrency serving for dashboards and APIs. The minimum billing period is 1 hour and the minimum auto-suspend is 24 hours, so they are always-on serving, not batch compute.

### Caching: three layers

1. **Result cache (cloud services).**
   - **Retention:** 24 hours, reset on each reuse, up to 31 days after the first run.
   - **Reuse requires** identical query text, unchanged data, no non-reusable functions (`RANDOM`, `UUID_STRING`), no external functions or hybrid tables, and a role with the right privileges. No warehouse is needed.
   - Use `USE_CACHED_RESULT = FALSE` when benchmarking.
2. **Warehouse cache (local SSD).** Holds micro-partition data the warehouse has already read. It is **dropped on suspend** and when the warehouse is resized down. That is why auto-suspend is a tradeoff: the docs suggest about 10 minutes for BI warehouses and about 5 for ETL or dev.
3. **Metadata.** Per-partition counts and min/max let `COUNT(*)`, `MIN` and `MAX` return without scanning data **(⚠ verify which types and predicates qualify)**.

### Credit model and editions

- **Warehouse compute:** size rate × seconds running. Usually most of the bill.
- **Serverless features:** Snowpipe, Automatic Clustering, SOS, MV refresh, serverless tasks and QAS. They never sit idle, but their cost can grow without anyone noticing.
- **Cloud services:** billed only for the part above **10% of that day's warehouse compute** (computed per UTC day, serverless excluded). Millions of tiny queries or metadata-heavy work can push you over the threshold.
- **Storage:** per compressed TB per month, averaged daily, including Time Travel and Fail-safe bytes (7 days for permanent tables). Order of magnitude: tens of dollars per TB-month **(⚠ verify for region and contract)**.
- **Data transfer:** ingress is free. Egress across regions or clouds, or to the internet, is charged.

| Edition | Adds |
|---|---|
| Standard | Core engine, Time Travel up to 1 day, Fail-safe |
| Enterprise | Multi-cluster, Time Travel up to 90 days, SOS, QAS, MVs, row/column policies, tagging, Adaptive Compute |
| Business Critical | Tri-Secret Secure, private connectivity, support for regulated data (HIPAA, PCI DSS) |
| Virtual Private Snowflake | Business Critical in an isolated environment with a dedicated metadata store |

### Reading performance

The Query Profile answers four questions. Shrink bytes scanned first (P4), then decide whether more compute is needed.

1. **Pruning.** Compare partitions scanned with partitions total. If a selective filter scans most partitions, add clustering or SOS, or fix the predicate: functions on the column and type mismatches defeat pruning.
2. **Spilling.** Spill to *local* storage means memory ran out and the warehouse used its SSD. Spill to *remote* storage means the SSD ran out too, which is far slower. Scale up or process smaller batches. With QAS on, a little remote spill is normal.
3. **Queuing.** Queued time (`QUEUED_OVERLOAD_TIME`) means concurrency. Scale out or isolate; don't upsize.
4. **Operators.** Look for exploding joins, skew and huge sorts.

### Failure modes

- **Oversized warehouses with long auto-suspend** burn credits while idle: an XL costs 16 credits per hour.
- **Auto-suspend too aggressive** means a cold cache on every visit plus repeated 60-second minimums.
- **Clustering every table "just in case"** wastes serverless credits.
- **Ad-hoc users on the ETL warehouse** cause queuing and cost that can't be attributed.
- **Runaway queries** run until the default statement timeout of 2 days **(⚠ verify)** unless you set `STATEMENT_TIMEOUT_IN_SECONDS`.
- **Guardrails:** resource monitors and budgets ([§13](13-performance-cost.md)).

## Comparison

| Symptom | Lever | Mechanism | Billed as | Don't use when |
|---|---|---|---|---|
| Selective range filter scans most partitions | Clustering key | Re-sorts partitions so min/max ranges stop overlapping | Serverless credits + storage churn | Table is small, churns heavily, or is already clustered |
| Lookup or substring search on a high-cardinality column | Search Optimization | Search access path finds the partitions holding a value | Serverless build/upkeep + storage | Queries return many rows |
| Rare huge scan on a right-sized warehouse | QAS | Borrows serverless compute for scan fragments | Serverless credits, capped | The query isn't eligible |
| Remote spill | Scale up | More memory and SSD per query | 2× credits per size step | The problem is queuing or skew |
| Queries queuing | Multi-cluster | More same-size clusters | Credits per running cluster | Single queries are slow |
| Same aggregate all day | MV or result cache | Precomputed or reused result | Refresh credits, or free | Base data changes constantly |

| | Snowflake standard table | Delta table |
|---|---|---|
| Data files | Proprietary micro-partitions, Snowflake-managed storage | Parquet in your bucket |
| Transaction metadata | Cloud services metadata store | `_delta_log` next to the data |
| File stats | Every column, automatic | First N columns by default |
| Layout upkeep | Automatic sizing; optional serverless clustering | `OPTIMIZE`, liquid clustering, predictive optimization |
| Readers | Snowflake engine only | Any Delta-capable engine |
| Old versions | Time Travel + 7-day Fail-safe, billed | Kept until `VACUUM` |

## Interview questions

**Q: Explain Snowflake's architecture and why it matters.**

It has three layers: immutable columnar micro-partitions in object storage, any number of isolated virtual warehouses, and an always-on cloud services layer that handles metadata, optimization, transactions and security. Because storage and compute are separate, they scale and bill independently (P2). You isolate a workload by giving it a warehouse instead of a copy of the data, and resizing takes seconds because nothing is redistributed. The tradeoff is a proprietary, managed format: you get almost no ops work but lose physical control and openness (P10, P11). Iceberg tables are the hedge.

**Q: What is the cloud services layer actually doing?**

It authenticates the session, checks privileges, parses and optimizes the query, prunes partitions from metadata, checks the result cache, and queues and dispatches work to a warehouse. It also commits DML by atomically publishing a new metadata version. That is why cached results and clones need no running warehouse. You pay for it only above 10% of daily warehouse compute. Millions of trivial queries or metadata-heavy jobs can cross that line, and you can't scale this layer yourself.

**Q: How does Snowflake make a query fast without indexes?**

Mostly by not reading data (P4). Per-partition min/max lets it prune at compile time, columnar storage reads only the referenced columns, and the result and warehouse caches skip repeated work. Vectorized parallel execution handles whatever is left. When filters don't line up with load order you add a clustering key, and for needle lookups you add SOS. The metadata is the index.

**Q: How is Snowflake's storage different from a Delta table?**

Both are immutable columnar files plus a metadata layer, which is how they get ACID, time travel and clones (P3). Delta is Parquet in your bucket with an open log that any Delta engine can read, and you maintain the layout yourself. Snowflake's micro-partitions are proprietary, keep stats on every column, and are sized and compacted for you, but only Snowflake's engine reads them directly. You trade openness for operability (P11). Snowflake-managed Iceberg tables sit in between.

**Q: Scale up vs scale out?**

Scale up when single queries are slow from big scans, joins or spilling: a parallelizable query at 2× size runs in about half the time for the same credits. Scale out (multi-cluster) when queries queue because of concurrency; no individual query gets faster. The profile tells you which you need: remote spill means scale up, queued time means scale out. The wrong choice spends money without fixing the problem.

**Q: When would you define a clustering key and when not?**

Define one on a multi-TB table that is filtered repeatedly and selectively on columns its load order doesn't sort, when clustering depth confirms poor pruning and reads far outnumber writes. Skip it for small, high-churn or already naturally clustered tables. Don't cluster on raw high-cardinality columns; use `TO_DATE(ts)`. Measure the clustering credits against the drop in partitions scanned. A table has only one sort order, so serve other access patterns with SOS or an MV.

**Q: Clustering, Search Optimization, QAS, or a materialized view: how do you choose?**

Each fixes a different problem. Clustering serves range filters on a few columns of a big table. SOS serves few-row lookups, substrings and VARIANT paths. QAS absorbs occasional outlier scans. An MV serves one aggregation that runs over and over. All four run in the background on credits, so each has to earn its keep on measured queries. "Enable everything" is the anti-pattern.

**Q: What happens when a warehouse is suspended?**

The compute is released and billing stops, once that run's 60-second minimum has been paid. The SSD cache is dropped. Data, metadata and the result cache are untouched, so cached results still come back. The next query resumes the warehouse in seconds, but on a cold cache, so the first queries run slower. Set `AUTO_SUSPEND` to match how far apart queries arrive: too short wastes the cache and repeats the minimums, too long pays for idle time.

**Q: How would you control Snowflake costs?**

Measure first: attribute spend with `WAREHOUSE_METERING_HISTORY`, `QUERY_HISTORY` and the serverless history views. Then fix the structure:

- one warehouse per workload;
- the smallest size that meets the SLA without remote spill;
- auto-suspend tuned to how queries arrive;
- caps on multi-cluster maximums, with the Economy policy where latency is tolerant.

Reduce the work itself with pruning-friendly layouts and incremental builds. Add guardrails: resource monitors, budgets and statement timeouts. Remember P7: a task that runs every minute keeps a warehouse awake all day.

**Q: A dashboard query went from 2 seconds to 40. How do you diagnose it?**

Split the time into queued, compiling and executing. Queued means add clusters or isolate the workload. Executing means check pruning, spill and the most expensive operator. Also check whether new frequent loads now invalidate the result cache, or a shorter auto-suspend leaves the cache cold. Fix the cause before upsizing.

**Q: When does the result cache not help?**

Any change to an underlying table invalidates it, so tables loaded every few minutes rarely get hits. The text has to match exactly, so tools that inject varying literals miss. Non-reusable functions, external functions and hybrid tables are never cached. For dashboards on fresh data, rely on the warehouse cache, MVs or interactive tables instead.

**Q: How would you lay out warehouses for ETL, BI and data science?**

Separate them; isolation is cheap because they all share the data.

- **ETL:** sized for throughput, single cluster, short auto-suspend.
- **BI:** multi-cluster with the Standard policy and a longer auto-suspend to keep the cache warm. Use interactive warehouses for sub-second serving.
- **Data science:** Snowpark-optimized.
- **Ad-hoc:** small, with a statement timeout.

Put a resource monitor on each one. Don't split so finely that dozens of warehouses each pay resume minimums on cold caches.

**Q: Gen2 and Adaptive Compute are new. How do they change sizing decisions?**

Gen2 charges a higher hourly rate for faster execution, so cost per job usually drops if runtime falls more than the rate rises. Benchmark it, especially on DML-heavy pipelines. Adaptive Compute removes the size decision and bills per query, which suits mixed, unpredictable workloads. In exchange, spend per hour is less predictable, the feature is newer, and it needs Enterprise in limited regions. Keep fixed sizes where budgets must be predictable.

**Q: What does micro-partition immutability cost you?**

Write amplification. Updating one row rewrites its whole partition, so MERGE-heavy CDC into a huge table rewrites many partitions. That costs compute, and the old versions stay billed through Time Travel and Fail-safe. To mitigate it, batch changes, cluster on the merge key, use transient tables for staging, and shorten retention on high-churn tables. For true row-level OLTP, use hybrid tables ([§06](06-snowflake-features.md)) or an OLTP database (P1).

## Takeaways
- Snowflake is storage, isolated elastic warehouses, and a cloud services brain; the separation is the point (P2).
- Micro-partitions (50–500 MB uncompressed) are immutable and columnar with per-column stats; pruning, Time Travel and clones follow (P3, P4).
- The metadata is the index. Clustering fixes range pruning, SOS fixes needle lookups, QAS absorbs outliers.
- Credits double per size; billing is per second with a 60-second minimum per resume.
- Spill → scale up; queuing → scale out.
- Suspend drops the warehouse cache. The result cache (24 h, up to 31 days) survives.
- Cloud services is billed only above 10% of daily warehouse compute.
- Most cost incidents are structural (shared, oversized, idle warehouses, or forgotten serverless features), not query-level.

## Sources
- https://docs.snowflake.com/en/user-guide/intro-key-concepts
- https://docs.snowflake.com/en/user-guide/tables-clustering-micropartitions
- https://docs.snowflake.com/en/user-guide/tables-clustering-keys
- https://docs.snowflake.com/en/user-guide/tables-auto-reclustering
- https://docs.snowflake.com/en/user-guide/search-optimization-service
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
