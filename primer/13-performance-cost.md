# 13. Performance and Cost (FinOps)

> As of October 2026. Tags: **(GA)** generally available · **(Preview)** public/private preview · **(⚠ verify)** not confirmed against official docs.

## Why this exists

With separated storage and usage-priced compute (P2), you pay for every second compute runs and every byte stored, so performance and cost are the same problem. A query that reads less finishes sooner and bills less. Spend is also decentralized: anyone with a role and a warehouse can spend. FinOps makes that spend visible, attributable and bounded without slowing teams down.

## Core concepts

### Performance levers: skip work first (P4)

Query cost is roughly bytes read × work per byte, plus idle time. Reduce bytes read before you buy faster hardware.

- **Pruning.**
  - Snowflake prunes micro-partitions using per-column min/max metadata. When ingestion order doesn't match the filters people use, a clustering key plus Automatic Clustering (serverless, billed) restores pruning.
  - Databricks has Hive-style partitions (coarse; only for low-cardinality columns), Z-order (legacy) and **liquid clustering** (incremental, and keys can be changed). Predictive optimization runs OPTIMIZE, VACUUM and ANALYZE on UC managed tables. `CLUSTER BY AUTO` picks the keys on UC managed tables **(GA)**.
  - Details: [§05](05-snowflake-architecture.md), [§04](04-table-formats-catalogs.md).
- **Data-skipping stats.** Delta collects min/max/null counts for the first 32 columns by default, so put filter columns early or set `delta.dataSkippingStatsColumns`. On UC managed tables, predictive optimization instead collects stats on the most-filtered columns, with no 32-column limit. Snowflake keeps metadata for every column automatically.
- **Small-file problem.** Each file costs a metadata entry, a list and open call, footer parsing, and poor compression, because a tiny file holds few values per column chunk. Streaming micro-batches, over-partitioning and frequent small appends cause it.
  - Databricks fixes: `OPTIMIZE` (bin-packing), optimized writes and auto-compaction, `delta.targetFileSize`, predictive optimization, coarser partitions, and less frequent triggers.
  - Snowflake sizes micro-partitions itself. The analogue is loading: aim for files of roughly 100–250 MB compressed for COPY and Snowpipe.
- **Joins and skew.** Broadcast the small side and shuffle when both sides are big. Skew means one key holds most of the rows, so one task straggles while the rest wait. Databricks AQE splits skewed partitions, coalesces small ones and switches to broadcast at runtime. Salting is the manual fix. In Snowflake, find skew in the query profile and fix it at the data (pre-aggregate, or isolate hot keys).
- **Spill.** When hash tables or sorts exceed memory they spill, first to local disk and then to remote storage, with each tier much slower. Snowflake's profile shows bytes spilled to local and to remote storage. Fix the cause first: filter or project earlier, or fix an exploding join. Then size up: a bigger warehouse or memory-optimized nodes. Scaling up is justified when it eliminates spill.
- **Caching.**
  - Snowflake: result cache (exact query, unchanged data, no compute), warehouse local disk cache (lost on suspend), metadata cache (COUNT/MIN/MAX without scanning).
  - Databricks: disk cache (local SSD copies of Parquet), SQL warehouse query result cache, Delta log stats.
  - Non-deterministic functions such as `CURRENT_TIMESTAMP()` in dashboard SQL defeat result caching. Details: [§05](05-snowflake-architecture.md).
- **Scale up vs scale out.** Size up for one big or spilling query. Scale out (multi-cluster warehouses, DBSQL warehouse scaling) for concurrency and queuing.

### Cost model contrast

| Dimension | Snowflake | Databricks |
|---|---|---|
| Compute unit | Credits. Warehouse size doubles credits/hour per step. Per-second billing, 60 s minimum per resume. | DBUs × $/DBU, priced by SKU (jobs, all-purpose, SQL, serverless) |
| Infrastructure | Included in the credit price | **Classic:** cloud VMs, disks and network billed separately in your cloud account. **Serverless:** bundled into the DBU rate. |
| Serverless | Snowpipe, Automatic Clustering, MV maintenance, search optimization, serverless tasks, QAS: each metered separately | Serverless SQL warehouses, jobs, pipelines, notebooks |
| Storage | Per TB-month compressed, including Time Travel and Fail-safe bytes | Your object storage bill, including old Delta versions until `VACUUM` |
| Hidden lines | Cloud services billed only above 10% of daily compute; replication and transfer | NAT, egress, idle classic clusters |
| Telemetry | `ACCOUNT_USAGE` (`METERING_HISTORY`, `WAREHOUSE_METERING_HISTORY`, `QUERY_ATTRIBUTION_HISTORY`), `ORGANIZATION_USAGE` | `system.billing.usage`, `system.billing.list_prices`, `system.compute.*`, `system.query.history` |

A common misconception is that DBU spend is the whole Databricks bill. On classic compute, VM cost appears only in the cloud bill, so it must be joined on propagated cluster tags.

### FinOps practice

**Attribution.**
- **Snowflake.**
  - A warehouse per team or workload is the simplest, most reliable attribution.
  - On shared warehouses, use object tags on warehouses and users plus `QUERY_TAG`.
  - `QUERY_ATTRIBUTION_HISTORY` splits warehouse cost by query, but idle time is excluded and must be allocated separately.
- **Databricks.**
  - Custom tags on clusters, warehouses and jobs flow to `system.billing.usage.custom_tags` and to the cloud VMs.
  - Compute policies make tags mandatory.
  - Serverless usage policies (formerly budget policies) tag serverless usage **(Preview)**.

**Showback vs chargeback.** Showback reports cost per team. It's cheap, drives behavior, and is where to start. Chargeback bills cost centers. It needs high tagging coverage and an agreed rule for shared costs (platform overhead, idle time, shared storage), or it turns into disputes. Move to chargeback once untagged spend is small and stable.

**Guardrails.**

| Guardrail | Snowflake | Databricks |
|---|---|---|
| Hard stop | Resource monitors: notify, suspend, or suspend immediately. **Warehouses only**; they can't cover serverless features. | Compute policies cap node types, size and DBU/h, and enforce auto-termination |
| Forecast and alert | Budgets: account and custom, covering warehouses **and** serverless. They alert; they don't stop. | Budgets **(GA)** alert on spend; they don't stop compute |
| Idle | `AUTO_SUSPEND` (≈60 s for ad hoc warehouses) | Auto-termination; SQL warehouse auto-stop |
| Runaway queries | `STATEMENT_TIMEOUT_IN_SECONDS`, `STATEMENT_QUEUED_TIMEOUT_IN_SECONDS`, `MAX_CLUSTER_COUNT` | Query and job timeouts, warehouse max clusters |

**Unit economics.** Track cost per pipeline run, per dashboard refresh, per GB ingested and per active consumer. Watch the trend. A rising unit cost with flat usage is a regression worth a ticket.

### Anti-patterns

- **One giant shared warehouse:** no attribution, noisy neighbors, and sized for the peak around the clock.
- **Always-on clusters,** or auto-suspend disabled "for the cache".
- **`SELECT *` on wide columnar tables,** which throws away column pruning.
- **Full-table recomputes** instead of incremental MERGE, dynamic tables or streaming tables (P6, P7).
- **Over-clustering:** clustering small or high-churn tables, or too many keys. Reclustering then costs more than the pruning saves.
- **Unbounded streaming for data read daily.** Use scheduled or `availableNow` runs instead (P7).
- **Bigger warehouses for concurrency problems.** Scale out instead.

## Comparison

| Lever | Snowflake | Databricks |
|---|---|---|
| Layout | Clustering keys + Automatic Clustering | Liquid clustering, predictive optimization |
| File/partition sizing | Managed by the platform | `OPTIMIZE`, auto-compaction, target file size |
| Concurrency | Multi-cluster warehouses | Warehouse autoscaling / serverless |
| Attribution | Warehouse-per-team, object tags, `QUERY_TAG` | Custom tags, compute and serverless usage policies |
| Spend telemetry | `ACCOUNT_USAGE`, `ORGANIZATION_USAGE` | `system.billing.*` + cloud bill (classic) |

## Interview questions

**Q: How do you make a slow query fast in Snowflake vs Databricks?**

Start with the profile, not the warehouse size. In Snowflake, check partitions scanned vs total (pruning), bytes spilled (memory), and exploding joins. In Databricks, check files pruned, shuffle size, skewed tasks and spill in the Spark UI or query profile. Fix pruning first: filter on clustered columns, cluster or liquid-cluster the table, and compact small files. Then fix the plan (join order, broadcast, skew), and only then size up to remove spill. Scaling first hides a layout problem you'll keep paying for.

**Q: What's the small-file problem and how do you fix it?**

Every file carries fixed overhead: a metadata entry, listing and open calls, footer parsing and poor compression. Thousands of tiny files make planning and scanning slower than the data volume justifies. Streaming micro-batches and over-partitioning are the usual causes. In Databricks, fix it with `OPTIMIZE`, optimized writes and auto-compaction, a target file size, predictive optimization, and fewer or coarser partitions. In Snowflake the engine sizes micro-partitions, so the concern shifts to staging well-sized files for loading.

**Q: How would you implement chargeback?**

Make attribution structural rather than voluntary. Snowflake: warehouses per team, tags, and `QUERY_TAG` on shared warehouses. Databricks: mandatory tags through compute policies, and serverless usage policies for serverless. Then publish an allocation rule for shared costs such as idle time, platform services and shared storage. Run showback for a few cycles until untagged spend is small and teams trust the numbers, then switch to chargeback. Chargeback on poor tagging produces arguments, not savings.

**Q: Costs doubled last month. How do you investigate?**

Decompose top-down. Split by service (compute, serverless, storage, transfer) using `METERING_DAILY_HISTORY` or `system.billing.usage` joined to list prices, then by warehouse or cluster, then by team tag, then by query or job. Look for step changes: a new always-on job, a disabled auto-suspend, a full refresh replacing an incremental one, a clustering key causing reclustering churn, or a retry loop. Check unit cost too. If the cost per pipeline run is flat, growth is volume, not waste.

**Q: Serverless vs provisioned compute: what are the cost tradeoffs?**

Serverless removes idle and startup cost and the work of capacity planning. You pay a higher unit rate in exchange for paying only while work runs. It wins for spiky, intermittent or small workloads. Provisioned compute on commitments can be cheaper for steady, high-utilization workloads and gives more control over instance types and networking (P10). Decide on measured utilization, not list price.

**Q: How do you prevent runaway spend?**

Layer the controls. Use hard stops where they exist: Snowflake resource monitors suspend warehouses, and Databricks compute policies cap size and enforce auto-termination. Add budgets that alert on forecasts, including serverless, which resource monitors don't cover. Statement and job timeouts kill runaway queries. Restrict who can create or resize compute. Budgets alone don't stop anything, so a budget without a hard stop is just a notification.

**Q: When do you scale up vs scale out?**

Scale up when single queries are slow because of spill or a large scan. For parallelizable work, doubling the size roughly halves runtime at about the same cost. Scale out when queries are queuing, which is a concurrency problem a bigger node won't fix. Sizing up for concurrency just pays for idle cores.

**Q: When is clustering worth paying for?**

Clustering pays off when a table is large, queries filter consistently on a few columns, and the natural load order doesn't match those filters. It does not pay off on small tables, on tables with no stable filter pattern, or on high-churn tables where reclustering never settles. Measure partitions or files scanned before and after against the clustering spend. Snowflake bills Automatic Clustering as a serverless line, and Databricks bills OPTIMIZE and predictive optimization as compute.

## Takeaways

- With usage pricing, every byte you skip is money saved (P2, P4).
- Profile first: pruning, then plan, then size.
- Small files are a metadata tax; compact and size your writes.
- Scale up for spill, scale out for queuing.
- DBUs aren't the whole Databricks bill on classic compute.
- Snowflake resource monitors stop warehouses only; budgets alert on everything.
- Showback first; chargeback only once tagging is trustworthy.
- Incremental beats full recompute; schedule what nobody reads in real time (P6, P7).

## Sources

- https://docs.snowflake.com/en/user-guide/cost-overview
- https://docs.snowflake.com/en/user-guide/cost-controlling
- https://docs.snowflake.com/en/user-guide/resource-monitors
- https://docs.databricks.com/gcp/admin/usage/budget-policies
- https://docs.databricks.com/aws/en/release-notes/product/
- https://www.databricks.com/blog/attribute-serverless-costs-departments-and-users-budget-policies
- https://docs.databricks.com/aws/en/admin/usage/budget-policies
- https://docs.databricks.com/aws/en/admin/account-settings/budgets
- https://docs.databricks.com/aws/en/admin/system-tables/
- https://docs.databricks.com/aws/en/delta/clustering
- https://docs.databricks.com/aws/en/release-notes/product/2025/june
- https://docs.databricks.com/aws/en/delta/data-skipping
- https://docs.snowflake.com/en/user-guide/data-load-considerations-prepare
- https://docs.snowflake.com/en/sql-reference/account-usage/query_attribution_history
- https://docs.snowflake.com/en/user-guide/budgets
