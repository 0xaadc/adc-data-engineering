# Cheat Sheet

> As of October 2026. This is the last-15-minutes page. Each line links back to the section that explains *why*. Numbers marked ~ are orders of magnitude: know the shape, not the decimal.

## 1. The eleven principles ([§01](01-first-principles.md))

| | Principle | Say it as |
|---|---|---|
| P1 | Write- vs. read-optimized | "OLTP rows and OLAP columns are opposite layouts, so data has to move." |
| P2 | Separate storage/compute | "One copy in object storage, many isolated elastic compute clusters." |
| P3 | Immutable files + metadata log | "Never update in place; swap metadata atomically. That's why time travel and clones are free." |
| P4 | Skip work | "The fastest byte is the one you never read: pruning, clustering, caching." |
| P5 | The log is truth | "Keep the ordered change history and you can rebuild or replay anything." |
| P6 | Idempotent steps | "Retries are inevitable, so running twice must equal running once." |
| P7 | Latency is a cost dial | "Buy only the freshness the decision needs." |
| P8 | Schema is a contract | "Most incidents are unannounced upstream change." |
| P9 | Govern the data | "Policies in the catalog, not in each tool." |
| P10 | Control vs. operability | "Managed removes knobs and toil; choose by team and workload." |
| P11 | Open formats | "Open data, but the catalog is the new lock-in point." |

## 2. How to answer

- **Pattern:** mechanism → consequence → tradeoff → when not.
- **Why X over Y:** name the tension → state assumptions → argue X → steelman Y → decide and name the reversal cost ([§15](15-why-x-over-y.md)).
- **Without hands-on experience:** "I haven't run *X* in production. Mechanically it works like *Y* (principle), so I'd expect *Z*. In Databricks the equivalent is *W*, and the difference is…" Honest, and it shows depth.
- **Design questions:** requirements (consumers, freshness, volume, correctness, compliance, team) → sources → ingest → store → transform → serve → govern/observe → tradeoffs → failure modes ([§16](16-system-design.md)).

## 3. Databricks → Snowflake map ([§05](05-snowflake-architecture.md), [§06](06-snowflake-features.md))

| Databricks | Snowflake |
|---|---|
| Account / workspace + metastore | Organization / account |
| UC `catalog.schema.table` | `database.schema.object`, governed by Horizon |
| SQL warehouse / cluster | Virtual warehouse (T-shirt sizes, multi-cluster) |
| DBU (+ VM bill on classic) | Credit (infrastructure included) |
| Delta/Parquet in your bucket + `_delta_log` | Micro-partitions in Snowflake storage + cloud services metadata (Iceberg is the open option) |
| Liquid clustering / Z-order | Clustering key + Automatic Clustering |
| `OPTIMIZE`, predictive optimization | Nothing to run; file sizing is automatic |
| Bloom filters | Search Optimization Service |
| Disk cache / query result cache | Warehouse SSD cache (lost on suspend) / result cache (survives suspend) |
| Delta time travel / shallow clone | Time Travel / zero-copy clone |
| Change Data Feed | Streams |
| Lakeflow Jobs | Tasks (task graphs) |
| Spark Declarative Pipelines MVs / streaming tables | Dynamic Tables (target lag) |
| Auto Loader | Snowpipe (files) / Snowpipe Streaming (rows) |
| Delta Sharing (open protocol) | Secure Data Sharing (zero-copy in-region, closed) / listings / Marketplace |
| PySpark DataFrame | Snowpark (DataFrame compiled to SQL pushdown) |
| Mosaic AI / Agent Bricks / AI Functions / AI Search (formerly Vector Search) / Genie | Cortex AI functions / Cortex Search / Cortex Analyst / Snowflake Intelligence |
| Lakebase (Postgres) | Hybrid tables / Snowflake Postgres |
| Lakeflow Connect | Openflow (managed NiFi) |

## 4. Snowflake numbers worth knowing ([§05](05-snowflake-architecture.md), [§06](06-snowflake-features.md))

- Micro-partition: ~50–500 MB uncompressed, immutable, columnar, with per-column min/max stats. **The metadata is the index.**
- Warehouse credits **double per size** (XS = 1 credit/hour). Billed **per second with a 60-second minimum on every resume**.
- **Spill → scale up. Queuing → scale out** (multi-cluster).
- Result cache lasts 24 h (resetting on reuse, up to 31 days) and survives suspend. The warehouse SSD cache is lost on suspend or resize.
- Cloud services is billed only above **10% of daily warehouse compute**.
- Time Travel: **1 day default, up to 90 days** (Enterprise+, permanent tables). Fail-safe: **7 days**, recoverable only through Snowflake support. Transient and temporary tables skip Fail-safe.
- Load dedup metadata: COPY **64 days**, Snowpipe **14 days**.
- Dynamic Tables minimum target lag is **~60 s**, and it's a staleness target, not a schedule.
- Snowpipe Streaming / Kafka connector v4: **~5–10 s** to queryable ([§11](11-stream-processing.md)).
- Resource monitors cap **warehouses only**. Budgets cover serverless spend too, but only alert.

## 5. Kafka in ten lines ([§10](10-kafka.md))

1. A replicated, partitioned, **append-only log**. Consumers track their own offsets, so fan-out and replay are cheap (P5).
2. **Ordering holds only within a partition.** The key picks the partition, so changing the partition count remaps keys.
3. Partition count is the **ceiling on consumer-group parallelism**: at most one consumer per partition per group.
4. Consumer lag is the health metric. Rebalances pause consumption (mitigated by cooperative rebalancing and the KIP-848 protocol).
5. Durability: **RF=3, `min.insync.replicas=2`, `acks=all`** survives one broker loss with no acknowledged-data loss. A second loss stops writes but loses nothing.
6. Retention topics are **histories**. Compacted topics keep the latest value per key, so they behave like **tables** (tombstones delete).
7. Idempotent producer and `acks=all` are default since 3.0. Transactions + `read_committed` give **exactly-once Kafka→Kafka only**. Anything else needs an idempotent sink (P6).
8. Schema Registry: **BACKWARD → upgrade consumers first. FORWARD → producers first. FULL → either order** (P8).
9. Connect moves data in and out. **Debezium** reads the database log (WAL/binlog). The **outbox pattern** avoids dual writes.
10. **4.0 is KRaft-only** (no ZooKeeper). Tiered storage is GA, diskless storage is coming, and topic→Iceberg/Delta materialization (e.g., Tableflow) is blurring streams and tables.

**Kafka vs. a managed bus:** Kafka for ecosystem, portability and many consumers. Event Hubs (Kafka endpoint), Kinesis or Pub/Sub for low ops on one cloud at modest scale. **Don't self-manage without a team** (P10).

## 6. Streaming in eight lines ([§11](11-stream-processing.md))

1. Batch is bounded streaming. The hard parts are **time, state and failure**.
2. Use **event time** for business logic and processing time for pipeline health only.
3. A **watermark** is a guess about completeness: tight means low latency and dropped late data, loose means more state (P7).
4. Every stateful operator needs an **expiry story**: a watermark, a time-bounded join, or a TTL.
5. **Exactly-once = replayable source + deterministic checkpointed processing + idempotent or transactional sink.** Side effects need idempotency keys.
6. **Spark SS** for lakehouse-native pipelines (micro-batch; real-time mode is Preview). **Flink** for ms-latency, event-time-heavy state. **Kafka Streams** for Kafka-native services (a library, not a cluster).
7. Databricks: use `availableNow` for "streaming code at batch cost". `Trigger.Once` is deprecated and continuous mode isn't supported, so use real-time mode instead.
8. **Push back on "real-time"**: ask what decision it feeds and how fast that decision is made, then price each latency tier.

## 7. Snowflake vs. Databricks in six lines ([§08](08-snowflake-vs-databricks.md))

1. Snowflake bet on **managed SQL over vendor-owned storage**. Databricks bet on **code over open files**. The defaults still follow those bets.
2. The core difference: **where table metadata lives**, in a vendor service layer or in a log beside the data.
3. Choose the platform whose **default path is your main workload** and whose operating model fits your team.
4. Running both: **one writer and one catalog of record per table**, contracts at the boundary, identity from one IdP.
5. Unit price is not job price. **TCO includes people.**
6. Iceberg makes "one copy, two engines" real. It fails through catalog ambiguity, feature lag and unclear maintenance ownership.

## 7b. Recent renames and 2025–26 changes (stale prep material gets these wrong)

| Old / commonly assumed | Current (Oct 2026) | Where |
|---|---|---|
| Delta Live Tables / "Lakeflow Declarative Pipelines" | **Lakeflow Spark Declarative Pipelines**; core open-sourced in Apache Spark; `APPLY CHANGES` → **AUTO CDC** | [§07](07-databricks-gap-fill.md) |
| Databricks Workflows | **Lakeflow Jobs** | [§07](07-databricks-gap-fill.md) |
| Databricks Asset Bundles | **Declarative Automation Bundles** (same `databricks bundle` CLI) | [§07](07-databricks-gap-fill.md), [§14](14-reliability-dataops.md) |
| Mosaic AI Vector Search | **AI Search** | [§18](18-ai-on-data-platform.md) |
| "Databricks has no managed DR" | Managed Disaster Recovery in gated Preview | [§14](14-reliability-dataops.md) |
| Snowflake Open Catalog for external Iceberg access | **Horizon Catalog's own Iceberg REST endpoint** (external reads and writes GA); Open Catalog closed to new customers | [§04](04-table-formats-catalogs.md), [§06](06-snowflake-features.md) |
| Apache Polaris (incubating) | Apache **top-level project** | [§04](04-table-formats-catalogs.md) |
| Snowflake can't read Delta | Reads **Delta Shares natively** (as catalog-linked databases) | [§08](08-snowflake-vs-databricks.md) |
| Snowpipe Streaming (classic) | **High-performance architecture** (PIPE object, ~5–10 s); Kafka connector v4 built on it | [§06](06-snowflake-features.md), [§11](11-stream-processing.md) |
| Multi-cluster warehouse max = 10 | Size-dependent (up to 300 for XS–M) | [§05](05-snowflake-architecture.md) |
| Warehouses only come in T-shirt sizes | **Adaptive Compute** (GA, no fixed size) and **Interactive warehouses** also exist | [§05](05-snowflake-architecture.md) |
| Kafka + ZooKeeper | **KRaft-only since 4.0**; share groups ("queues") GA | [§10](10-kafka.md) |
| Spark continuous trigger on Databricks | Not supported. Use **real-time mode** (Preview); `Trigger.Once` deprecated → `availableNow` | [§11](11-stream-processing.md) |
| dbt Labs independent | **Merged with Fivetran** | [§09](09-batch-pipelines.md) |

These came from search summaries of official release notes, so confirm them before stating dates.

## 8. Foundations in one line each

- **Lakehouse vs. warehouse (2026):** the real axis is format ownership, catalog ownership and engine plurality ([§02](02-storage-paradigms.md)).
- **Medallion:** its value is the replayable raw layer and explicit layer contracts. Collapse layers that add no contract.
- **Table format** = immutable files + atomically swapped metadata. Delta commits in the log, Iceberg in the catalog's compare-and-swap ([§04](04-table-formats-catalogs.md)).
- **COW pays on write, MOR pays on read, deletion vectors split the difference.** Compaction is mandatory either way.
- **Modeling:** declare grain first. Use Kimball for consumption and Data Vault for many audited sources. OBT is a serving layer, not the record. SCD2 = surrogate key + effective dates + atomic MERGE ([§03](03-data-modeling.md)).
- **Incremental loads:** watermark + lookback + idempotent MERGE + reconciliation. Order CDC by source position (LSN), never arrival time ([§09](09-batch-pipelines.md)).
- **Governance:** RBAC for reach, ABAC (tags + policies) for visibility. Raw storage credentials bypass every policy ([§12](12-governance-security.md)).
- **FinOps:** profile first (pruning → plan → size). Small files are a metadata tax. Show back costs before charging back ([§13](13-performance-cost.md)).
- **Reliability:** a successful job can still produce wrong data. Write-audit-publish. Ship breaking changes as expand → migrate → contract ([§14](14-reliability-dataops.md)).

## 9. Failure modes worth naming unprompted

- Shared or oversized warehouses idling. Forgotten serverless features (clustering, search optimization) quietly billing.
- Watermark-based incremental loads missing long-transaction commits and hard deletes.
- Non-idempotent reruns double-counting.
- Hot Kafka partitions from skewed keys. Retention shorter than your replay needs.
- Unbounded streaming state. Checkpoints invalidated by query changes.
- Two engines writing one table through different catalogs.
- Policies defined per tool and drifting. Over-tight governance breeding shadow copies.
