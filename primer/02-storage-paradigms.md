# 02. Storage Paradigms and Architectures

> As of October 2026. Tags: **(GA)** generally available · **(Preview)** public/private preview · **(⚠ verify)** not confirmed against official docs.

## Why this exists

Analytics needs data that lives in systems built for something else (P1). Every architecture in this file answers the same four questions: where the analytical copy lives, in what format, who governs it, and how many copies exist. Each generation fixed the previous one's worst pain and created a new one. If you know which pain drove each step, you can reason about any "X vs Y" question from first principles instead of reciting definitions.

## Core concepts

### Row vs columnar storage

```
Row store (OLTP page):   [1|Ann|NY|42.0] [2|Bo|SF|17.5] [3|Cy|NY|99.9]

Column store (OLAP):     id   [1 | 2 | 3]
                         name [Ann | Bo | Cy]
                         city [NY | SF | NY]  → dict {NY:0, SF:1} → [0|1|0]
                         amt  [42.0 | 17.5 | 99.9]   + min/max per block
```

**Row stores** (Postgres, MySQL, OLTP engines) keep a whole row together on a page and index it with B-trees. A point lookup or single-row update touches one page, the write-ahead log makes it durable, and in-place updates are cheap. Scanning one column across a billion rows, though, means reading every byte of every row.

**Column stores** keep each column contiguous. Analytics is fast for four mechanical reasons:

1. **Less I/O.** `SUM(amt) WHERE city='NY'` reads 2 of 100 columns, so roughly 2% of the bytes.
2. **Compression.** A column holds one type, often with few distinct values, so dictionary, run-length and delta encodings work well. Fewer bytes means less I/O and more data fits in memory.
3. **Vectorized execution.** Operators loop over arrays of one type in batches. That is CPU-cache-friendly and SIMD-friendly, and engines can often evaluate predicates on encoded values directly.
4. **Skipping.** Per-block min/max statistics let the engine skip blocks that can't match (P4).

**Why updates hurt columnar storage.** One logical row is spread across N column segments that are compressed in blocks, and on object storage they are immutable (P3). Changing one value means rewriting the block or file. So columnar systems batch writes, implement UPDATE and MERGE as file rewrites, or defer the cost with delete markers and deletion vectors (see [§04](04-table-formats-catalogs.md)). Parquet's internals are covered in [§04](04-table-formats-catalogs.md).

### Data warehouse (1990s onward)

The problem was that every operational system had its own copy of "customer" and "revenue," and reports disagreed. The warehouse's answer was a single integrated, curated, historical store, built schema-on-write: data is cleaned and conformed by ETL *before* it lands, typically into dimensional models ([§03](03-data-modeling.md)). It ran on MPP appliances with storage and compute bundled into the same nodes.

- **Solved:** one trusted version of the numbers, and fast SQL for BI.
- **Broke:** cost and rigidity. You scaled by buying bigger boxes, and storage could not grow without compute. Onboarding a new source took months. The model handled structured data only and gave little to ML or data science, and BI concurrency contended for the same hardware.

### Data lake (2010s)

Hadoop and later cloud object storage (S3, ADLS, GCS) made storage cheap and decoupled from any database. The bet was to land everything raw, in any format, and decide the schema when you read it (schema-on-read). Compute engines such as MapReduce and then Spark go to the data.

- **Solved:** cost, variety (logs, JSON, images), fast ingest, and direct file access for ML.
- **Broke:** everything a database used to guarantee. There was no ACID, so failed jobs left partial data. Nothing enforced a schema (P8). Governance was folder permissions. BI was slow because there were no statistics, LIST calls were expensive and small files piled up. GDPR deletes meant rewriting directories. Without curation the lake became a **data swamp**: data nobody trusted or could find.

### Two-tier: lake plus warehouse

The pragmatic fix was to keep the lake for raw data and ML, then ETL a curated subset into a warehouse for BI. The lakehouse paper (CIDR 2021) names this pattern's costs: **reliability** (two systems kept consistent by brittle ETL), **staleness** (the warehouse lags the lake), **limited advanced analytics** (ML can't efficiently use warehouse data), and **total cost of ownership** (data stored twice, compute paid twice), plus lock-in to the warehouse's format. A further cost follows from P9: two governance models that drift apart. This is the **two-copy problem**.

### Lakehouse

The CIDR 2021 paper "Lakehouse: A New Generation of Open Platforms that Unify Data Warehousing and Advanced Analytics," by Databricks authors, proposed collapsing the two tiers. Keep data in open, directly accessible formats (Parquet) on low-cost storage, and add warehouse capabilities on top:

- a **transactional metadata layer** (Delta Lake, Iceberg, Hudi; [§04](04-table-formats-catalogs.md)) for ACID, versioning and schema enforcement (P3);
- **performance features** such as statistics, caching, clustering and query optimization, so open files can be fast enough for BI (P4);
- **direct file access** through DataFrame APIs, so ML reads the same copy (P11).

The paper reports a Parquet-based system competitive with cloud warehouses on TPC-DS. The lakehouse also creates new costs. You now own table maintenance (compaction, vacuum, clustering), commit concurrency on object storage is coarser than a database's, and the **catalog** becomes the new lock-in point ([§04](04-table-formats-catalogs.md)).

### The line is blurring: what still differs

Snowflake was never an appliance. It separated storage from compute from day one (P2), storing data in proprietary columnar micro-partitions on object storage and running elastic, isolated compute clusters ([§05](05-snowflake-architecture.md)). It now reads and writes Iceberg tables too. Meanwhile, Databricks added serverless SQL warehouses, BI-oriented optimizations and a governance catalog. Both are now "managed catalog + columnar files on object storage + elastic compute." What still differs:

1. **Default format ownership.** Snowflake native tables are readable only through Snowflake, while a lakehouse defaults to open files that any engine can read.
2. **Where optimization lives.** A closed format lets the vendor change layout and metadata freely. Open-format optimizations must stay spec-compatible across engines.
3. **Engine plurality.** One engine per copy versus many engines on one copy, with the interop failure modes covered in [§04](04-table-formats-catalogs.md).
4. **Operating model (P10).** SQL-first and managed versus code-first and configurable, though this gap is narrowing.

The useful framing in 2026 is no longer "warehouse vs lakehouse." It is: who owns the format, who owns the catalog, and how many engines touch the copy. See [§08](08-snowflake-vs-databricks.md).

### Medallion architecture (bronze, silver, gold)

The pattern was popularized by Databricks (⚠ verify the exact origin). It is a convention for progressive refinement:

- **Bronze:** data exactly as received, append-only, plus ingestion metadata such as source, load time and offsets. This is your **replay point**: if logic changes, you rebuild downstream from here without re-pulling the source (P5, P6).
- **Silver:** cleaned, typed, deduplicated and conformed entities, with keys and contracts enforced (P8).
- **Gold:** consumption-ready data: dimensional models, aggregates, feature tables and data products.

**Why it works:** each layer has a clear quality contract, a bad transform affects only one layer, and backfills are possible because raw data is never mutated.

**Criticisms:**

- It is a **naming convention, not a data model**. It says nothing about grain, keys, slowly changing dimensions or conformed dimensions, so it **doesn't replace dimensional modeling** ([§03](03-data-modeling.md)).
- **Copies multiply.** Each layer is another materialized copy, adding storage, compute and latency per hop (P7).
- **"Gold" is ambiguous.** It can mean marts, aggregates, semantic-layer tables or data products, and teams disagree on which.
- **Cargo-cult hops.** Teams force three layers when one or two would do, or each team builds its own bronze, silver and gold for the same source.
- **Silver becomes a dumping ground** when nobody owns its contract.

**When to collapse layers:**

- Merge bronze into silver when the source is already typed and trustworthy, such as CDC from a well-modeled OLTP database or a strict-schema API, *and* the upstream log (for example a Kafka topic with sufficient retention) can serve as the replay point.
- Make gold a set of views or semantic-layer metrics over silver when query cost allows.
- Cut hops on low-latency paths.
- **Keep bronze** when the source can't be re-pulled, because bronze is then your only history, and when audit or legal rules require raw retention.

### Lambda vs Kappa

**Lambda** (early 2010s) runs a **batch layer** that periodically recomputes views from an immutable master dataset (complete and correct, but hours late), a **speed layer** that processes recent events incrementally (fast and approximate, replaced once the batch catches up), and a **serving layer** that merges the two. It existed because stream processors of that era couldn't guarantee exactly-once results, handle late data or cheaply replay history, so batch was the correctness backstop. The cost is **two codebases** implementing the same logic in two engines. They drift apart ("why do the dashboard and the report disagree?") and double the operational work.

**Kappa** (2014) treats everything as a stream. The log is the source of truth (P5). To reprocess, you start a new version of the job from the earliest offset, write to a new output, cut over once it has caught up, and drop the old output. It requires log retention long enough to cover a replay (or a bronze table acting as the log), deterministic and idempotent processing (P6), and enough throughput to replay quickly.

**Why Kappa-style became practical:**

- **Unified engines.** Spark Structured Streaming runs the same DataFrame or SQL code incrementally or as batch, and Flink treats batch as a bounded stream. One codebase covers both.
- **Table formats as transactional sinks and sources.** A micro-batch commits its output atomically together with its progress, which makes exactly-once writes a property of the commit (P3, P6). Change feeds let tables act as streams.
- **Cheaper long retention** in Kafka through tiered storage ([§10](10-kafka.md)).

In practice the common shape is "Kappa-ish": stream raw events into a bronze table, process incrementally downstream, and backfill by re-running from bronze.

**When Lambda still makes sense:**

- Authoritative corrections arrive in batch, as with billing, finance reconciliation or restatements.
- The computation is impractical as a stream, such as joins over years of history or model training.
- Replaying years of history through a stream engine is too slow or costly.
- An approximate real-time view plus an authoritative nightly view is cheaper than making the stream exact (P7).

Processing semantics are covered in [§11](11-stream-processing.md).

### HTAP and "zero-ETL": trying to collapse P1

A newer trend puts transactional and analytical storage under one roof, or hides the pipe between them. Examples are Snowflake hybrid tables, which are row-oriented tables inside Snowflake (⚠ verify current availability), managed Postgres offerings on both platforms (Snowflake Postgres; Databricks Lakebase, ⚠ verify status of both), and cloud "zero-ETL" replication from OLTP databases into warehouses. None of these repeals P1. They run a row engine and a column engine side by side and automate the CDC between them. You still own modeling, replication lag, schema-change propagation (P8) and the cost of the managed pipe. See [§06](06-snowflake-features.md) and [§07](07-databricks-gap-fill.md).

## Comparison

| | OLTP (row store) | OLAP (column store) |
|---|---|---|
| Typical operation | Point read and write by key | Scan and aggregate many rows |
| Rows per query | Few | Millions to billions |
| Columns per query | All of a row | Few of many |
| Update cost | Cheap, in place | Expensive (rewrite or delete markers) |
| Optimized for | Concurrency, latency, integrity | Throughput, compression, scans |

| | Warehouse | Lake | Two-tier | Lakehouse |
|---|---|---|---|---|
| Storage | Coupled MPP, later cloud object storage | Object storage / HDFS | Both | Object storage |
| Format | Proprietary | Any (Parquet, JSON, CSV) | Both | Open table format on Parquet |
| Schema | On write | On read | Both | On write, with evolution |
| ACID | Yes | No | Warehouse only | Yes (metadata log) |
| ML access | Poor | Direct | Lake side only | Direct |
| Main pain | Cost, rigidity | Swamp, slow BI | Staleness, 2× cost, 2 governance models | Maintenance, catalog lock-in |

| | Lambda | Kappa |
|---|---|---|
| Code paths | Batch + speed (two codebases) | One streaming codebase |
| Reprocessing | Rerun batch layer | Replay the log into a new output |
| Correctness | Batch is the backstop | Requires exactly-once/idempotent processing |
| Prerequisite | None special | Long log retention, replay throughput |
| Use when | Batch corrections, heavy history compute | Logic is the same for live and historical data |

## Interview questions

**Q: Lakehouse vs warehouse vs lake: what's the difference, and what problem does each solve?**

A warehouse solved "the numbers disagree" with curated, schema-on-write, fast SQL, at the cost of price and rigidity. A lake solved cost and variety with cheap storage and schema-on-read, but lost ACID, governance and BI performance. A lakehouse adds a transactional metadata layer and performance features to open files, so one copy serves BI and ML (P3, P11). The tradeoff is that you now operate table maintenance and must choose a catalog of record.

**Q: Why did the lakehouse emerge?**

The two-tier pattern of a lake plus a warehouse copy meant stale data, double storage and compute, brittle ETL between two systems, ML locked out of warehouse data, and two governance models. Table formats made ACID possible on object storage, and statistics plus caching made open files fast enough for BI. Together those let the warehouse tier collapse into the lake. The CIDR 2021 lakehouse paper formalized the argument.

**Q: Why is columnar storage faster for analytics?**

A query reads only the columns it references, so its I/O is proportional to the columns used rather than the full row width. Same-typed columns compress well, which reduces I/O and memory further. Vectorized operators process batches of one type in cache-friendly, SIMD-friendly loops, and block-level min/max stats skip data entirely (P4). The flip side is that single-row updates are expensive, which is why you don't run OLTP on it.

**Q: Why are updates and deletes expensive on lake or columnar storage, and how do systems cope?**

A row is spread across compressed, immutable column segments, so changing one value means rewriting a block or file. Systems cope by batching changes, using copy-on-write rewrites for bulk changes, or using merge-on-read and deletion vectors that mark rows deleted and defer the rewrite to compaction ([§04](04-table-formats-catalogs.md)). Failure mode: high-frequency single-row updates without compaction produce small-file and delete-file buildup that slows every read.

**Q: What are the criticisms of medallion architecture?**

It's a naming convention, not a model. It says nothing about grain, keys or history, so it doesn't replace dimensional modeling. Every layer is a materialized copy with storage, compute and latency costs. "Gold" means different things to different teams, and teams often force three hops, or duplicate the layers per team, when one or two layers would do. Its genuine value is the replayable raw layer and explicit quality contracts, so keep those and skip the dogma.

**Q: When would you collapse medallion layers?**

Merge bronze and silver when the source is already clean and typed (for example CDC from a well-modeled database) and the upstream log retains enough history to replay. Serve gold as views or semantic-layer metrics when compute cost allows, and cut hops on latency-sensitive paths (P7). Don't drop bronze when the source can't be re-pulled or when audit requires raw retention, because then it is your only history.

**Q: Lambda vs Kappa?**

Lambda pairs a correct-but-slow batch layer with a fast-but-approximate speed layer. It was a workaround for early stream processors that couldn't guarantee correctness, and it costs two codebases that drift. Kappa uses one streaming codebase over a retained log and reprocesses by replay (P5, P6). It became practical with unified engines and transactional table sinks. Choose Lambda only when batch carries information the stream doesn't (late corrections, restatements) or when replaying history through a stream is too expensive.

**Q: How do you reprocess history in a Kappa architecture?**

Deploy the new job version reading from the earliest retained offset, or from the bronze table, and write to a new output table. Let it catch up while the old version keeps serving, compare the two outputs, then switch consumers and retire the old one. This only works if retention covers the replay window, processing is deterministic and idempotent, and the cluster can replay faster than real time. Otherwise you are effectively back to a batch backfill.

**Q: Is the lakehouse vs warehouse distinction still meaningful in 2026?**

The architecture has converged. Both separate storage from compute (P2), both read open table formats, and both offer managed SQL and governance. What still differs is format ownership (proprietary native tables vs open files by default), where optimizations can live, whether one engine or many touch the copy, and operating style (P10). The better interview answer is to compare who owns the format, who owns the catalog, and how many engines share one copy ([§08](08-snowflake-vs-databricks.md)).

**Q: Do HTAP or "zero-ETL" offerings eliminate data pipelines?**

They eliminate pipe-building, not P1. Underneath they run row and column storage side by side and automate the CDC between them. You still own modeling, replication lag, schema-change handling (P8) and the managed pipe's cost. They work well for operational analytics on fresh data from one source. They don't replace integrating many sources into conformed models.

## Takeaways
- Every architecture answers the same questions: where the analytical copy lives, in what format, who governs it, and how many copies exist.
- Columnar wins on scans through less I/O, compression, vectorization and skipping, and loses on single-row updates (P1).
- Warehouse led to lake, then two-tier, then lakehouse. Each fixed the last one's worst pain: rigidity, then the swamp, then two copies.
- In 2026 the meaningful axis is format ownership, catalog ownership and engine plurality, not the "warehouse" or "lakehouse" label.
- Medallion's value is the replayable raw layer and explicit layer contracts. Collapse layers that add no contract.
- Kappa-style processing (one codebase, replay the log) is the default now. Keep Lambda for batch-only corrections and history-heavy compute.

## Sources
- Lakehouse paper (CIDR 2021): https://vldb.org/cidrdb/papers/2021/cidr2021_paper17.pdf · https://www.databricks.com/research/lakehouse-a-new-generation-of-open-platforms-that-unify-data-warehousing-and-advanced-analytics
- Apache Parquet format specification: https://github.com/apache/parquet-format/blob/master/README.md
