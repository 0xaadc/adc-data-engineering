# Question Bank (Self-Test)

Every interview question from the primer, grouped by section. Answers are hidden: say your answer out loud, then expand to compare.
Grade yourself on **mechanism → consequence → tradeoff → when not** ([§01](01-first-principles.md)), not on matching the wording.

**203 questions.**

- [01. First Principles: Why Data Engineering Exists](#01-first-principles-why-data-engineering-exists) — 10 questions · [read section](01-first-principles.md)
- [02. Storage Paradigms and Architectures](#02-storage-paradigms-and-architectures) — 10 questions · [read section](02-storage-paradigms.md)
- [03. Data Modeling](#03-data-modeling) — 11 questions · [read section](03-data-modeling.md)
- [04. Open Table Formats and Catalogs](#04-open-table-formats-and-catalogs) — 12 questions · [read section](04-table-formats-catalogs.md)
- [05. Snowflake I: Architecture, Performance, and Cost](#05-snowflake-i-architecture-performance-and-cost) — 14 questions · [read section](05-snowflake-architecture.md)
- [06. Snowflake II: Data Features](#06-snowflake-ii-data-features) — 14 questions · [read section](06-snowflake-features.md)
- [07. Databricks Gap-Fill: What's New or Renamed](#07-databricks-gap-fill-whats-new-or-renamed) — 8 questions · [read section](07-databricks-gap-fill.md)
- [08. Snowflake vs. Databricks](#08-snowflake-vs-databricks) — 14 questions · [read section](08-snowflake-vs-databricks.md)
- [09. Batch Pipelines, CDC, and Orchestration](#09-batch-pipelines-cdc-and-orchestration) — 11 questions · [read section](09-batch-pipelines.md)
- [10. Kafka and Event Streaming, From Zero](#10-kafka-and-event-streaming-from-zero) — 13 questions · [read section](10-kafka.md)
- [11. Stream Processing: Time, State, and Exactly-Once](#11-stream-processing-time-state-and-exactly-once) — 14 questions · [read section](11-stream-processing.md)
- [12. Governance and Security](#12-governance-and-security) — 10 questions · [read section](12-governance-security.md)
- [13. Performance and Cost (FinOps)](#13-performance-and-cost-finops) — 8 questions · [read section](13-performance-cost.md)
- [14. Quality, Reliability, and DataOps](#14-quality-reliability-and-dataops) — 8 questions · [read section](14-reliability-dataops.md)
- [15. Why X over Y: The Tradeoff Bank](#15-why-x-over-y-the-tradeoff-bank) — 20 questions · [read section](15-why-x-over-y.md)
- [16. System Design Walkthroughs](#16-system-design-walkthroughs) — 8 questions · [read section](16-system-design.md)
- [17. Leadership and Behavioral (Architect / Co-Lead)](#17-leadership-and-behavioral-architect--co-lead) — 9 questions · [read section](17-leadership-behavioral.md)
- [18. AI on the Data Platform](#18-ai-on-the-data-platform) — 9 questions · [read section](18-ai-on-data-platform.md)

## 01. First Principles: Why Data Engineering Exists

<details>
<summary><b>Q: Why does data engineering exist as a discipline? What is a data platform's core job?</b></summary>

Operational systems are built for single-row writes and analytical consumers need large scans, and those workloads want opposite physical layouts (P1). So data has to be moved, reshaped, reconciled, and governed between them. The hard part isn't the moving; it's doing it correctly under partial failure (P6), at an acceptable freshness and cost (P7), while sources and schemas keep changing (P8). A platform's job is to make that repeatable for many teams, so each pipeline isn't a bespoke snowflake (lowercase).

</details>

<details>
<summary><b>Q: Why not just run analytics on the production database or a read replica?</b></summary>

Row-oriented OLTP storage makes large scans expensive, and analytical queries compete with transactions for the same resources. A read replica removes the contention but not the layout mismatch. It also does nothing for history (OLTP overwrites state), integration across sources, or governed access. A replica is reasonable for small-scale operational reporting; beyond that you want a columnar copy fed by CDC.

</details>

<details>
<summary><b>Q: Why separate storage and compute, and what does it cost you?</b></summary>

Decoupling lets you size and bill compute per workload, scale it to zero, and run many isolated clusters over one copy of the data (P2). That eliminates both the "resize means redistribute data" problem and contention between workloads. The cost is that object storage reads are high-latency network calls. Performance therefore depends on caching, metadata, and pruning (P4), and a cold cache after scale-to-zero is a real tradeoff.

</details>

<details>
<summary><b>Q: How do you get ACID transactions on object storage that doesn't support them?</b></summary>

Don't modify files. Write new immutable files, then commit by atomically publishing a new metadata version that lists the table's files (P3). Delta does this with an exclusive put of the next log entry; Iceberg does it with a compare-and-swap of the catalog's metadata pointer. Readers always see a consistent snapshot, and concurrent writers conflict-check at commit time (optimistic concurrency). Time travel and cheap clones are free side effects. Write amplification and small files are the price.

</details>

<details>
<summary><b>Q: What does "exactly-once" really mean?</b></summary>

No system can guarantee that a message is *delivered* exactly once across failures. What you can guarantee is that its *effect* is applied exactly once. That takes a replayable source (P5), state and offsets checkpointed together, and an idempotent or transactional sink (P6). Anything with external side effects, such as sending an email or calling an API, breaks the guarantee unless it is made idempotent too. Details are in [§11](11-stream-processing.md).

</details>

<details>
<summary><b>Q: Schema-on-read vs. schema-on-write: which do you prefer?</b></summary>

Both, at different layers. Land raw data schema-on-read so you can always replay from it (P5). Enforce schema at the boundary where data becomes a product others depend on (P8). Pure schema-on-read pushes parsing and breakage onto every consumer. Pure schema-on-write forces you to reject or lose data whenever a producer changes something.

</details>

<details>
<summary><b>Q: How do you decide how fresh the data needs to be?</b></summary>

Ask what decision consumes the data and how quickly that decision gets made, then price each latency tier (P7). Fraud blocking needs seconds. Inventory replenishment needs minutes to hours. Finance close needs correctness more than speed. Often the right answer is incremental batch on a schedule using the streaming code path, which can be tightened later without a rewrite.

</details>

<details>
<summary><b>Q: Open format vs. proprietary format: does it still matter?</b></summary>

Less than it used to, and in a different place. Snowflake supports Iceberg and Databricks supports Iceberg alongside Delta, so the file format is rarely the lock-in now. The catalog is: whoever holds the current-snapshot pointer and the policies controls access and interoperability (P11). A proprietary format can still win on co-designed optimization. The architect's question is: "Which engines need to write this table, and who is the catalog of record?"

</details>

<details>
<summary><b>Q: In one breath each: why Snowflake, why Databricks?</b></summary>

Snowflake: managed, SQL-first, strong workload isolation and sharing. It's the shortest path to governed, concurrent analytics for SQL-skilled teams (P10 managed side, P2). Databricks: one open copy of the data serving engineering, streaming, ML, and AI with code-first control (P11, P10 control side). They're converging, so the real decision is about team skills, the workload mix, and where governance lives ([§08](08-snowflake-vs-databricks.md)).

</details>

<details>
<summary><b>Q: What's the most common root cause of data incidents?</b></summary>

Unannounced upstream change: a renamed column, a changed meaning, a new enum value, or a source that silently stopped updating (P8). Next most common are non-idempotent reruns that duplicate data (P6) and incremental loads with flawed watermarks that silently skip rows ([§09](09-batch-pipelines.md)). The defenses are contracts at the producer boundary, freshness and volume checks, and steps designed to be replayed.

</details>

## 02. Storage Paradigms and Architectures

<details>
<summary><b>Q: Lakehouse vs warehouse vs lake: what's the difference, and what problem does each solve?</b></summary>

A warehouse solved "the numbers disagree" with curated, schema-on-write, fast SQL, at the cost of price and rigidity. A lake solved cost and variety with cheap storage and schema-on-read, but lost ACID, governance and BI performance. A lakehouse adds a transactional metadata layer and performance features to open files, so one copy serves BI and ML (P3, P11). The tradeoff is that you now operate table maintenance and must choose a catalog of record.

</details>

<details>
<summary><b>Q: Why did the lakehouse emerge?</b></summary>

The two-tier pattern of a lake plus a warehouse copy meant stale data, double storage and compute, brittle ETL between two systems, ML locked out of warehouse data, and two governance models. Table formats made ACID possible on object storage, and statistics plus caching made open files fast enough for BI. Together those let the warehouse tier collapse into the lake. The CIDR 2021 lakehouse paper formalized the argument.

</details>

<details>
<summary><b>Q: Why is columnar storage faster for analytics?</b></summary>

A query reads only the columns it references, so its I/O is proportional to the columns used rather than the full row width. Same-typed columns compress well, which reduces I/O and memory further. Vectorized operators process batches of one type in cache-friendly, SIMD-friendly loops, and block-level min/max stats skip data entirely (P4). The flip side is that single-row updates are expensive, which is why you don't run OLTP on it.

</details>

<details>
<summary><b>Q: Why are updates and deletes expensive on lake or columnar storage, and how do systems cope?</b></summary>

A row is spread across compressed, immutable column segments, so changing one value means rewriting a block or file. Systems cope by batching changes, using copy-on-write rewrites for bulk changes, or using merge-on-read and deletion vectors that mark rows deleted and defer the rewrite to compaction ([§04](04-table-formats-catalogs.md)). Failure mode: high-frequency single-row updates without compaction produce small-file and delete-file buildup that slows every read.

</details>

<details>
<summary><b>Q: What are the criticisms of medallion architecture?</b></summary>

It's a naming convention, not a model. It says nothing about grain, keys or history, so it doesn't replace dimensional modeling. Every layer is a materialized copy with storage, compute and latency costs. "Gold" means different things to different teams, and teams often force three hops, or duplicate the layers per team, when one or two layers would do. Its genuine value is the replayable raw layer and explicit quality contracts, so keep those and skip the dogma.

</details>

<details>
<summary><b>Q: When would you collapse medallion layers?</b></summary>

Merge bronze and silver when the source is already clean and typed (for example CDC from a well-modeled database) and the upstream log retains enough history to replay. Serve gold as views or semantic-layer metrics when compute cost allows, and cut hops on latency-sensitive paths (P7). Don't drop bronze when the source can't be re-pulled or when audit requires raw retention, because then it is your only history.

</details>

<details>
<summary><b>Q: Lambda vs Kappa?</b></summary>

Lambda pairs a correct-but-slow batch layer with a fast-but-approximate speed layer. It was a workaround for early stream processors that couldn't guarantee correctness, and it costs two codebases that drift. Kappa uses one streaming codebase over a retained log and reprocesses by replay (P5, P6). It became practical with unified engines and transactional table sinks. Choose Lambda only when batch carries information the stream doesn't (late corrections, restatements) or when replaying history through a stream is too expensive.

</details>

<details>
<summary><b>Q: How do you reprocess history in a Kappa architecture?</b></summary>

Deploy the new job version reading from the earliest retained offset, or from the bronze table, and write to a new output table. Let it catch up while the old version keeps serving, compare the two outputs, then switch consumers and retire the old one. This only works if retention covers the replay window, processing is deterministic and idempotent, and the cluster can replay faster than real time. Otherwise you are effectively back to a batch backfill.

</details>

<details>
<summary><b>Q: Is the lakehouse vs warehouse distinction still meaningful in 2026?</b></summary>

The architecture has converged. Both separate storage from compute (P2), both read open table formats, and both offer managed SQL and governance. What still differs is format ownership (proprietary native tables vs open files by default), where optimizations can live, whether one engine or many touch the copy, and operating style (P10). The better interview answer is to compare who owns the format, who owns the catalog, and how many engines share one copy ([§08](08-snowflake-vs-databricks.md)).

</details>

<details>
<summary><b>Q: Do HTAP or "zero-ETL" offerings eliminate data pipelines?</b></summary>

They eliminate pipe-building, not P1. Underneath they run row and column storage side by side and automate the CDC between them. You still own modeling, replication lag, schema-change handling (P8) and the managed pipe's cost. They work well for operational analytics on fresh data from one source. They don't replace integrating many sources into conformed models.

</details>

## 03. Data Modeling

<details>
<summary><b>Q: Kimball vs Inmon vs Data Vault?</b></summary>

They solve different problems. Kimball optimizes delivery and query usability, integrating through conformed dimensions. Inmon optimizes enterprise consistency with a normalized core, but delivers slowly. Data Vault optimizes integration of many changing sources with full auditability and parallel loading, and pushes usability into a presentation layer. In practice they stack rather than compete: Data Vault or 3NF in silver for integration and history, Kimball stars in gold for consumption.

</details>

<details>
<summary><b>Q: When would you choose Data Vault?</b></summary>

When there are many sources that change often, regulation requires a provable, insert-only record of what arrived and when, and several teams need to load in parallel without coordinating key generation. Hash keys and insert-only satellites deliver exactly those properties. Don't choose it for a small team with a handful of stable sources: you pay the join complexity, PIT/bridge maintenance and a second modeling layer for flexibility you won't use.

</details>

<details>
<summary><b>Q: Star vs snowflake vs OBT on a modern columnar engine?</b></summary>

Star is the default. Columnar compression makes denormalized dimensions nearly free, and single-hop joins are what optimizers and BI tools handle best. A snowflake schema saves storage that no longer matters and adds joins. OBT removes joins entirely and is great for serving, but it duplicates logic and freezes history at build time, so build it *from* the star. Never make it the only model.

</details>

<details>
<summary><b>Q: What is grain and why declare it first?</b></summary>

Grain is what one row means, for example "one row per order line per shipment". It determines which dimensions can attach and which measures can be summed. Declare it first because every double count traces back to mixed grains: a header measure repeated on lines, or a join to a finer-grained table. If you can't state a table's grain in one sentence, the table is wrong.

</details>

<details>
<summary><b>Q: A dashboard total is higher than finance's number. What do you check first?</b></summary>

Fan-out: a join to a finer-grained table, a many-to-many bridge or a second fact multiplies rows before the `SUM`. Then check for mixed grains (header values on line rows), SCD2 joins missing the `is_current` or effective-date predicate (so every version matches), and duplicate loads from a non-idempotent pipeline. The structural fix is drill-across: aggregate each fact to a common grain, then join on conformed keys.

</details>

<details>
<summary><b>Q: How would you implement SCD Type 2, end to end?</b></summary>

Detect changes by comparing a hash of the tracked attributes against the current row. For changed keys, close the current version (set `effective_to`, `is_current = false`) and insert a new row with a new surrogate key, in one atomic MERGE using the NULL-merge-key union trick. Fact loads look up the surrogate key whose effective range contains the event time. Watch for multiple changes per key in a batch (order them and build versions with `LEAD`), late changes that require splitting a version, and deletes. On Databricks, AUTO CDC `STORED AS SCD TYPE 2` handles sequencing for you. On either platform, dbt snapshots are the portable option.

</details>

<details>
<summary><b>Q: How do you choose between SCD Type 1, 2 and 3 per attribute?</b></summary>

Ask whether past facts should keep the old value. If not, or if the change is a correction, use Type 1. If analysis needs the value as it was, use Type 2. If the business needs only a one-step "before and after" view of a planned change, Type 3 is cheaper. Choose per column, not per table, and avoid making high-churn attributes Type 2: move them to a mini-dimension (Type 4), or the dimension explodes.

</details>

<details>
<summary><b>Q: Surrogate vs natural keys?</b></summary>

Natural keys change, get reused and collide across sources, and they can't represent unknown members. Surrogates insulate the model from all of that, and they are what makes Type 2 history possible. Keep the natural key as a durable attribute for lineage and as-is joins. Hash-based surrogates trade integer compactness for deterministic, lookup-free parallel loading, which suits Data Vault and distributed ingestion.

</details>

<details>
<summary><b>Q: How do you keep metric definitions consistent across tools?</b></summary>

Define each metric once, in a governed semantic layer (dbt MetricFlow, Snowflake semantic views, UC metric views or Cube), version it like code, and make BI tools and agents query through it instead of hand-writing SQL. Pick the layer by where consumption happens: a platform-native layer if one platform dominates, a warehouse-agnostic one if BI spans engines. Watch Apache Ossie (formerly OSI) for portability between them. The failure mode is a semantic layer that BI tools bypass, which leaves you with one more definition instead of one fewer.

</details>

<details>
<summary><b>Q: Why does a semantic layer matter for LLM text-to-SQL?</b></summary>

Without one, the model infers joins, grain and formulas from column names, so answers differ between runs and fall into fan-out traps while sounding confident. With one, the model selects from governed metrics and dimensions, joins are pre-validated, and permissions are already applied. That turns an open-ended generation problem into a constrained selection problem with auditable outputs.

</details>

<details>
<summary><b>Q: Is dimensional modeling dead in the lakehouse era?</b></summary>

The *physical* tricks are fading: aggressive aggregate tables and snowflaked dimensions mattered on row stores and expensive storage. The *logical* discipline is not: declared grain, conformed dimensions, explicit history and business-process facts are about meaning, and cheap compute doesn't make meaning free. OBTs, semantic layers and AI agents all sit on top of well-defined dimensions and facts, so dimensional modeling has become the substrate rather than the final serving layer.

</details>

## 04. Open Table Formats and Catalogs

<details>
<summary><b>Q: What does a table format add over Parquet?</b></summary>

Parquet describes one file. A table format describes which set of files is the table at version N, and publishes each new version atomically. That gives you atomic multi-file commits, snapshot isolation for readers, schema enforcement, time travel, and file-level stats that let planners prune without LIST calls (P3, P4). Without it, a failed job leaves partial data visible and an UPDATE means hand-rewriting directories.

</details>

<details>
<summary><b>Q: Delta vs Iceberg: how do you choose?</b></summary>

The mechanisms have converged: both have optimistic commits, deletion vectors, variant and row tracking or lineage. So choose by ecosystem and catalog, not features. If Databricks is the primary writer and Spark streaming dominates, Delta is the native path and UniForm can expose it to Iceberg readers. If several engines must write, or Snowflake, Trino or Flink are first-class, Iceberg with a REST catalog of record is the neutral choice. The real decision is which catalog owns commits and governance. The format follows from that.

</details>

<details>
<summary><b>Q: How does Iceberg achieve atomic commits?</b></summary>

The writer creates all new data and metadata files, which nobody can see yet, and then asks the catalog to compare-and-swap the table's metadata pointer from the version it started from to the new one. If another writer committed first, the CAS fails. The writer re-reads, checks for conflicts, rewrites its metadata on the new base and retries. Atomicity therefore depends on the catalog. A catalog with weak CAS semantics, or a writer that bypasses it, breaks isolation.

</details>

<details>
<summary><b>Q: How does Delta commit on object storage, and why was S3 a problem?</b></summary>

A commit is the creation of the next numbered JSON file in `_delta_log`, and it must fail if that file already exists. S3 didn't originally offer put-if-absent, so two clusters could both "win" version N+1. OSS Delta solved this with a DynamoDB-backed log store (still marked experimental), and Databricks with its own commit service. Catalog-managed tables (Preview in Delta 4.0, production-supported since 4.1) hand commit arbitration to the catalog, which is the same direction Iceberg took.

</details>

<details>
<summary><b>Q: Copy-on-write vs merge-on-read?</b></summary>

COW rewrites every file an update touches. Writes are expensive and reads are clean, which suits read-heavy tables with batch updates. MOR writes small deltas (log files, delete files, deletion vectors) and merges them at read time. Writes are cheap, but reads degrade until compaction runs, which suits CDC and frequent MERGE. The failure mode of MOR is skipping compaction, so read latency creeps up week by week.

</details>

<details>
<summary><b>Q: What are deletion vectors, and where does each format stand?</b></summary>

A deletion vector is a bitmap of row positions marked deleted in an immutable data file. A DELETE writes kilobytes instead of rewriting gigabytes, and an UPDATE becomes a DV plus a new row in a new file. Delta has had them as a table feature for years. Iceberg added them in v3, stored in Puffin, and v3 tables may no longer add position delete files. Hudi gets the same effect through MOR log files. The catch is interop: readers on an older spec (Iceberg v2 clients, UniForm v2) can't read tables that use them.

</details>

<details>
<summary><b>Q: Why do hidden partitioning and partition evolution matter?</b></summary>

In Hive-style tables users must filter on the physical partition column, and a query that filters on `ts` instead of `ts_date` silently scans everything. Iceberg derives partitions from transforms of real columns, so pruning happens no matter how the user writes the filter. Because each manifest remembers its spec, you can change granularity as volumes grow without rewriting history. Delta's answer to the same problem is liquid clustering rather than partitioning.

</details>

<details>
<summary><b>Q: What's the role of the catalog, and why is everyone fighting over it?</b></summary>

It maps names to the current metadata pointer, arbitrates commits, and holds grants, policies, lineage and credential vending. Once formats are open, the catalog is the remaining control point: whoever owns it owns what "current" means and who may see it (P9, P11). That's why Unity Catalog, Polaris/Open Catalog, Horizon and Glue all now speak the Iceberg REST protocol. The architect's job is to pick one catalog of record per table and federate the rest.

</details>

<details>
<summary><b>Q: How would you make one dataset readable by both Snowflake and Databricks?</b></summary>

Pick the catalog of record by who writes most. If Databricks writes, use a UC managed Iceberg table, or Delta with UniForm. Snowflake reads it through a catalog integration or catalog-linked database pointing at UC's Iceberg REST endpoint. If Snowflake writes, use a Snowflake-managed Iceberg table, and Databricks or Spark access it through Horizon's Iceberg REST API. Then pin the spec version both engines support, assign maintenance to one side, and re-implement row and column policies wherever data is read outside the owning catalog. Don't do this when one engine only needs a small aggregate; a shared or exported gold table is simpler.

</details>

<details>
<summary><b>Q: What breaks with two engines writing the same table?</b></summary>

Committing through two different catalogs creates two "current" versions, which is split brain. Committing through one catalog is safe, but problems remain. One engine can enable a spec feature the other can't read. One engine's files may lack the stats or layout the other's optimizer expects. Two maintenance jobs can race, or one can expire snapshots the other still needs. Policies may be enforced by one engine and bypassed by the other. Keep one primary writer per table, with other engines writing only through the same catalog, under agreed feature flags.

</details>

<details>
<summary><b>Q: When would you pick Hudi?</b></summary>

Pick Hudi for very high-rate upserts from CDC into large tables, where you need key-based routing to touch only affected files, plus incremental "changed since T" reads for downstream jobs. Its record-level index and MOR table type exist for exactly that. The costs are more knobs to tune (index type, compaction, cleaning) and less native support in Snowflake and Databricks, so you'd usually expose it to those engines via XTable or keep it inside a Spark/Flink-centric stack.

</details>

<details>
<summary><b>Q: What does VACUUM or snapshot expiry trade off?</b></summary>

Every rewrite leaves the old files in place, so storage grows and time travel keeps working until cleanup removes them. Aggressive retention saves cost but breaks long time travel, and it can break streaming readers or other engines that are still behind. Long retention costs storage and slows metadata operations. Set retention from the longest legitimate consumer lag plus your recovery-point needs, not from the default.

</details>

## 05. Snowflake I: Architecture, Performance, and Cost

<details>
<summary><b>Q: Explain Snowflake's architecture and why it matters.</b></summary>

It has three layers: immutable columnar micro-partitions in object storage, any number of isolated virtual warehouses, and an always-on cloud services layer that handles metadata, optimization, transactions and security. Because storage and compute are separate, they scale and bill independently (P2). You isolate a workload by giving it a warehouse instead of a copy of the data, and resizing takes seconds because nothing is redistributed. The tradeoff is a proprietary, managed format: you get almost no ops work but lose physical control and openness (P10, P11). Iceberg tables are the hedge.

</details>

<details>
<summary><b>Q: What is the cloud services layer actually doing?</b></summary>

It authenticates the session, checks privileges, parses and optimizes the query, prunes partitions from metadata, checks the result cache, and queues and dispatches work to a warehouse. It also commits DML by atomically publishing a new metadata version. That is why cached results and clones need no running warehouse. You pay for it only above 10% of daily warehouse compute. Millions of trivial queries or metadata-heavy jobs can cross that line, and you can't scale this layer yourself.

</details>

<details>
<summary><b>Q: How does Snowflake make a query fast without indexes?</b></summary>

Mostly by not reading data (P4). Per-partition min/max lets it prune at compile time, columnar storage reads only the referenced columns, and the result and warehouse caches skip repeated work. Vectorized parallel execution handles whatever is left. When filters don't line up with load order you add a clustering key, and for needle lookups you add SOS. The metadata is the index.

</details>

<details>
<summary><b>Q: How is Snowflake's storage different from a Delta table?</b></summary>

Both are immutable columnar files plus a metadata layer, which is how they get ACID, time travel and clones (P3). Delta is Parquet in your bucket with an open log that any Delta engine can read, and you maintain the layout yourself. Snowflake's micro-partitions are proprietary, keep stats on every column, and are sized and compacted for you, but only Snowflake's engine reads them directly. You trade openness for operability (P11). Snowflake-managed Iceberg tables sit in between.

</details>

<details>
<summary><b>Q: Scale up vs scale out?</b></summary>

Scale up when single queries are slow from big scans, joins or spilling: a parallelizable query at 2× size runs in about half the time for the same credits. Scale out (multi-cluster) when queries queue because of concurrency; no individual query gets faster. The profile tells you which you need: remote spill means scale up, queued time means scale out. The wrong choice spends money without fixing the problem.

</details>

<details>
<summary><b>Q: When would you define a clustering key and when not?</b></summary>

Define one on a multi-TB table that is filtered repeatedly and selectively on columns its load order doesn't sort, when clustering depth confirms poor pruning and reads far outnumber writes. Skip it for small, high-churn or already naturally clustered tables. Don't cluster on raw high-cardinality columns; use `TO_DATE(ts)`. Measure the clustering credits against the drop in partitions scanned. A table has only one sort order, so serve other access patterns with SOS or an MV.

</details>

<details>
<summary><b>Q: Clustering, Search Optimization, QAS, or a materialized view: how do you choose?</b></summary>

Each fixes a different problem. Clustering serves range filters on a few columns of a big table. SOS serves few-row lookups, substrings and VARIANT paths. QAS absorbs occasional outlier scans. An MV serves one aggregation that runs over and over. All four run in the background on credits, so each has to earn its keep on measured queries. "Enable everything" is the anti-pattern.

</details>

<details>
<summary><b>Q: What happens when a warehouse is suspended?</b></summary>

The compute is released and billing stops, once that run's 60-second minimum has been paid. The SSD cache is dropped. Data, metadata and the result cache are untouched, so cached results still come back. The next query resumes the warehouse in seconds, but on a cold cache, so the first queries run slower. Set `AUTO_SUSPEND` to match how far apart queries arrive: too short wastes the cache and repeats the minimums, too long pays for idle time.

</details>

<details>
<summary><b>Q: How would you control Snowflake costs?</b></summary>

Measure first: attribute spend with `WAREHOUSE_METERING_HISTORY`, `QUERY_HISTORY` and the serverless history views. Then fix the structure:

- one warehouse per workload;
- the smallest size that meets the SLA without remote spill;
- auto-suspend tuned to how queries arrive;
- caps on multi-cluster maximums, with the Economy policy where latency is tolerant.

Reduce the work itself with pruning-friendly layouts and incremental builds. Add guardrails: resource monitors, budgets and statement timeouts. Remember P7: a task that runs every minute keeps a warehouse awake all day.

</details>

<details>
<summary><b>Q: A dashboard query went from 2 seconds to 40. How do you diagnose it?</b></summary>

Split the time into queued, compiling and executing. Queued means add clusters or isolate the workload. Executing means check pruning, spill and the most expensive operator. Also check whether new frequent loads now invalidate the result cache, or a shorter auto-suspend leaves the cache cold. Fix the cause before upsizing.

</details>

<details>
<summary><b>Q: When does the result cache not help?</b></summary>

Any change to an underlying table invalidates it, so tables loaded every few minutes rarely get hits. The text has to match exactly, so tools that inject varying literals miss. Non-reusable functions, external functions and hybrid tables are never cached. For dashboards on fresh data, rely on the warehouse cache, MVs or interactive tables instead.

</details>

<details>
<summary><b>Q: How would you lay out warehouses for ETL, BI and data science?</b></summary>

Separate them; isolation is cheap because they all share the data.

- **ETL:** sized for throughput, single cluster, short auto-suspend.
- **BI:** multi-cluster with the Standard policy and a longer auto-suspend to keep the cache warm. Use interactive warehouses for sub-second serving.
- **Data science:** Snowpark-optimized.
- **Ad-hoc:** small, with a statement timeout.

Put a resource monitor on each one. Don't split so finely that dozens of warehouses each pay resume minimums on cold caches.

</details>

<details>
<summary><b>Q: Gen2 and Adaptive Compute are new. How do they change sizing decisions?</b></summary>

Gen2 charges a higher hourly rate for faster execution, so cost per job usually drops if runtime falls more than the rate rises. Benchmark it, especially on DML-heavy pipelines. Adaptive Compute removes the size decision and bills per query, which suits mixed, unpredictable workloads. In exchange, spend per hour is less predictable, the feature is newer, and it needs Enterprise in limited regions. Keep fixed sizes where budgets must be predictable.

</details>

<details>
<summary><b>Q: What does micro-partition immutability cost you?</b></summary>

Write amplification. Updating one row rewrites its whole partition, so MERGE-heavy CDC into a huge table rewrites many partitions. That costs compute, and the old versions stay billed through Time Travel and Fail-safe. To mitigate it, batch changes, cluster on the merge key, use transient tables for staging, and shorten retention on high-churn tables. For true row-level OLTP, use hybrid tables ([§06](06-snowflake-features.md)) or an OLTP database (P1).

</details>

## 06. Snowflake II: Data Features

<details>
<summary><b>Q: Time Travel vs Fail-safe?</b></summary>

Both keep superseded immutable micro-partitions instead of deleting them (P3). Time Travel is user-facing (query, clone, undrop): 1 day by default, up to 90 days on Enterprise for permanent tables. Fail-safe follows it for a fixed 7 days, reachable only by Snowflake Support, and is a backstop, not a recovery plan. Both cost storage in proportion to churn, which is why rebuildable high-churn tables should be transient.

</details>

<details>
<summary><b>Q: When do you choose transient over permanent tables?</b></summary>

When upstream data can rebuild the table, as with staging, intermediate models and dev copies. Transient tables skip Fail-safe and cap Time Travel at 1 day, which can cut storage several-fold on frequently rewritten tables. Keep raw landing data and systems of record permanent, because they are your replay source (P5).

</details>

<details>
<summary><b>Q: How does zero-copy cloning work and how would you use it?</b></summary>

A clone copies the micro-partition pointer list, so it is near-instant and free until either side writes. Writes then create new micro-partitions owned only by the writer. I use clones for production-sized dev environments, for backfills (clone, backfill, validate, `SWAP WITH`), and for point-in-time debugging with `AT(...)`. Watch for missing grants on table clones, tasks starting suspended, and long-lived clones pinning storage. A clone is not DR.

</details>

<details>
<summary><b>Q: How do Streams give you exactly-once-ish processing?</b></summary>

A stream is a stored offset that advances only when a DML statement reading it commits, so consuming the delta and writing the result are atomic: a failure rolls both back and a retry sees the same delta (P6). It is "ish" because upstream systems can resend data, so the MERGE must still be keyed and order-aware.

</details>

<details>
<summary><b>Q: What happens if a stream goes stale, and how do you prevent it?</b></summary>

Once the offset falls outside the source's retention, the history needed for the delta is purged and the stream becomes unusable. Recovering means recreating it and running a full reconciliation. Prevent it by consuming regularly (triggered tasks), alerting on `STALE_AFTER`, and setting adequate retention on CDC sources. The retention extension Snowflake applies is a buffer, not a guarantee.

</details>

<details>
<summary><b>Q: Streams + Tasks vs Dynamic Tables?</b></summary>

Dynamic tables are declarative: you state the result and a freshness target, and Snowflake handles incremental refresh and scheduling across the DAG. They are my default for SQL transformations. Streams + Tasks are imperative, which is right for custom SCD2, procedural logic or side effects. The dynamic-table risk is a query that silently refreshes in FULL mode on a large table, so check its refresh mode and refresh history.

</details>

<details>
<summary><b>Q: Dynamic tables vs materialized views?</b></summary>

Snowflake MVs are single-table with no joins, always current and serverless-maintained, so they are an acceleration structure for one hot query. Dynamic tables are pipeline building blocks that support joins and chains, refreshed to a chosen lag on your warehouse.

</details>

<details>
<summary><b>Q: Snowpipe vs Snowpipe Streaming vs COPY?</b></summary>

All three are idempotent loaders that differ in unit and trigger. COPY loads batches of files on your warehouse and remembers them for 64 days. Snowpipe is event-driven serverless COPY with about a minute of latency and 14-day pipe dedupe. Snowpipe Streaming pushes rows with offset tokens and lands data in seconds. Buy only the freshness the decision needs (P7).

</details>

<details>
<summary><b>Q: How do you get exactly-once delivery from Kafka into Snowflake?</b></summary>

The Kafka connector (version 4, built on high-performance Snowpipe Streaming) maps partitions to channels and commits Kafka offsets as offset tokens. On restart it resumes after the last committed token, so neither crashes nor rebalances duplicate or drop records (P5/P6). Duplicates sent by the producer still need a business-key dedupe downstream.

</details>

<details>
<summary><b>Q: How does Snowflake data sharing work without copying data?</b></summary>

In-region accounts share the storage and metadata layer, so a share is only grants, and the consumer's warehouse reads the provider's micro-partitions in place (P2, P3). The provider pays storage and the consumer pays compute, and both updates and revocations take effect instantly. Cross-region sharing needs replication or listing auto-fulfillment. Compared with Delta Sharing's open file-URL protocol, it gives up reach in exchange for live policy enforcement.

</details>

<details>
<summary><b>Q: When would you use Iceberg tables in Snowflake?</b></summary>

When several engines must read or write the same data, or the data must stay in your bucket in an open format (P11). Snowflake-managed Iceberg tables keep near-native features while other engines use Horizon's Iceberg REST endpoint. Externally managed tables, or a catalog-linked database, let another catalog remain the system of record. If Snowflake is the only engine, native tables are simpler and better optimized.

</details>

<details>
<summary><b>Q: What is Snowpark and why not just use Spark?</b></summary>

Snowpark is a DataFrame API compiled to SQL and run on warehouses, with Python UDFs and stored procedures for what SQL can't express. It avoids data export, a second governance perimeter and cluster operations (P1, P9, P10). Spark remains better for low-level distributed control, streaming and lakehouse-centred estates.

</details>

<details>
<summary><b>Q: How would you build an incremental pipeline entirely in Snowflake?</b></summary>

Land data with Snowpipe or Snowpipe Streaming into permanent, append-only raw tables, which serve as the replay source (P5). For custom semantics, put an append-only stream on raw and have a triggered task run a keyed, deduplicating MERGE (P6). Build the downstream models as dynamic tables, with `DOWNSTREAM` lag on intermediates and SLA-based lag on the marts. Make staging transient, test changes on a clone, and monitor task history, refresh history and stream staleness.

</details>

<details>
<summary><b>Q: Why is a warehouse vendor shipping Postgres, hybrid tables and Openflow?</b></summary>

Data originates in OLTP systems, and moving it between write-optimized and read-optimized stores is the core cost (P1). Owning both ends removes ELT hops and gives AI apps governed low-latency state. Be sceptical of hybrid tables for high-throughput OLTP, and weigh the lock-in of putting every tier with one vendor.

</details>

## 07. Databricks Gap-Fill: What's New or Renamed

<details>
<summary><b>Q: Streaming table or materialized view: how do you choose?</b></summary>

A streaming table processes each source record once using a checkpoint. That makes it cheap and right for append-only ingestion, but it won't reprocess an upstream correction. A materialized view keeps a query result correct as its inputs change, which suits joins, aggregations and sources with updates or deletes. The cost is a more expensive refresh when it can't be incremental. I use streaming tables at bronze and MVs above that. When a streaming source starts receiving updates, I switch that step to AUTO CDC or an MV.

</details>

<details>
<summary><b>Q: Why liquid clustering over partitioning plus Z-order? When would you still partition?</b></summary>

Partitioning fixes the data layout in directory names at write time. A wrong or high-cardinality key causes small files and skew, and changing it means rewriting the table. Z-order rewrote whole partitions on every `OPTIMIZE`. Liquid clustering keeps keys in metadata, clusters incrementally, and lets you change keys without a rewrite. I'd still partition only a very large table with a stable, low-cardinality key that every query filters on, such as a date used to drop old data.

</details>

<details>
<summary><b>Q: When doesn't Photon help?</b></summary>

Photon speeds up columnar operators: scans, joins, aggregations and writes. It does nothing for Python or pandas UDFs, RDD code, I/O-bound jobs or very short queries, either because it falls back to Spark or because the bottleneck is elsewhere. On classic compute it bills a higher DBU rate. So the real test is cost per job with Photon on and off.

</details>

<details>
<summary><b>Q: Serverless or classic compute?</b></summary>

Serverless removes cluster management, starts in seconds and upgrades itself, which makes it the right default for SQL, notebooks and most jobs (P10). Classic still wins when you need specific instance types or GPUs, Spark configs that serverless blocks, compute that must stay inside your network, or spot pricing for large steady jobs. The risk with serverless is cost visibility. Watch the billing system tables and set budget policies.

</details>

<details>
<summary><b>Q: How do system tables support FinOps and governance?</b></summary>

They expose usage, prices, audit logs, lineage and query history as queryable Delta tables. Joining usage to list prices and custom tags gives cost by team, job or project, which supports chargeback and catches idle compute. Audit and lineage tables show who accessed what and what sits downstream of a change. The prerequisite is tagging discipline, because untagged spend can't be attributed.

</details>

<details>
<summary><b>Q: How does Delta Sharing work, and how would you share with a Snowflake account?</b></summary>

The sharing server authorizes the recipient and returns short-lived pre-signed URLs. The recipient reads the files directly with its own compute, and the provider remains the single source of truth. Between workspaces I'd use Databricks-to-Databricks sharing. For Snowflake I'd use open sharing, which Snowflake now consumes natively as a catalog-linked database. The shared tables are read-only in Snowflake, and I'd plan for token expiry or use OIDC instead of bearer tokens.

</details>

<details>
<summary><b>Q: Why use Declarative Automation Bundles instead of building in the UI?</b></summary>

Jobs, pipelines and dashboards created by clicking drift between environments, can't be code-reviewed and are hard to recreate. A bundle declares them as YAML next to the code, so CI/CD deploys the same definition to dev, staging and prod with per-target parameters. That makes deployments repeatable (P6), and changes go through the same review as code.

</details>

<details>
<summary><b>Q: Why put Postgres (Lakebase) inside a lakehouse, and when wouldn't you?</b></summary>

Apps need fast single-row reads and writes, which columnar tables handle poorly (P1). A separate OLTP database plus reverse ETL adds a pipeline, a copy and another governance model. Lakebase separates storage from compute to get autoscaling, scale-to-zero and instant branches, all under platform governance. I wouldn't move a high-throughput core system of record there until its HA and operational track record meet our SLAs.

</details>

## 08. Snowflake vs. Databricks

<details>
<summary><b>Q: Why Snowflake when Databricks exists?</b></summary>

You buy an operating model as much as an engine. Snowflake owns the format, metadata and tuning, so a small team gets fast, concurrent SQL with almost nothing to run, and per-workload warehouses isolate BI from ELT by design (P2, P10). Sharing is mature because storage is central, so a share is a grant rather than a copy. I wouldn't make it the primary platform for heavy custom ML or complex stateful streaming, and I'd model the cost of large always-on compute first.

</details>

<details>
<summary><b>Q: Why Databricks when Snowflake exists?</b></summary>

One open copy of data serves SQL, streaming, data science and ML under one catalog, and you keep control of compute (P11). It suits organizations whose hard problems are engineering problems: messy ingestion, streaming, Python feature pipelines, model training and serving, and a no-lock-in mandate. The price is surface area. Clusters, runtimes, layout and cost need engineering discipline, even with serverless. For a small SQL-only BI team it's more platform than they need.

</details>

<details>
<summary><b>Q: Your company runs both; how do you split workloads?</b></summary>

Split by strength and by the direction data flows, not by team preference. Commonly that's Databricks for ingestion, engineering and ML, and Snowflake for curated BI serving and external sharing, or the reverse where Snowflake is already the warehouse of record. The boundary is a set of contracted data products (P8), each with one writer and one catalog of record, read across through Iceberg instead of copied. Centralize identity, tags and cost reporting. If the overlap keeps growing, consolidation is a legitimate outcome, because running both costs two governance models and two skill sets.

</details>

<details>
<summary><b>Q: How would you govern data consistently across both?</b></summary>

Policies don't travel with files, so each table gets one catalog of record that owns and enforces its policies (P9). Use one IdP with SCIM-synced groups and a shared tag taxonomy applied at write time, and keep policies as code deployed to both Unity Catalog and Horizon. Serve fine-grained-protected data only through the engine that enforces it, and give the other platform a masked or aggregated product. Merge both platforms' audit logs in one place, because silent drift between two policy sets is the usual failure.

</details>

<details>
<summary><b>Q: How do you avoid two copies of the data?</b></summary>

Store each shared table once, as Iceberg or as Delta with UniForm, with one catalog of record that owns commits and maintenance. The other engine reads through Iceberg REST: Snowflake through a catalog-linked database pointed at Unity Catalog, or Databricks through catalog federation against Horizon. Delta Sharing into Snowflake is also a no-copy path. The failure modes are writers in two catalogs, table features one engine can't read, and maintenance run by the wrong side. Sometimes a governed copy is still right, for example when the consumer needs native-only performance or the alternative is cross-region reads.

</details>

<details>
<summary><b>Q: Compare the cost models.</b></summary>

Snowflake bills credits for warehouse time per second (60-second minimum per resume) at a rate that doubles per size step, plus serverless features, cloud services above 10% of daily warehouse use, and storage. Databricks bills DBUs by compute type; classic compute adds your cloud VM bill, while serverless bundles infrastructure into the DBU rate. Unit prices don't compare directly, because cost is rate × runtime plus idle time, and runtime depends on the engine and how well concurrency is packed. I'd benchmark our top workloads on both, include people in TCO, and hunt for idle leaks.

</details>

<details>
<summary><b>Q: Where does Fabric fit?</b></summary>

Fabric is Microsoft's SaaS analytics platform on OneLake, with capacity pricing and native Power BI. In an Azure and Power BI shop it usually fits first as the consumption layer: Azure Databricks tables mirrored as metadata through shortcuts, Snowflake data mirrored or read as Iceberg, and Direct Lake serving Power BI without import refreshes. As a full replacement, weigh contention on shared capacity, uneven feature maturity, and the third governance model it adds. It's a poor fit for multi-cloud estates or teams that need strict per-workload compute isolation.

</details>

<details>
<summary><b>Q: What's converging, and does the choice still matter?</b></summary>

Features are converging: both have SQL warehouses, Python, notebooks, Iceberg, managed Postgres, ingestion connectors, semantic layers and AI assistants. Defaults and maturity aren't. Snowflake's default path is managed storage and SQL, Databricks' is open tables and code, and each one's non-default path is younger. So the choice now turns on operating model, team skills, cost on your workloads, and where the catalog of record lives, which is the new lock-in point (P11). Pick the platform whose default path is your main workload.

</details>

<details>
<summary><b>Q: Dashboards slow down during nightly ELT. How does each platform fix that?</b></summary>

Both fix it by giving each workload its own compute over shared storage (P2). In Snowflake that's separate warehouses, with a multi-cluster warehouse for BI so peaks add clusters instead of queuing. In Databricks it's a dedicated serverless SQL warehouse for BI while ELT runs on job compute. If dashboards are still slow after that, the cause is usually the data: ELT is rewriting the tables BI reads, which invalidates caches and layout. Publish gold tables atomically, on a schedule.

</details>

<details>
<summary><b>Q: Both platforms write to the same Iceberg table. What goes wrong?</b></summary>

An Iceberg commit atomically swaps the current-metadata pointer in a catalog (P3). Writers using one shared catalog just conflict and retry. Writers using different catalogs each see a different "current" pointer, so one silently reads stale data or overwrites the other's snapshot. Maintenance run from one side can also delete files the other side still references. Make one platform the writer and owner, and route the other platform's changes through it.

</details>

<details>
<summary><b>Q: How would you run a fair cost and performance bake-off?</b></summary>

Use your own top workloads by spend, not vendor benchmarks, covering ELT, BI concurrency and at least one ML pipeline. Let each platform's experts tune their side, then measure cost per completed workload at the required SLA, including idle time. Add operating effort and migration cost to get TCO. Re-run periodically, because engines and price lists change.

</details>

<details>
<summary><b>Q: To serve Databricks data to Snowflake, would you use managed Iceberg, UniForm or Delta Sharing?</b></summary>

Managed Iceberg in Unity Catalog is cleanest when Snowflake is a first-class reader, because there's one format and no translation. UniForm keeps Delta for Databricks-native features and adds Iceberg metadata asynchronously, so expect brief lag and limit the table to features Iceberg readers support. Delta Sharing fits a consumer in another account or organization, since a share is a revocable, audited grant, and Snowflake now consumes shares natively. In every case there's one writer, and the consumer doesn't write back.

</details>

<details>
<summary><b>Q: Where is the lock-in in each platform?</b></summary>

Snowflake native tables are readable only by Snowflake. Iceberg tables remove the format lock-in, but proprietary features such as sharing, governance and some performance features remain. Databricks data is open, so its lock-in sits in Unity Catalog policies and lineage, jobs, pipelines and runtime-specific features. On both, the catalog with its permissions, lineage and semantics is the real lock-in point (P11). Estimate exit cost by what it takes to migrate policies and pipelines, not bytes.

</details>

<details>
<summary><b>Q: Both platforms now offer managed Postgres. Why, and when would you use it?</b></summary>

Applications need fast single-row reads and writes, which columnar engines handle poorly (P1). Reverse ETL to a separate database adds pipelines, copies and governance gaps. Snowflake Postgres and Lakebase put OLTP next to the analytical data, with sync paths and shared governance. That suits app state, serving lookups and agent memory. I wouldn't move a high-volume core system of record onto either until its HA and operational track record meets our SLAs.

</details>

## 09. Batch Pipelines, CDC, and Orchestration

<details>
<summary><b>Q: ETL vs ELT: when each?</b></summary>

ELT is the default because cheap object storage and separately billed elastic compute (P2) make it cheap to keep raw data and rebuild from it without re-extracting. Choose ETL, or at least a small in-flight transform, when regulated data must never land raw, when transforms are heavy and not SQL, or when raw volume is huge and low value. A common mistake is landing PII raw "temporarily": clones, time travel and fail-safe keep copies alive long after the delete.

</details>

<details>
<summary><b>Q: How do you design an idempotent pipeline?</b></summary>

Parameterize every run by a logical interval rather than `now()`, so the same parameters select the same input. Write with partition overwrite (facts) or MERGE on business keys with an ordering guard (entities, CDC). Never use bare append. Stamp `_run_id` and source positions so one run's output can be removed, and rely on atomic table commits so a crash leaves nothing partial. The test: rerun yesterday's job twice and diff the outputs.

</details>

<details>
<summary><b>Q: Log-based vs query-based CDC?</b></summary>

Query-based CDC polls a change column, so it can't see deletes or intermediate updates, it loads the source, and it is exposed to late-commit and clock-skew misses. Log-based CDC reads the WAL/binlog the database already writes, so it gets every committed change in commit order at low source cost. You pay with privileged access, snapshot handoff, schema-change handling and log-retention risk (a stalled consumer can fill the source disk). Use query-based for append-mostly sources where deletes don't matter, and log-based for anything correctness-critical.

</details>

<details>
<summary><b>Q: Your incremental load silently misses rows. What are the likely causes?</b></summary>

The usual cause is long transactions: rows stamped before your watermark but committed after you read past it. Others: clock skew across writers, `updated_at` not maintained by bulk updates, hard deletes, sequence IDs committing out of order, `>` vs `>=` or precision truncation, and an upper bound that moves during the run. Fix it with a lookback overlap plus idempotent MERGE, a fixed upper bound that trails `now()`, and count/hash reconciliation per partition. If exactness matters, move to log-based CDC.

</details>

<details>
<summary><b>Q: How do you apply a CDC stream to a table correctly?</b></summary>

Land the events append-only. Dedup to the latest event per key ordered by LSN, not arrival time. MERGE only events newer than the target's stored position, so replays and overlaps are no-ops. Choose soft vs hard deletes deliberately: soft propagates downstream, hard is required for erasure. Failure modes: duplicate keys within a batch making the MERGE nondeterministic, and ordering by ingest time after a retry.

</details>

<details>
<summary><b>Q: How do you handle late-arriving data?</b></summary>

For late facts, overwrite a rolling reprocessing window of N days, sized from the measured lateness distribution, and backfill the long tail. A wider window costs more (P7). For late dimensions, insert an inferred member with a real surrogate key so facts link immediately, then overwrite it in place when the true record arrives. Silently dropping or holding facts is the failure mode: revenue disappears until someone notices.

</details>

<details>
<summary><b>Q: How would you backfill two years of data safely?</b></summary>

Use the production job itself, parameterized over historical intervals and chunked by month with bounded concurrency, so it can be resumed and doesn't hammer the source. Read from a replica, and run on isolated compute to protect production SLAs and attribute the cost. Write into a zero-copy clone, validate counts, sums and key sets against prod, then swap. Keep time travel as the rollback, and reprocess downstream incremental models. If the backfill needs different code from the daily job, fix idempotency first.

</details>

<details>
<summary><b>Q: Airflow vs Lakeflow Jobs vs Snowflake Tasks?</b></summary>

Choose by where the work lives and how heterogeneous the estate is (P10). Airflow 3 is the neutral cross-system control plane (remote execution, DAG versioning, asset events), but self-hosting it is real operational load. Lakeflow Jobs and Snowflake Tasks are zero-infrastructure and natively aware of their platform's objects and triggers, but weak outside it. A common split: native schedulers inside each platform, with Airflow or Dagster only where pipelines cross systems.

</details>

<details>
<summary><b>Q: Schedule-driven or event-driven orchestration?</b></summary>

Schedules are predictable, but they trade wasted runs against latency because you're guessing when upstream data will be ready. Event triggers run exactly when data lands. They bring bursts, duplicate events and the need for absence alerting. Use events for data-readiness dependencies and schedules for business deadlines, and monitor freshness either way.

</details>

<details>
<summary><b>Q: dbt vs Dynamic Tables / Lakeflow pipelines?</b></summary>

dbt gives portability, CI-friendly tests, docs and environments, and a vendor-neutral DAG. Native declarative tables let the engine own incremental computation and scheduling, with fewer moving parts and lower latency, in exchange for lock-in (P10, P11). The native risk is a silent full-refresh fallback, so pin the refresh mode. Pragmatically: author in dbt and materialize latency-sensitive models as dynamic or streaming tables, and check an adapter's Fusion status before standardizing on Fusion.

</details>

<details>
<summary><b>Q: Build vs buy ingestion?</b></summary>

Buy connectors for commoditized SaaS and standard databases. API churn and schema drift are toil with no differentiation. Build, or use streaming CDC, where volume or churn makes row-based pricing exceed engineering cost, where latency needs are below the tool's sync floor, or where data can't transit a vendor. Either way, land in open formats and keep raw so the vendor can be replaced.

</details>

## 10. Kafka and Event Streaming, From Zero

<details>
<summary><b>Q: Can you explain Kafka to me as if I'm a DBA?</b></summary>

Kafka is the database's WAL taken out of the database and turned into a shared, durable service. Applications append events in order. Any number of readers tail it from their own positions and can rewind, the way a replica replays a log. Partitions hash-partition the log for parallelism, so order holds within a partition but not across partitions. It isn't a query engine. You derive tables from it (P5).

</details>

<details>
<summary><b>Q: How does Kafka guarantee ordering?</b></summary>

Only within a partition. Records route by `hash(key)`, so per-key order holds when three things are true: the key is the entity that needs ordering, idempotence prevents retries from reordering, and the partition count never changes. Global order requires a single partition and therefore a single consumer, so design so you don't need it. Parallel consumer threads can still reorder events, so carry a version field and apply only newer changes.

</details>

<details>
<summary><b>Q: How many partitions should a topic have?</b></summary>

Use the larger of two numbers: target throughput divided by measured per-partition throughput (check the producer and the slowest consumer), or the maximum consumer parallelism you'll need. Add growth headroom, because adding partitions to a keyed topic later remaps keys. Don't overshoot either, since each partition costs memory, replication and recovery time. Most topics land in the tens.

</details>

<details>
<summary><b>Q: What happens when a consumer in a group dies?</b></summary>

Once `session.timeout.ms` passes without a heartbeat, the coordinator reassigns its partitions. The new owner resumes from the last committed offset, so any uncommitted work is redone. That's at-least-once delivery, so sinks must be idempotent. Eager rebalancing pauses the whole group; cooperative assignment and KIP-848 move only the dead member's partitions. A consumer that's alive but slower than `max.poll.interval.ms` gets evicted the same way, which is a classic cause of rebalance storms.

</details>

<details>
<summary><b>Q: acks=all with min.insync.replicas=2 and RF=3: what failures can you survive?</b></summary>

One broker, or one zone if replicas are rack-aware. Leadership fails over, writes continue, and nothing acknowledged is lost, because every acknowledged record was on at least two replicas. With two brokers down, writes stop (`NotEnoughReplicas`), but acknowledged data survives and is still readable. Unclean election would trade that durability for availability. Bad writes and regional outages need cross-cluster replication and backups.

</details>

<details>
<summary><b>Q: At-least-once vs exactly-once: how does Kafka implement EOS and what are its limits?</b></summary>

At-least-once means process, then commit. A crash causes duplicates but never loss, and that's the sane default. EOS combines three pieces: the idempotent producer (a producer ID plus sequence numbers), transactions that atomically commit outputs together with input offsets and fence zombies, and `read_committed` consumers. Its limits: it covers Kafka-to-Kafka processing only, external side effects fall outside it, and it adds latency. End to end, you still need idempotent sinks (P6, [§11](11-stream-processing.md)).

</details>

<details>
<summary><b>Q: Log compaction: when and why?</b></summary>

Use it when a topic holds the current state per key rather than a history: table CDC, reference data, config, or state-store changelogs. A new consumer rebuilds the whole table by replaying from offset 0 without needing infinite retention, and tombstones carry deletes. Don't use it where every event matters. Don't treat it as dedupe either, because the active segment isn't compacted. Tiered storage also excludes compacted topics.

</details>

<details>
<summary><b>Q: How do you handle schema evolution?</b></summary>

Treat the schema as a contract the producer owns (P8): Avro or Protobuf in a registry, compatibility set per subject, checks in CI. BACKWARD means you deploy consumers first, FORWARD means producers first, and FULL allows either order, which suits widely shared topics. For breaking changes such as renames or type changes, add a new field or a versioned topic and dual-publish during migration.

</details>

<details>
<summary><b>Q: How would you do CDC from Postgres into the lakehouse?</b></summary>

I'd use Debezium with `pgoutput` and a replication slot. It takes a snapshot and then streams the WAL to one topic per table, keyed by primary key, in Avro. Land the raw events append-only in bronze, then `MERGE` into silver by key in LSN order, applying deletes ([§11](11-stream-processing.md)). The main risk is a stalled slot pinning WAL and filling the primary's disk, so alert on slot lag. If minutes of latency are acceptable and nothing else consumes the stream, managed CDC to files is simpler (P7, P10).

</details>

<details>
<summary><b>Q: What's the outbox pattern?</b></summary>

It solves dual writes, because a service can't atomically update its database and publish to Kafka. The service writes the business change and an outbox event row in one local transaction. CDC then publishes the committed outbox rows, keyed by aggregate ID. The database provides atomicity; publishing is at-least-once, so consumers dedupe on event ID. The cost is an outbox table to prune and CDC infrastructure to run.

</details>

<details>
<summary><b>Q: Kafka vs Event Hubs vs Kinesis: how do you choose?</b></summary>

Start from the cloud and the team (P10). Event Hubs is Azure-native and gives you a Kafka endpoint with no brokers, but partitions and retention are tier-capped, you can't tune brokers, and newer Kafka features lag. Kinesis is AWS-native and integrates with Lambda and Firehose, but it has its own API and per-shard limits. Choose Kafka (MSK, Confluent or self-run) for Connect, transactions, compaction, long retention or portability. Prefer managed Kafka unless you have a platform team.

</details>

<details>
<summary><b>Q: Consumer lag is growing. How do you diagnose it?</b></summary>

First check whether it's a few partitions (a hot key, a poison pill or a hung consumer) or all of them (a throughput mismatch). Then check rebalances, consumer CPU and the sink. Slow database writes or tiny file commits are the most common cause. Fix it by scaling to the partition count, batching sink writes, rekeying, or sending poison pills to a DLQ. Add partitions only if parallelism really is the ceiling, and judge lag in seconds against the freshness SLA (P7).

</details>

<details>
<summary><b>Q: Why did Kafka drop ZooKeeper?</b></summary>

ZooKeeper was a second distributed system to run and secure, and failover through it was slow, which also capped partition counts. KRaft stores metadata as a Raft-replicated internal log, so you get faster failover, more partitions and one system to operate. Since 4.0, KRaft is the only mode, so a ZooKeeper cluster must migrate while on 3.x (3.9 is the bridge release) before it can upgrade.

</details>

## 11. Stream Processing: Time, State, and Exactly-Once

<details>
<summary><b>Q: Event time vs processing time: why does it matter?</b></summary>

Event time is when it happened. Processing time is when the engine saw it, and the gap includes offline devices, outages and backlogs. Business aggregates must use event time, because processing-time results change with lag and won't reproduce on replay or backfill. The price is disorder: you must decide how long to wait for late events, which is the watermark. Processing time is fine for metrics about the pipeline itself.

</details>

<details>
<summary><b>Q: Explain watermarks to a non-streaming engineer.</b></summary>

It's the engine saying "I'm fairly sure I've now seen everything up to 10:05," usually computed as the latest event time minus an allowed delay. When it passes a window's end, the engine publishes that result and frees its memory. Tight means fast but drops stragglers; loose means complete but slower, with more memory. It's a latency-vs-completeness dial (P7), set from the measured lateness distribution.

</details>

<details>
<summary><b>Q: How do you handle late-arriving events?</b></summary>

First, price lateness with the business. Then pick a policy: drop, allowed lateness with re-fired corrections, a side output to a repair table, or updates into an upserting sink. For financial numbers, I pair a fast provisional stream with a scheduled batch reconciliation from the log. Always count late drops; silent drops are the real failure.

</details>

<details>
<summary><b>Q: What makes a stream–stream join expensive?</b></summary>

Neither side is ever complete, so both are buffered in state until no future match is possible: roughly rate × retention on each side. You bound it with watermarks on both inputs plus an event-time range condition; outer joins require both, and they emit unmatched rows only after the watermark passes. If "current dimension value" is enough, use a stream–static join; if "as of event time" matters, use a temporal join. Both are far cheaper.

</details>

<details>
<summary><b>Q: How does a streaming job recover without losing or double-counting?</b></summary>

It snapshots source offsets and operator state consistently. Flink sends barriers through the data and snapshots asynchronously. Spark logs planned offsets before each batch and the commit after it, so an incomplete batch re-runs with identical input. That gives exactly-once *state*. Exactly-once *output* additionally needs an idempotent or transactional sink.

</details>

<details>
<summary><b>Q: How do you get exactly-once from Kafka to a Delta table and to a Snowflake table?</b></summary>

Exactly-once needs a replayable source, deterministic checkpointed processing, and an idempotent or transactional sink. For Delta, the streaming sink records the batch id in the commit, so replays are skipped; in `foreachBatch` I use `txnAppId`/`txnVersion` or a MERGE. For Snowflake, the Kafka connector v4 on Snowpipe Streaming tracks committed offsets per channel and resumes from them, which is "store the offset with the data." I still MERGE on event id downstream, because producer-side duplicates upstream of Kafka aren't caught by any sink protocol.

</details>

<details>
<summary><b>Q: Flink vs Spark Structured Streaming: when each?</b></summary>

On a Databricks lakehouse, Spark is my default: one API for batch and stream, Delta-native exactly-once, managed operations, and seconds-to-minutes latency. Flink wins when you need sub-second latency *and* heavy event-time logic: large keyed state, timers, temporal joins, CEP, Kafka-to-Kafka services. Spark's real-time mode (GA on Databricks) narrows the latency gap but still excludes stream–stream outer joins, session windows and Delta sinks. Flink's cost is a second platform, so I'd prefer a managed service.

</details>

<details>
<summary><b>Q: Kafka Streams vs Flink?</b></summary>

Kafka Streams is a library: deployed like any microservice, scaled by instances up to the partition count, with state made durable in Kafka changelog topics. It fits Kafka-to-Kafka logic owned by an application team. Flink is a separate engine with broader connectors, richer event time and better large-state rescaling, so I'd use it for shared platform pipelines and non-Kafka sinks. One watch-out for Kafka Streams: restoring a lost instance's state can be slow without standby replicas.

</details>

<details>
<summary><b>Q: Design: stream from Kafka into both Databricks and Snowflake.</b></summary>

Kafka is the shared log (P5) with Schema Registry contracts (P8), and each platform consumes independently. Databricks: a Lakeflow streaming table into bronze Delta. Snowflake: Kafka connector v4 (~5–10 s) into a raw table, then Dynamic Tables. Alternatively, land once in an open table (Tableflow, or a bronze Iceberg/Delta table that Snowflake reads) to avoid two copies (P11). The trade-offs are two catalogs to govern, duplicate cost, and reconciliation: I'd run daily per-hour count and checksum comparisons.

</details>

<details>
<summary><b>Q: When would you push back on a real-time requirement?</b></summary>

When no one acts faster than the data refreshes: an hourly dashboard, a daily decision, a human reviewer, or a batch-only source. I ask what happens in the first minute and what a 15-minute delay costs. If it costs nothing, I propose the same streaming code run as `availableNow` on a schedule. That is near-batch cost, with no 24×7 compute or pager, and keeps the upgrade path open (P7). I'd still go real-time for fraud, outages or in-session personalization.

</details>

<details>
<summary><b>Q: How do you reprocess or backfill a streaming pipeline?</b></summary>

Treat the raw log as truth and make the sink idempotent (P5, P6). If Kafka retention covers the range, run the new version with a fresh checkpoint into a *new* table, validate, then swap (blue/green). If it doesn't, backfill from bronze Delta/Iceberg with the same code in batch or `availableNow` mode, then hand off at a known offset. During fast replays, partitions racing ahead in event time inflate state; Flink watermark alignment or `maxOffsetsPerTrigger` keeps it bounded.

</details>

<details>
<summary><b>Q: You changed a streaming query and it won't restart from its checkpoint. Why, and what now?</b></summary>

The checkpoint encodes state layout: operators, keys, state schema, and Spark's shuffle partition count. Changing any of those invalidates it; stateless edits usually don't. Either migrate state (Flink savepoints with stable UIDs; limited state schema evolution in `transformWithState`) or start fresh and backfill. Prevention: version checkpoint paths with the code and ask "is this state-compatible?" in review.

</details>

<details>
<summary><b>Q: A job's state keeps growing until it falls over. What do you check?</b></summary>

Find what never expires: an aggregation without a watermark, `dropDuplicates` without event time, an unbounded stream–stream join, `complete` mode, or keyed state without TTL. Then look for key explosion (a request id used as a grouping key) and a frozen watermark (an idle Flink partition, or a lagging input under Spark's min policy). Fix the logic before tuning RocksDB.

</details>

<details>
<summary><b>Q: Streaming into Delta created millions of small files. What do you do?</b></summary>

It's mechanical: each micro-batch commits at least one file per writing task. Lengthen the trigger if freshness allows, reduce write parallelism, or enable optimized writes. Then add auto-compaction or scheduled `OPTIMIZE` (or predictive optimization), and use liquid clustering instead of high-cardinality partitions. Keep compaction decoupled from ingestion so neither blocks the other.

</details>

## 12. Governance and Security

<details>
<summary><b>Q: RBAC or ABAC: when do you use each?</b></summary>

RBAC suits coarse, stable boundaries such as catalog or schema access per team, because it's easy to audit. It breaks when access depends on several attributes at once, since roles multiply and every new table needs grants. ABAC moves the decision onto tags and user attributes, so policy count tracks rules rather than tables. The catch is that correctness now depends on tagging quality: a missed tag is an exposed column.

</details>

<details>
<summary><b>Q: How would you design a Snowflake role hierarchy?</b></summary>

Use access roles for object privileges, compose them into functional roles that model jobs, and grant only functional roles to users via SCIM. Roll everything up to SYSADMIN, keep ACCOUNTADMIN to a few MFA-protected people, and administer grants through SECURITYADMIN. Managed access schemas and future grants stop ad hoc grants and keep new tables covered. Define all of it in IaC so drift shows up in a diff.

</details>

<details>
<summary><b>Q: How does Unity Catalog's permission model differ from Snowflake's?</b></summary>

UC grants go to users, groups or service principals, and effective rights are the union of a user's groups. Snowflake evaluates the session's active role. UC privileges inherit down catalog → schema → table, so a catalog grant covers future tables without a separate mechanism. Because "current role" and "member of group" are different predicates, policies and access reviews port awkwardly between the two.

</details>

<details>
<summary><b>Q: How do you implement row-level security, and what does it cost?</b></summary>

Attach a row policy (row access policy, row filter or ABAC policy) that the engine injects as a predicate. This is better than dynamic views, which duplicate logic and are bypassed through base tables. The cost is evaluation on every query: a mapping-table lookup becomes a join on every read, and opaque UDFs can block pruning and caching. Keep the logic deterministic, base it on existing attributes such as group or region, and load-test with real roles.

</details>

<details>
<summary><b>Q: Why tags plus policies instead of per-column grants?</b></summary>

Per-column grants scale with columns × roles and must be repeated for every new table, so coverage decays. You classify once, automatically and then reviewed by a person. A tag-bound policy then protects every current and future column carrying the tag, and access questions become queries. The failure mode is classifier false negatives, which is why producer-side tagging belongs in the contract.

</details>

<details>
<summary><b>Q: An auditor asks who accessed customer PII in the last 90 days. How do you answer?</b></summary>

Join PII-tagged columns to access logs. In Snowflake that is `ACCESS_HISTORY` (base columns per query) with tag references and `QUERY_HISTORY`. In Databricks it is `system.access.audit` with lineage and query-history system tables. Retention bounds the answer, so check it ahead of time. Anything outside the platform, such as extracts or copies on the other platform, is invisible, which is why restricting direct storage access matters.

</details>

<details>
<summary><b>Q: What is a data contract and where is it enforced?</b></summary>

A data contract is a versioned agreement on schema, semantics, freshness SLA, quality rules, owner and deprecation policy. It is enforced in producer CI, at ingestion through validation and quarantine, and through catalog metadata. The technology is trivial. It works only if a named producer is accountable and changes ship as versions with notice (P8).

</details>

<details>
<summary><b>Q: When do you need customer-managed keys or Tri-Secret Secure?</b></summary>

When regulation or contracts require that you can revoke the provider's ability to read your data. It costs higher editions or tiers, key operations, and a new failure mode: a revoked or lost key is a self-inflicted outage. If the requirement is only "encrypted at rest," default encryption already meets it.

</details>

<details>
<summary><b>Q: How do you govern consistently across Snowflake and Databricks?</b></summary>

Unify identity (one IdP, SCIM groups), taxonomy (one tag set) and ownership (one owner per dataset). Enforce each dataset's policy in the platform that owns it, and give the other governed access paths, never raw storage credentials, because masks live in the engine. Accept that policy semantics don't translate 1:1. Generate both from one IaC definition or use a push-down policy engine, and centralize audit logs.

</details>

<details>
<summary><b>Q: Analysts are exporting data to spreadsheets because access is too slow. What do you do?</b></summary>

Treat it as a governance failure, because shadow copies have no masking, lineage or audit. Measure time-to-access, add self-service requests with auto-approval for low-sensitivity data, and offer masked or aggregated access instead of denial. Then close the export paths that remain.

</details>

## 13. Performance and Cost (FinOps)

<details>
<summary><b>Q: How do you make a slow query fast in Snowflake vs Databricks?</b></summary>

Start with the profile, not the warehouse size. In Snowflake, check partitions scanned vs total (pruning), bytes spilled (memory), and exploding joins. In Databricks, check files pruned, shuffle size, skewed tasks and spill in the Spark UI or query profile. Fix pruning first: filter on clustered columns, cluster or liquid-cluster the table, and compact small files. Then fix the plan (join order, broadcast, skew), and only then size up to remove spill. Scaling first hides a layout problem you'll keep paying for.

</details>

<details>
<summary><b>Q: What's the small-file problem and how do you fix it?</b></summary>

Every file carries fixed overhead: a metadata entry, listing and open calls, footer parsing and poor compression. Thousands of tiny files make planning and scanning slower than the data volume justifies. Streaming micro-batches and over-partitioning are the usual causes. In Databricks, fix it with `OPTIMIZE`, optimized writes and auto-compaction, a target file size, predictive optimization, and fewer or coarser partitions. In Snowflake the engine sizes micro-partitions, so the concern shifts to staging well-sized files for loading.

</details>

<details>
<summary><b>Q: How would you implement chargeback?</b></summary>

Make attribution structural rather than voluntary. Snowflake: warehouses per team, tags, and `QUERY_TAG` on shared warehouses. Databricks: mandatory tags through compute policies, and serverless usage policies for serverless. Then publish an allocation rule for shared costs such as idle time, platform services and shared storage. Run showback for a few cycles until untagged spend is small and teams trust the numbers, then switch to chargeback. Chargeback on poor tagging produces arguments, not savings.

</details>

<details>
<summary><b>Q: Costs doubled last month. How do you investigate?</b></summary>

Decompose top-down. Split by service (compute, serverless, storage, transfer) using `METERING_DAILY_HISTORY` or `system.billing.usage` joined to list prices, then by warehouse or cluster, then by team tag, then by query or job. Look for step changes: a new always-on job, a disabled auto-suspend, a full refresh replacing an incremental one, a clustering key causing reclustering churn, or a retry loop. Check unit cost too. If the cost per pipeline run is flat, growth is volume, not waste.

</details>

<details>
<summary><b>Q: Serverless vs provisioned compute: what are the cost tradeoffs?</b></summary>

Serverless removes idle and startup cost and the work of capacity planning. You pay a higher unit rate in exchange for paying only while work runs. It wins for spiky, intermittent or small workloads. Provisioned compute on commitments can be cheaper for steady, high-utilization workloads and gives more control over instance types and networking (P10). Decide on measured utilization, not list price.

</details>

<details>
<summary><b>Q: How do you prevent runaway spend?</b></summary>

Layer the controls. Use hard stops where they exist: Snowflake resource monitors suspend warehouses, and Databricks compute policies cap size and enforce auto-termination. Add budgets that alert on forecasts, including serverless, which resource monitors don't cover. Statement and job timeouts kill runaway queries. Restrict who can create or resize compute. Budgets alone don't stop anything, so a budget without a hard stop is just a notification.

</details>

<details>
<summary><b>Q: When do you scale up vs scale out?</b></summary>

Scale up when single queries are slow because of spill or a large scan. For parallelizable work, doubling the size roughly halves runtime at about the same cost. Scale out when queries are queuing, which is a concurrency problem a bigger node won't fix. Sizing up for concurrency just pays for idle cores.

</details>

<details>
<summary><b>Q: When is clustering worth paying for?</b></summary>

Clustering pays off when a table is large, queries filter consistently on a few columns, and the natural load order doesn't match those filters. It does not pay off on small tables, on tables with no stable filter pattern, or on high-churn tables where reclustering never settles. Measure partitions or files scanned before and after against the clustering spend. Snowflake bills Automatic Clustering as a serverless line, and Databricks bills OPTIMIZE and predictive optimization as compute.

</details>

## 14. Quality, Reliability, and DataOps

<details>
<summary><b>Q: How do you ensure data quality in a pipeline?</b></summary>

Use layers that match the pyramid. Contract and schema checks at ingestion, unit tests on transforms in CI, data tests on every load, reconciliation against the source, and anomaly detection for what nobody anticipated. Each check gets an action (block, quarantine or warn) chosen by the cost of bad data downstream, and every table has an owner who receives its alerts. Tests without owners and actions are just logs.

</details>

<details>
<summary><b>Q: What is write-audit-publish?</b></summary>

Write new data somewhere consumers can't see it, audit it with quality checks, and only then publish atomically. Mechanisms include a staging table, a clone or an Iceberg branch, published by swap, MERGE or fast-forward. Readers see either the old good state or the new validated state, never a half-written or failed one. The costs are extra storage and latency, so reserve WAP for tables where wrong data is expensive.

</details>

<details>
<summary><b>Q: Design DR for a Snowflake + Databricks platform.</b></summary>

Start from RPO and RTO per data tier, not one number for everything. Snowflake: replication groups for databases, failover groups (Business Critical) for account objects and promotion, and Client Redirect so clients follow the primary. Databricks: managed DR where it's available and fits, otherwise deep-clone replication plus IaC to rebuild workspaces and UC objects. Replicate the dependencies too (IdP, private endpoints, secrets, orchestrator), keep immutable backups for logical errors because replication copies mistakes, and run failover drills.

</details>

<details>
<summary><b>Q: How do you structure dev/test/prod for data?</b></summary>

Isolate environments by account or database (Snowflake) or by workspace-bound catalog (Databricks). Give each developer a schema. CI builds changed models against clones or deferred prod references. Test data comes from zero-copy or shallow clones so it's realistic and cheap, with masking enforced so non-prod doesn't become a PII leak. Deploys go through automation only (bundles, DCM, Terraform), so prod has no manual drift.

</details>

<details>
<summary><b>Q: A dashboard showed wrong numbers this morning. Walk through your response.</b></summary>

First, contain: banner the dashboard, notify consumers and pause downstream publishes. Then triage with lineage. Walk upstream from the dashboard and compare freshness, volume and schema at each hop until the first bad node, and walk downstream to find everything else affected. Fix the cause, backfill the affected partitions with idempotent reruns, and verify by reconciliation before lifting the banner. In the postmortem, add the check that would have caught it, and ask why the monitoring didn't.

</details>

<details>
<summary><b>Q: How do you deploy a breaking schema change safely?</b></summary>

Turn it into a sequence of non-breaking steps with expand/contract. Add the new structure alongside the old, backfill and dual-write, move consumers with notice using lineage to find them, and drop the old structure after a published deprecation window. Version the contract and gate producer CI on compatibility checks. When dual-write isn't possible, use a versioned table or view plus blue/green cutover.

</details>

<details>
<summary><b>Q: Is replication a backup?</b></summary>

No. Replication copies the current state, including your mistakes, usually within minutes. A backup is an independent point-in-time copy that can't be altered by the incident it protects against. Time Travel covers short-window logical errors. WORM backups or deep clones cover ransomware, insiders and longer retention.

</details>

<details>
<summary><b>Q: How do you avoid alert fatigue in data observability?</b></summary>

Alert on SLO risk for tables that have real consumers, not on every failed check. Route alerts to the owner, and suppress downstream alerts once an upstream root cause has fired. Tune anomaly detectors on history before turning on paging. Send warn-level signals to a digest instead of a pager.

</details>

## 15. Why X over Y: The Tradeoff Bank

<details>
<summary><b>Q: Why choose Snowflake when Databricks exists?</b></summary>

Choose Snowflake when the center of gravity is SQL analytics and BI with many concurrent consumers, and the team is small relative to what it supports. Separate virtual warehouses over shared storage give each workload hard isolation (P2), and the managed service removes most tuning (P10). Sharing and the marketplace are mature, and Iceberg tables have narrowed the old openness gap (P11). I wouldn't make it the primary engine for heavy custom Python/ML or high-volume stateful streaming, where Spark-native tooling is deeper. I'd also govern cost early: per-second credits suit bursty SQL, but constant heavy compute with no guardrails gets expensive.

</details>

<details>
<summary><b>Q: Why choose Databricks when Snowflake exists?</b></summary>

Choose Databricks when the workloads are diverse (ingestion, streaming, ML, AI) and you want one open copy of the data in your own storage to serve all of them (P11, P3). Code-first teams get control over compute, libraries, and engines. Unity Catalog governs tables, models, and features in one place. The tradeoff is more surface area to operate and govern, which serverless has reduced but not eliminated. For a small SQL-only BI team that wants zero operations, it is usually more platform than they need.

</details>

<details>
<summary><b>Q: Lakehouse or warehouse?</b></summary>

The real question is whether you want one open copy served to many engines and workload types (P11, P3), or storage and engine co-designed and managed for SQL (P10). The warehouse wins for governed, SQL-centric analytics with minimal ops. The lakehouse wins for mixed workloads, ML on the same data, very large raw data, or an open-format requirement. In 2026 the line is blurry: Snowflake reads and writes Iceberg, and Databricks runs serverless SQL warehouses. So the decision that still matters is who owns the storage format and catalog, and which engines need to write.

</details>

<details>
<summary><b>Q: Delta or Iceberg?</b></summary>

Both implement transactions on object storage (P3) with different metadata designs. Delta uses an ordered transaction log. Iceberg uses a tree of metadata and manifests, with the atomic commit happening in the catalog, plus hidden partitioning and partition evolution. Iceberg has the broadest multi-engine write support and an open REST catalog spec. Delta has the deepest integration and the fastest feature delivery on Databricks. Decide by which engines must *write*: Databricks-centric means Delta (exposing Iceberg metadata to readers via UniForm); multi-engine or Snowflake-primary means Iceberg. Either way, the format matters less than choosing one catalog of record and one writer per table ([§04](04-table-formats-catalogs.md)).

</details>

<details>
<summary><b>Q: ETL or ELT?</b></summary>

ELT became the default because elastic in-platform compute made transforming after load cheap (P2), and keeping raw data lets you replay when the logic changes (P5). ETL still wins when data must be filtered or masked before it lands (PII, residency), when transforms are heavy and non-SQL (binary parsing, ML enrichment), or when raw volume is too costly to keep. Most real platforms are really "EtLT": validate and scrub lightly in flight, then model heavily inside the platform.

</details>

<details>
<summary><b>Q: Streaming or batch?</b></summary>

Start from how quickly the consuming decision is made (P7). Streaming pays for always-on compute, state management, small files, and harder reprocessing and on-call. Batch is cheaper, simpler, and easier to backfill. Stream when value decays in seconds to minutes: fraud, alerting, operational triggers. The pragmatic middle is to write the logic with incremental, streaming semantics and run it on a schedule (e.g., an `availableNow` trigger, or a Dynamic Table with a target lag), then tighten the latency only when a real consumer needs it.

</details>

<details>
<summary><b>Q: A stakeholder asks for "real-time." How do you respond?</b></summary>

Translate the request into a requirement by asking what action someone takes on the data, how quickly, and what a five-minute or one-hour delay would actually cost. Then price the options: daily, hourly, minutes, seconds, each with its compute, complexity, and on-call cost (P7). Often "real-time" means "fresher than yesterday", and a 15-minute incremental pipeline meets the need at a fraction of the cost. When seconds genuinely matter (fraud blocking), build streaming for that narrow path only, not for the whole platform.

</details>

<details>
<summary><b>Q: Kafka or a managed event bus (Event Hubs, Kinesis, Pub/Sub)?</b></summary>

Kafka, whether self-managed, Confluent, or MSK, offers the richest ecosystem (Connect, Schema Registry, Kafka Streams, transactions, compaction, tiered storage) and portability across clouds. Managed buses offer less to operate and native cloud integration, but less control, tier-based limits on retention and partitions, and only partial Kafka feature compatibility. On Azure at modest scale, Event Hubs with its Kafka-compatible endpoint is often enough. At high scale, multi-cloud, or with heavy CDC/Connect use, I'd choose managed Kafka. I would never self-manage Kafka without a team staffed to run it (P10) ([§10](10-kafka.md)).

</details>

<details>
<summary><b>Q: Do you even need Kafka for CDC into the lakehouse?</b></summary>

Not always. If the only goal is replicating an operational database into the lakehouse, a managed connector (Lakeflow Connect, Fivetran, Openflow-style tooling) or Debezium writing straight to storage may be simpler and cheaper. Kafka earns its operational cost when several consumers need the same change stream, when event-driven services consume it, or when you need replay and fan-out as a shared backbone (P5). Adding Kafka for a single consumer is complexity without payoff.

</details>

<details>
<summary><b>Q: Flink or Spark Structured Streaming?</b></summary>

Flink processes record by record, with first-class event time, rich keyed state and timers, and millisecond latency. It suits complex event processing and large stateful applications where streaming *is* the product. Spark Structured Streaming defaults to micro-batches and shares its API with batch Spark. Its latency is good enough for most analytics, and it fits naturally when the stream feeds Delta tables on Databricks. Choose Flink when latency or state complexity is the core requirement and you can staff another engine. Choose Spark when streaming is a means of keeping the lakehouse fresh ([§11](11-stream-processing.md)).

</details>

<details>
<summary><b>Q: Data mesh or a centralized platform team?</b></summary>

Data mesh means domain ownership, data as a product, a self-serve platform, and federated governance. It removes the central-team bottleneck at scale, because domains understand their own data best. It only works with mature domain teams, a strong platform, and enforced standards; without them it fragments into inconsistent silos with duplicated effort. For small or medium organizations a central team is more efficient. The realistic middle is "mesh-lite": a central platform team owns the paved roads and governance (P9), and domains own their data products on top.

</details>

<details>
<summary><b>Q: Kimball, Data Vault, or One Big Table?</b></summary>

They optimize for different things:
- **Kimball star schemas** optimize for business comprehension and query performance, using conformed dimensions.
- **Data Vault** optimizes for auditable, insert-only integration of many changing sources with parallel loads, but needs a presentation layer (usually Kimball marts) on top.
- **OBT** optimizes for simple consumption on columnar engines, where repeated values compress well (P4), but it duplicates logic and handles history poorly.

They layer rather than compete: normalized or Vault-style integration in silver, Kimball marts in gold, and OBTs as consumption views for specific dashboards or ML features ([§03](03-data-modeling.md)).

</details>

<details>
<summary><b>Q: dbt or native transforms (Dynamic Tables, Lakeflow pipelines)?</b></summary>

dbt gives you version-controlled SQL with tests, docs, lineage, and a large ecosystem, and it is portable across engines. Native declarative transforms give you managed incremental refresh, dependency scheduling, and streaming semantics without an external tool, at the cost of platform lock-in (P10). Choose dbt for multi-platform estates and analyst-heavy teams; choose native for tight incremental or streaming pipelines on one platform. They aren't exclusive: dbt adapters can materialize models as platform-native objects: `dynamic_table` in dbt-snowflake, and `materialized_view` / `streaming_table` in dbt-databricks (the latter needs Unity Catalog and serverless SQL warehouses).

</details>

<details>
<summary><b>Q: Serverless or provisioned compute?</b></summary>

Serverless means the vendor holds capacity: fast start, no idle cost visible to you, nothing to tune. The price is a higher unit cost and less control over instance types, networking, and libraries. Provisioned or reserved compute is cheaper per unit at sustained high utilization, but you pay for idle time and for operating it. Bursty, interactive, or unpredictable workloads favor serverless; steady, long-running, high-utilization jobs favor provisioned. Whichever you choose, put budgets and attribution on serverless spend, because "no clusters to manage" also means "no cluster to notice is expensive" ([§13](13-performance-cost.md)).

</details>

<details>
<summary><b>Q: Build or buy ingestion?</b></summary>

Buy the commodity, build the differentiator. Standard SaaS sources (CRM, ERP, ads platforms) are solved problems, and maintaining their API changes yourself is pure cost. Build when the source is unusual, when volume makes per-row pricing explode (high-churn tables under monthly-active-row pricing), or when latency or control requirements exceed what the vendor offers. Count the people cost of maintenance, the security review, and the exit cost, not just the license ([§09](09-batch-pipelines.md)).

</details>

<details>
<summary><b>Q: One platform or two (Snowflake + Databricks)?</b></summary>

One platform minimizes governance surface, data copies, and the skills split. Two can be justified by clear workload specialization (e.g., Databricks for engineering and ML, Snowflake for BI and sharing) or by organizational history such as an acquisition. If you run both, define the rules explicitly:
- a single writer per table
- one catalog of record
- interop through open formats or sharing rather than copies
- identity federated from one IdP
- cost attributed per platform

Running two because two teams each prefer one is an organizational problem disguised as an architecture ([§08](08-snowflake-vs-databricks.md)).

</details>

<details>
<summary><b>Q: Partitioning or clustering?</b></summary>

Hive-style partitioning physically separates data by directory. That works for coarse, low-cardinality columns that every query filters on (e.g., date on very large tables). On high-cardinality columns it produces small files, and a bad choice is expensive to undo. Clustering sorts data within files so that min/max ranges are narrow and skipping works (P4), and modern implementations (liquid clustering, Snowflake automatic clustering) can be changed and maintained incrementally. Today's default is to cluster rather than partition. Databricks' own guidance is: don't partition under 1 TB, prefer liquid clustering from 1 to 100 TB, and consider partitioning only above ~100 TB, with each partition holding at least 1 GB.

</details>

<details>
<summary><b>Q: <code>MERGE</code> into current state, or append and deduplicate on read?</b></summary>

Append is cheap to write and keeps full history (P5). `MERGE` gives consumers clean current state but rewrites files on every update (P3 write amplification). Append-plus-dedup-in-a-view moves that cost to every read. Decide by the read-to-write ratio: write-heavy, rarely read data favors append; widely read data favors `MERGE`, ideally with deletion vectors to cheapen the rewrites. A common hybrid keeps an append-only history table and a merged current-state table derived from it.

</details>

<details>
<summary><b>Q: Precompute (materialized views, aggregates) or compute at query time (views)?</b></summary>

Precomputing trades storage and refresh compute for read latency and per-query cost. Query-time views keep one definition and always-fresh results, but pay compute on every read. Precompute when reads vastly outnumber refreshes and the freshness tolerance allows it (P7); keep views when results are rarely read or must be exact at query time. Incremental refresh (Dynamic Tables, materialized views over streaming tables) shifts the balance toward precompute.

</details>

<details>
<summary><b>Q: On Azure, where does Microsoft Fabric fit versus Databricks or Snowflake?</b></summary>

Fabric is a SaaS bundle with capacity-based pricing, centered on Power BI and OneLake, with Delta as its native table format. It fits Microsoft-centric organizations that are BI-led and have moderate engineering and ML needs. Databricks or Snowflake fit when engineering, ML, or SQL-warehouse requirements are heavier, or the platform needs multi-cloud portability. Coexistence is common and cheap thanks to OneLake shortcuts and mirroring, so the question is usually "where does each workload live" rather than either/or ([§08](08-snowflake-vs-databricks.md)).

</details>

## 16. System Design Walkthroughs

<details>
<summary><b>Q: Can you walk me through how you'd approach a design question?</b></summary>

I clarify consumers, the decisions they make, freshness and correctness needs, volumes, compliance, and team skills, and I state my assumptions aloud. I sketch sources → ingest → store → transform → serve → govern, then go deep on the hardest one or two parts. I finish with tradeoffs, failure modes and recovery, and evolution at 10x. Each component gets a one-line "why" tied to a requirement.

</details>

<details>
<summary><b>Q: How do you decide between a single platform and two?</b></summary>

My default is one, because a second platform doubles governance planes, skills, and integration failure points. I add one only for a concrete reason: an existing estate with real users, a sharing ecosystem, or a workload one engine serves clearly better. If I do, data stays in open formats with one catalog of record per table, and policy-as-code plus drift detection start on day one. I revisit the decision yearly, because feature gaps close.

</details>

<details>
<summary><b>Q: How do you de-risk a warehouse migration?</b></summary>

I skip unused workloads, build foundations once, and migrate in domain waves starting with a low-complexity one. Each wave runs in parallel with legacy through at least one month-end and is reconciled through counts, sums, and row hashes against pre-agreed tolerances. Cutover is per domain, with legacy read-only and a rehearsed BI-repoint rollback. Decommission waits for owner sign-off.

</details>

<details>
<summary><b>Q: What would you monitor first in a new platform?</b></summary>

Data SLAs first: freshness and completeness at the gold outputs people use, because that's what users feel. Then pipeline failures, Kafka consumer lag and CDC slot lag, daily cost per workload with anomaly alerts, and reads of restricted data. Infrastructure metrics come after. Every alert needs an owner and a runbook ([§14](14-reliability-dataops.md)).

</details>

<details>
<summary><b>Q: Your design must cut cost by 40%; what do you change?</b></summary>

First I attribute cost per workload, since a few usually dominate. Then I work through a menu:
- Delete unused pipelines and dashboards.
- Relax freshness the decision doesn't need (P7).
- Convert full refreshes to incremental.
- Improve pruning with clustering (P4).
- Right-size compute and auto-suspend it.
- Remove duplicate copies between platforms.

Each item shows its savings and what it costs in freshness, so stakeholders choose knowingly ([§13](13-performance-cost.md)).

</details>

<details>
<summary><b>Q: How do you handle a source system with no CDC capability?</b></summary>

First I ask for log access or native change tracking. Otherwise I use a high-watermark extract on a reliable `updated_at`, with an overlap window and dedup by key. That misses hard deletes, so I add periodic full snapshots and diff row hashes to detect them. Longer term, I push for an outbox or event emission in the application. Every extract is idempotent (P6) and reconciled against source counts.

</details>

<details>
<summary><b>Q: How do you get exactly-once results from a streaming pipeline?</b></summary>

I aim for effectively-once. Producers use idempotent writes and transactions. The processor checkpoints offsets and state together, so replay is safe. Sinks are idempotent: a MERGE on a natural key, a Delta sink that skips replayed batches, or a transactional commit. External side effects get idempotency keys.

</details>

<details>
<summary><b>Q: How do you handle late data when the same events feed real-time dashboards and finance?</b></summary>

The real-time path uses event-time windows and a watermark, and its numbers are labelled provisional. The batch path MERGEs late rows and recomputes a lookback window, and it's the system of record. Finance closes at a cutoff, and later corrections post as adjustments. The two paths are reconciled daily to catch logic drift.

</details>

## 17. Leadership and Behavioral (Architect / Co-Lead)

<details>
<summary><b>Q: How do you run a design review?</b></summary>

A short pre-read goes out 24–48 hours ahead, with explicit questions, a deadline, and a named decision owner. The meeting covers the open questions and one-way-door risks and parks style. It closes with decide-and-commit, captured as an ADR with the dissent and a revisit trigger.

</details>

<details>
<summary><b>Q: Can you tell me about a technical disagreement (structure of a strong answer)?</b></summary>

Use STAR+L, with the Action showing method rather than winning:
- **Situation:** "two viable options for [X]."
- **Action:** "we agreed on the criteria first, I proposed a two-day spike on real data, and the results favored option B on cost but A on operability."
- **Result:** "we chose B with a revisit trigger, and the trigger never fired."
- **Learning:** "agree on criteria before debating options."

Show respect for the other side's position, and show that you'd commit even if you lost.

</details>

<details>
<summary><b>Q: How do you say no to a stakeholder?</b></summary>

I say no to the solution and yes to the need. I ask which decision the data drives and what staleness actually costs. Then I present two or three options with freshness, cost, and risk, and recommend one. For example: "Hourly gets you 95% of the value at a third of the cost of streaming; if the hourly numbers show we're missing fraud, we escalate to streaming." The outcome goes in writing.

</details>

<details>
<summary><b>Q: How would you split responsibilities with a co-lead?</b></summary>

One of us owns architecture and technical direction, the other owns delivery and people, documented in a RACI per decision type. We hold a weekly sync, present one voice, resolve disagreements privately with an agreed tie-break, and review the split quarterly. We name the overlaps (hiring, incident command, roadmap) explicitly.

</details>

<details>
<summary><b>Q: How do you prioritize tech debt vs features?</b></summary>

I translate debt into risk, cost, and velocity metrics, so it's ranked in the same backlog as features. I keep a standing 15–20% capacity budget plus a fix-on-touch norm, and I leave debt alone in stable areas nobody touches. Debt that blocks a roadmap item gets bundled into that item's estimate.

</details>

<details>
<summary><b>Q: How do you set standards without becoming a bottleneck?</b></summary>

Paved roads plus automation (templates, CI checks, policy-as-code) make the standard the default. Human review is reserved for one-way doors, and ADRs let people self-serve the reasoning. A review queue forming behind me means I'm gatekeeping and need to automate or delegate.

</details>

<details>
<summary><b>Q: What would you do in your first 90 days?</b></summary>

- **Days 1–30, listen:** meet stakeholders and the team, map systems, costs, incidents, and pain points. Don't change anything structural.
- **Days 31–60, small wins:** fix one visible pain point, write the first ADRs for decisions already made implicitly, and agree on the co-lead split.
- **Days 61–90, direction:** a written platform strategy with a prioritized roadmap, standards, and metrics, reviewed with the team and stakeholders.

</details>

<details>
<summary><b>Q: Can you tell me about a time you were wrong?</b></summary>

Pick a real, consequential mistake, not a disguised strength. Keep the Situation short, and put the weight on how you detected it and how fast you owned and fixed it. The Learning is a changed habit, for example "I now benchmark on production-shaped data before committing."

</details>

<details>
<summary><b>Q: How do you grow engineers on the team?</b></summary>

Delegate outcomes with clear SLAs and step back as people show ownership. Pair on design docs instead of rewriting them, and rotate on-call and incident command. Success is people owning areas I used to own.

</details>

## 18. AI on the Data Platform

<details>
<summary><b>Q: How would you build RAG over governed enterprise data?</b></summary>

Parse into structured chunks carrying metadata and ACL attributes, redact PII, then embed into a hybrid index synced at the freshness the use case needs. At query time, a mandatory filter derived from the authenticated identity, or a per-audience index, ensures retrieval returns only what the user could read at the source. Answers cite chunks, and a versioned eval set gates every change.

</details>

<details>
<summary><b>Q: Feature store: why, and what's point-in-time correctness?</b></summary>

It gives one feature definition materialized offline for training and online for serving, which removes training-serving skew. Point-in-time correctness means each training row gets feature values as of the label's timestamp (an AS-OF join). A naive key join leaks the future and inflates offline metrics.

</details>

<details>
<summary><b>Q: Cortex vs Mosaic AI?</b></summary>

Cortex is SQL-first and managed: AI functions, Cortex Search, Analyst, and Agents run next to warehouse data under Snowflake RBAC, with little infrastructure to own (P10). Databricks offers more control: custom model serving, a gateway across providers, MLflow 3 evaluation, and agents and indexes governed in Unity Catalog. Pick by where the data and the team already are, and how much model customization you need. Feature status changes monthly, so verify it.

</details>

<details>
<summary><b>Q: How do you keep a vector index fresh and secure?</b></summary>

Drive it from the governed source table with incremental sync, monitor end-to-end lag against an SLA, and treat embedding-model changes as versioned backfills. Because indexes don't inherit per-user row policies by default, partition by audience or enforce server-side ACL filters, redact before embedding, and propagate deletions.

</details>

<details>
<summary><b>Q: What does 'AI-ready data' mean concretely?</b></summary>

Parsed, deduplicated, versioned content with lineage, classified and redacted before any model sees it. Add descriptive metadata and a semantic layer, freshness SLAs with owners, and versioned eval sets. It's BI's data-product discipline applied to documents and agents.

</details>

<details>
<summary><b>Q: How do you control cost of LLM inference over large tables?</b></summary>

Filter with cheap SQL first, dedup, and send minimal tokens to the smallest model that passes eval. Process only new or changed rows (cache by input hash), price a sample before the full run, set budgets, and materialize outputs.

</details>

<details>
<summary><b>Q: Why do text-to-SQL agents need a semantic layer?</b></summary>

Raw schemas don't encode grain, join paths, or which "revenue" is official, so the LLM guesses plausibly and wrongly. A semantic layer gives it governed metrics and verified queries.

</details>

<details>
<summary><b>Q: How do you evaluate a RAG system?</b></summary>

Score retrieval (recall@k, MRR against labeled sources) separately from generation (groundedness, correctness, citations) using human-calibrated LLM judges. Run it in CI on every chunking, embedding, prompt, or model change, and feed production traces back into the eval set.

</details>

<details>
<summary><b>Q: What changes when agents reach the platform through MCP?</b></summary>

The agent becomes a principal that can query and act. Use on-behalf-of-user auth with least privilege, audit every tool call, require approval for write tools because of prompt injection, and enforce rate and cost limits in a gateway.

</details>
