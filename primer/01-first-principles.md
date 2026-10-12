# 01. First Principles: Why Data Engineering Exists

> As of October 2026. Tags: **(GA)** generally available · **(Preview)** public/private preview · **(⚠ verify)** not confirmed against official docs.

## Why this exists

Every organization has the same structural problem. The systems that *run* the business record one event at a time, safely and quickly: an order, a payment, a timesheet. The people and models that *understand* the business ask questions across millions of those events at once. The two workloads want opposite physical designs, so no single copy of the data serves both well.

**Data engineering moves data from where it is cheap to write to where it is cheap to read.** Along the way it reshapes, reconciles, and governs the data, and it has to do this reliably, repeatedly, and at a cost the business accepts.

Every technology in this primer is one answer to that problem under specific constraints: warehouses, lakes, lakehouses, Kafka, Snowflake, Databricks. Learn the constraints and you can derive most of the answers instead of memorizing them. The rest of the primer refers back to the principles below by ID, e.g. (P3).

## Core concepts

### The four forces

Every design decision in data engineering is a negotiation between four forces:

| Force | What it means | Why it matters |
|---|---|---|
| **Physics** | Memory, local disk, network, and object storage differ in latency by orders of magnitude. Sequential I/O beats random I/O. | **Bytes moved dominate cost and time.** Most performance work is reading fewer bytes or moving them less. |
| **Economics** | In the cloud, storage is cheap (object storage costs roughly $20–25 per TB-month; know the order of magnitude), compute is expensive and billed by time, and people are the most expensive of all. | Designs that trade cheap storage for less compute or fewer people usually win. Idle compute is pure waste. |
| **Correctness** | Distributed systems fail partially: a node dies, a network call times out, a job half-finishes. | Every step must survive being retried, and must not silently produce wrong numbers. |
| **Change** | Sources, schemas, business questions, regulations, and teams all change constantly. | Architecture is mostly about making change cheap and safe. |

### The principles

Each principle below is written as: **statement → mechanism (why it's true) → where it shows up → what breaks when you ignore it.**

#### P1. Write-optimized vs. read-optimized

- **Statement:** Data is written where writes are cheap and read where reads are cheap. Moving and reshaping data between the two is the core job.
- **Mechanism:**
  - An OLTP database stores a row's fields together, so one `INSERT` or `UPDATE` touches one page. B-tree indexes make point lookups fast, and normalization avoids update anomalies.
  - An analytical query reads a few columns across millions of rows. Columnar storage reads only those columns, compresses runs of similar values (dictionary, run-length encoding), and processes them in CPU-friendly vectors.
  - The data is the same but the layouts are opposite.
- **Shows up in:** ETL/ELT, CDC, the very existence of warehouses, and HTAP and "zero-ETL" attempts to collapse the split (Databricks Lakebase, Snowflake hybrid tables and Snowflake Postgres).
- **Ignore it:** you run dashboards against the production database, and checkout slows down every Monday at 9 a.m.

#### P2. Separate storage from compute

- **Statement:** Keep data in cheap, durable object storage and rent compute only when needed. Each then scales, and is isolated, independently.
- **Mechanism:**
  - In a shared-nothing MPP warehouse or a Hadoop cluster, data lives on the compute nodes' disks. Adding compute means adding disks and rebalancing data. Idle nodes still cost money, and every workload competes for the same machines.
  - Object storage (S3, ADLS, GCS) is extremely durable, effectively infinite, and cheap per byte. Its weakness is high latency per request.
  - Decoupling lets many independent compute clusters read the same data, scale to zero, and avoid stepping on each other.
  - The cost is network reads. This is why decoupled engines depend on local SSD caches and on aggressive pruning (P4).
- **Shows up in:** Snowflake virtual warehouses, Databricks clusters and SQL warehouses, Kafka tiered storage, and "diskless" Kafka designs.
- **Ignore it:** you pay for peak capacity 24/7, and the ETL job and the CEO's dashboard fight over the same cluster.

#### P3. Immutable files + a metadata log = transactions on object storage

- **Statement:** Object stores don't support in-place updates or atomic multi-file operations. So never modify files: write new ones, then commit by atomically publishing a new version of the metadata that says which files make up the table.
- **Mechanism:**
  - The atomic step is tiny:
    - Delta writes the next numbered log entry, and only one writer can create it.
    - Iceberg's catalog does a compare-and-swap of the current-metadata pointer.
    - Snowflake's cloud services layer commits micro-partition metadata.
  - Readers see either the old snapshot or the new one, never a half-written state. This is snapshot isolation.
- **Consequences you get for free:**
  - **Time travel:** old snapshots still point at old files.
  - **Zero-copy clones:** a new table can point at the same files.
  - **Lock-free concurrent readers.**
  - **Cheap rollback.**
- **Costs:**
  - Updates rewrite whole files (write amplification). Deletion vectors and merge-on-read soften this.
  - Old files pile up until vacuum or retention removes them.
  - Frequent small commits create small files, which need compaction.
- **Shows up in:** Delta, Iceberg, Hudi, Snowflake micro-partitions ([§04](04-table-formats-catalogs.md), [§05](05-snowflake-architecture.md)).

#### P4. Skip work instead of doing it faster

- **Statement:** The fastest byte is the one you never read.
- **Mechanism:**
  - Query cost is roughly proportional to bytes scanned and shuffled.
  - Engines keep per-file or per-partition statistics (min/max, null counts). If a filter can't match a file's value range, the file is skipped.
  - Clustering keeps those ranges narrow, so skipping actually happens.
  - Caching avoids re-reading data. Predicate pushdown filters data at the source.
- **Shows up in:** partition pruning, micro-partition pruning, Z-order and liquid clustering, Snowflake's search optimization, result caches.
- **Ignore it:** data is randomly ordered on the filter column, so every file's min/max spans the whole domain and nothing gets pruned. Wrapping the filter column in a function has the same effect: the engine can't skip.

#### P5. The log is the source of truth

- **Statement:** A table is a snapshot. A log is the ordered history of changes that produced it. Keep the log and you can rebuild any state, derive new views, and add consumers without asking the producer.
- **Mechanism:**
  - Databases already work this way internally: the write-ahead log (WAL) is the truth, and tables are a cache of it.
  - CDC exposes that log. Kafka turns it into a shared, replayable service. The Delta log applies the same idea to files.
  - This is **stream–table duality**: replaying a stream produces a table, and capturing a table's changes produces a stream.
- **Shows up in:** Kafka, Debezium/CDC, the Delta log, Kappa architecture, event sourcing, and the "raw, replayable" bronze layer.
- **Costs:** retention storage. Ordering is guaranteed only within a partition or key. Compaction is needed if you only want the latest state per key.

#### P6. Idempotent and replayable steps

- **Statement:** Design every step so that running it twice gives the same result as running it once.
- **Mechanism:**
  - A distributed system can't tell "failed" from "slow", so the only safe recovery is to retry.
  - Retries create duplicates unless the step is idempotent.
  - Techniques:
    - deterministic outputs keyed by business keys
    - `MERGE`/upsert instead of blind append
    - overwrite-by-partition
    - committing the processed offset atomically with the output
    - dedup keys
- **The key insight:** "exactly-once" is really at-least-once delivery plus an idempotent effect.
- **Shows up in:** backfills, streaming checkpoints, Snowpipe's file-level dedup, Kafka's idempotent producer, `MERGE`-based CDC ([§09](09-batch-pipelines.md), [§11](11-stream-processing.md)).
- **Ignore it:** a retried job double-counts revenue, and nobody notices until finance does.

#### P7. Latency is a cost dial

- **Statement:** Fresher data costs more, so buy only the freshness the decision actually needs.
- **Mechanism:** lower latency requires:
  - compute that is running when data arrives (always-on)
  - state held across events (memory, checkpoints)
  - more frequent, smaller writes (small files)
  - harder failure handling and more on-call burden

  Cost rises steeply as the latency target drops: daily → hourly → minutes → seconds → sub-second.
- **The architect's question:** "What decision consumes this data, and how fast is that decision made?" A dashboard that a human checks each morning doesn't need a streaming pipeline.
- **Shows up in:** batch vs. micro-batch vs. streaming, Snowflake Dynamic Tables' target lag, Spark's `availableNow` trigger ("streaming code, batch cost").

#### P8. Schema is a contract

- **Statement:** Every consumer encodes assumptions about field names, types, and meanings. Decide where the schema is enforced and who owns changes to it.
- **Mechanism:**
  - *Schema-on-write* (warehouse style) rejects bad data early, at the cost of rigidity.
  - *Schema-on-read* (lake style) accepts anything. Every reader then re-implements parsing, and breaks silently when the data changes.
  - Most pipeline incidents trace back to a producer change that consumers didn't know about.
- **Remedy:** make compatibility explicit with schema registries and compatibility modes, data contracts, and expand/contract migrations.
- **Shows up in:** Kafka Schema Registry ([§10](10-kafka.md)), Delta schema enforcement and evolution, data contracts ([§12](12-governance-security.md)), SCDs, which manage *meaning* over time ([§03](03-data-modeling.md)).

#### P9. Govern the data, not the tool

- **Statement:** Attach access policies, classifications, and lineage to the data objects in a catalog, not to each tool that touches them.
- **Mechanism:**
  - If rules live in each BI tool, notebook, and engine, then N tools means N copies of the policy, and those copies drift.
  - Policies attached to data are enforced by every engine that goes through the catalog. Example: a `PII` tag drives a masking policy.
  - Audit and lineage come from the same choke point.
- **Shows up in:** Unity Catalog, Snowflake Horizon, tag-based masking, ABAC ([§12](12-governance-security.md)).
- **Ignore it:** someone reads the files directly from storage and bypasses everything. This is why catalogs now vend short-lived credentials instead of granting raw bucket access.

#### P10. Abstraction trades control for operability

- **Statement:** Every managed layer removes knobs, and with them the work of operating those knobs. You gain speed and reliability. You lose the ability to tune edge cases and, often, the ability to leave.
- **How to choose:** by team skills, by how diverse the workloads are, by whether you run at a scale where knobs pay off, and by whether the capability differentiates your business.
- **Shows up in:** Snowflake (managed, SQL-first) vs. Databricks (configurable, code-first), though both are converging on serverless. Also Confluent Cloud vs. self-managed Kafka, and Fivetran vs. custom ingestion.
- **Rule of thumb:** buy the commodity, build the differentiator.

#### P11. Open formats decouple data from engines

- **Statement:** Data stored in an open table format, in your own storage, can be read and written by any compliant engine. Adding or switching engines doesn't require migrating data.
- **Mechanism and cost:**
  - An open format is a lowest common denominator. A proprietary format can co-design storage and engine; Snowflake's micro-partitions are the example.
  - New features land unevenly across engines.
  - Lock-in moves up a layer: **whoever owns the catalog** (the pointer to the current snapshot, plus the policies) controls the table.
- **Shows up in:** Delta, Iceberg, Hudi, UniForm, the Iceberg REST catalog, Unity Catalog OSS, Apache Polaris / Snowflake Open Catalog ([§04](04-table-formats-catalogs.md)).

### How the principles combine: four historical bets

| Era | The bet | Principles embraced | Principles sacrificed | What broke |
|---|---|---|---|---|
| **Data warehouse** (1990s, e.g. Teradata) | A separate, curated analytical copy with strict schemas and SQL | P1, P8 (schema-on-write) | P2 (storage and compute coupled in appliances) | Expensive, hard to scale, rigid. Couldn't hold semi-structured or ML data. |
| **Data lake** (2010s, Hadoop/S3) | Dump everything cheaply and decide structure at read time | Cheap storage, flexibility | P3 (no transactions), P8 (no enforcement), P9 | "Data swamp": no ACID, poor BI performance, no governance. Most firms kept a warehouse copy as well (the two-copy problem). |
| **Cloud data warehouse** (Snowflake, founded 2012) | Keep warehouse semantics but rebuild them on cloud object storage, fully managed | P2 (multi-cluster, shared data), P3 internally (immutable micro-partitions), P10 managed side | P11 at first (proprietary storage format), now narrowed by Iceberg tables | Gets elastic, isolated, near-zero-ops SQL, plus sharing, time travel, and cloning. Cost: data lived in a proprietary format, and Python/ML was historically second-class. |
| **Lakehouse** (Databricks: Spark 2009–13, Delta open-sourced 2019) | Start from open files and a general-purpose engine, then add warehouse guarantees through an open table format | P3 via Delta, P11, P2, P10 control side | Historically P10 managed side (more knobs, more engineering needed), now narrowed by serverless | Gets one copy for BI, ML, and streaming on open files. Cost: historically more to operate and tune. |

**Why Snowflake, in one sentence of principles:** when the dominant workload is SQL analytics with many concurrent users, and the team values managed simplicity and per-workload isolation (P10 managed side, P2), Snowflake's bet fits directly.

**Why Databricks, in one sentence of principles:** when the workloads are diverse (engineering, streaming, ML, AI), teams are code-first, and open formats are a requirement (P11, P10 control side), Databricks' bet fits directly.

**Convergence in 2026:**
- Snowflake now reads and writes Iceberg and runs Python and containers.
- Databricks now offers serverless SQL warehouses and managed optimization.
- Both have added OLTP Postgres offerings, which attack P1 itself.

What still differs: defaults, ergonomics, ecosystem, pricing mechanics, and where governance lives. Full comparison in [§08](08-snowflake-vs-databricks.md).

### Using the principles in an interview

1. **Answer pattern: mechanism → consequence → tradeoff → when not.**
   - Question: "Why can Snowflake clone a 50 TB table in seconds?"
   - Mechanism: micro-partitions are immutable (P3), so a clone only copies metadata pointers.
   - Consequence: no extra storage until either side changes.
   - Tradeoff: storage grows as the two tables diverge, and governance, grants, and cost attribution need care.
2. **When you don't know a specific feature, reason out loud from principles and label the assumption.** For example: "I haven't run Snowflake in production, but since storage is decoupled and partitions are immutable, I'd expect sharing to be metadata-only. The Databricks equivalent is OpenSharing (formerly Delta Sharing), which differs because it's an open protocol." This is honest, and it shows the depth interviewers are actually probing for.
3. **Name the principle being traded.** Almost every "why X over Y" question ([§15](15-why-x-over-y.md)) is really asking which principle the organization values more in that context.

## Comparison

How each principle shows up in each system:

| Principle | Snowflake | Databricks | Kafka |
|---|---|---|---|
| P1 Write vs. read optimized | Columnar micro-partitions; hybrid tables / Snowflake Postgres for OLTP | Delta/Parquet columnar; Lakebase (Postgres) for OLTP | Append-only log: write-optimized, sequential |
| P2 Separate storage/compute | Virtual warehouses over shared storage | Clusters / SQL warehouses over cloud storage | Tiered storage; diskless designs emerging |
| P3 Immutable + metadata log | Micro-partitions + cloud services metadata → Time Travel, cloning | Delta log → time travel, shallow/deep clone | Immutable log segments |
| P4 Skip work | Pruning, clustering keys, search optimization, result cache | Data skipping, liquid clustering, disk cache, Photon | Consumers read only their partitions and offsets |
| P5 Log is truth | Streams (change offsets) on tables | Change Data Feed, the Delta log | The whole product |
| P6 Idempotency | Snowpipe file dedup, `MERGE`, stream offsets consumed in a transaction | Checkpoints + idempotent Delta sinks, `MERGE` | Idempotent producer, transactions |
| P7 Latency dial | Dynamic Tables target lag, Snowpipe Streaming | Trigger modes, streaming tables | Linger/batching; consumer poll cadence |
| P8 Schema contract | Schema-on-write; schema evolution options | Schema enforcement and evolution | Schema Registry compatibility modes |
| P9 Govern data | Horizon: tags, masking, row access policies | Unity Catalog: tags, row filters, column masks, ABAC | ACLs; governance mostly external |
| P10 Control vs. operability | Managed by default | Configurable by default, serverless option | Self-managed vs. Confluent/MSK/Event Hubs |
| P11 Open formats | Iceberg tables; Horizon Catalog Iceberg REST endpoint (Polaris-based) | Delta, UniForm, managed Iceberg, UC OSS | Open protocol; Tableflow-style topic→table |

## Interview questions

**Q: Why does data engineering exist as a discipline? What is a data platform's core job?**

Operational systems are built for single-row writes and analytical consumers need large scans, and those workloads want opposite physical layouts (P1). So data has to be moved, reshaped, reconciled, and governed between them. The hard part isn't the moving; it's doing it correctly under partial failure (P6), at an acceptable freshness and cost (P7), while sources and schemas keep changing (P8). A platform's job is to make that repeatable for many teams, so each pipeline isn't a bespoke snowflake (lowercase).

**Q: Why not just run analytics on the production database or a read replica?**

Row-oriented OLTP storage makes large scans expensive, and analytical queries compete with transactions for the same resources. A read replica removes the contention but not the layout mismatch. It also does nothing for history (OLTP overwrites state), integration across sources, or governed access. A replica is reasonable for small-scale operational reporting; beyond that you want a columnar copy fed by CDC.

**Q: Why separate storage and compute, and what does it cost you?**

Decoupling lets you size and bill compute per workload, scale it to zero, and run many isolated clusters over one copy of the data (P2). That eliminates both the "resize means redistribute data" problem and contention between workloads. The cost is that object storage reads are high-latency network calls. Performance therefore depends on caching, metadata, and pruning (P4), and a cold cache after scale-to-zero is a real tradeoff.

**Q: How do you get ACID transactions on object storage that doesn't support them?**

Don't modify files. Write new immutable files, then commit by atomically publishing a new metadata version that lists the table's files (P3). Delta does this with an exclusive put of the next log entry; Iceberg does it with a compare-and-swap of the catalog's metadata pointer. Readers always see a consistent snapshot, and concurrent writers conflict-check at commit time (optimistic concurrency). Time travel and cheap clones are free side effects. Write amplification and small files are the price.

**Q: What does "exactly-once" really mean?**

No system can guarantee that a message is *delivered* exactly once across failures. What you can guarantee is that its *effect* is applied exactly once. That takes a replayable source (P5), state and offsets checkpointed together, and an idempotent or transactional sink (P6). Anything with external side effects, such as sending an email or calling an API, breaks the guarantee unless it is made idempotent too. Details are in [§11](11-stream-processing.md).

**Q: Schema-on-read vs. schema-on-write: which do you prefer?**

Both, at different layers. Land raw data schema-on-read so you can always replay from it (P5). Enforce schema at the boundary where data becomes a product others depend on (P8). Pure schema-on-read pushes parsing and breakage onto every consumer. Pure schema-on-write forces you to reject or lose data whenever a producer changes something.

**Q: How do you decide how fresh the data needs to be?**

Ask what decision consumes the data and how quickly that decision gets made, then price each latency tier (P7). Fraud blocking needs seconds. Inventory replenishment needs minutes to hours. Finance close needs correctness more than speed. Often the right answer is incremental batch on a schedule using the streaming code path, which can be tightened later without a rewrite.

**Q: Open format vs. proprietary format: does it still matter?**

Less than it used to, and in a different place. Snowflake supports Iceberg and Databricks supports Iceberg alongside Delta, so the file format is rarely the lock-in now. The catalog is: whoever holds the current-snapshot pointer and the policies controls access and interoperability (P11). A proprietary format can still win on co-designed optimization. The architect's question is: "Which engines need to write this table, and who is the catalog of record?"

**Q: In one breath each: why Snowflake, why Databricks?**

Snowflake: managed, SQL-first, strong workload isolation and sharing. It's the shortest path to governed, concurrent analytics for SQL-skilled teams (P10 managed side, P2). Databricks: one open copy of the data serving engineering, streaming, ML, and AI with code-first control (P11, P10 control side). They're converging, so the real decision is about team skills, the workload mix, and where governance lives ([§08](08-snowflake-vs-databricks.md)).

**Q: What's the most common root cause of data incidents?**

Unannounced upstream change: a renamed column, a changed meaning, a new enum value, or a source that silently stopped updating (P8). Next most common are non-idempotent reruns that duplicate data (P6) and incremental loads with flawed watermarks that silently skip rows ([§09](09-batch-pipelines.md)). The defenses are contracts at the producer boundary, freshness and volume checks, and steps designed to be replayed.

## Takeaways

- Data engineering exists because write-optimized and read-optimized layouts are opposites (P1).
- Most performance work is reading fewer bytes (P4), and most reliability work is making retries safe (P6).
- Immutable files plus atomic metadata commits explain ACID, time travel, and cloning across Delta, Iceberg, and Snowflake (P3).
- Treat the log as truth and you can always rebuild state (P5).
- Freshness is a purchase, not a default (P7).
- Schema is a contract, and unannounced change is the top incident cause (P8).
- Put governance in the catalog, not in the tools (P9). The catalog is also the new lock-in point (P11).
- Snowflake and Databricks are two bets on the same principles that are converging. Compare them by team, workload, and governance, not by feature lists (P10).
- In interviews, use mechanism → consequence → tradeoff → when not. When unsure, reason from principles and label the assumption.

## Sources

- Armbrust et al., "Lakehouse: A New Generation of Open Platforms that Unify Data Warehousing and Advanced Analytics," CIDR 2021 — https://www.cidrdb.org/cidr2021/papers/cidr2021_paper17.pdf
- Armbrust et al., "Delta Lake: High-Performance ACID Table Storage over Cloud Object Stores," VLDB 2020 — https://www.vldb.org/pvldb/vol13/p3411-armbrust.pdf
- Dageville et al., "The Snowflake Elastic Data Warehouse," SIGMOD 2016 — https://dl.acm.org/doi/10.1145/2882903.2903741
- Kreps, "The Log: What every software engineer should know about real-time data's unifying abstraction," 2013 — https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying
- Akidau et al., "The Dataflow Model," VLDB 2015 — https://www.vldb.org/pvldb/vol8/p1792-Akidau.pdf
- Kleppmann, *Designing Data-Intensive Applications* (O'Reilly) — background for P1, P5, P6, P8
