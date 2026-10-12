# 04. Open Table Formats and Catalogs

> As of October 2026. Tags: **(GA)** generally available · **(Preview)** public/private preview · **(⚠ verify)** not confirmed against official docs.

## Why this exists

A data lake is a directory of files on object storage. Files alone give you none of the guarantees a table needs:

- **No atomicity.** A job that writes 200 files and dies after 120 leaves a half-written "table" that readers will happily scan.
- **No isolation.** A reader listing the directory mid-write sees a mix of old and new files.
- **No schema enforcement.** Any writer can drop a file with a different schema into the folder (P8).
- **Expensive discovery.** Query planning starts with LIST calls over prefixes. Those are slow, paginated, and historically only eventually consistent.
- **No safe UPDATE/DELETE.** Object storage can't modify a file in place. You can only replace whole objects.

A **table format** adds a metadata layer that defines exactly which files make up version *N* of the table. It never edits data files. It writes new files and then atomically publishes a new metadata version (P3). Atomic commits, snapshot isolation, time travel, schema enforcement and file-level statistics for pruning (P4) all follow from that one move.

Formats also need to be **open** (P11). If the metadata spec is public, many engines (Spark, Trino, Flink, Snowflake, DuckDB) can read and write the same copy, and you avoid one ETL copy per engine. The bet is that you give up some vendor-specific optimization in exchange for a single copy and freedom to switch engines. The lock-in doesn't go away. It moves into the **catalog** (see below).

## Core concepts

### Parquet: the common substrate

Parquet is a columnar file format whose design comes from Google's Dremel paper. All three table formats use it as the default data file.

```
PAR1
 Row group 1 ─┬─ column chunk A ─ pages (dictionary page, data pages)
              ├─ column chunk B ─ pages
              └─ ...
 Row group 2 ─ ...
 Footer (FileMetaData: schema, row-group locations, per-chunk min/max/null counts)
 4-byte footer length · PAR1
```

- **Row group:** a horizontal slice of rows; the unit of parallelism and of min/max skipping.
- **Column chunk:** one column's values within a row group, stored contiguously. A query needing 3 of 100 columns range-reads only those chunks (P1, P4).
- **Page:** the unit of encoding (dictionary, RLE/bit-packing, delta) and compression (Snappy, ZSTD). Same-typed neighbors compress far better than mixed rows.
- **Footer last:** allows single-pass writes. Readers fetch the footer first, then only the chunks they need.

Parquet knows nothing about other files, versions or transactions. That gap is what table formats fill.

### Delta Lake: an ordered transaction log

```
_delta_log/
  000...000.json        protocol, metaData, add(file, stats)
  000...001.json        add / remove / commitInfo
  ...
  000...010.checkpoint.parquet   full replayed state at v10
  _last_checkpoint
```

- **State = replay.** Each commit is a JSON file of actions (`add`, `remove`, `metaData`, `protocol`). The current table is the latest checkpoint plus every JSON commit after it. The protocol states that "Delta files are the unit of atomicity."
- **Atomic commit = create `N+1.json` only if it doesn't already exist.** This requires put-if-absent or mutual exclusion from storage. S3 historically lacked put-if-absent, so multi-cluster OSS writes needed a DynamoDB-backed LogStore (`S3DynamoDBLogStore`, still marked experimental) or a commit service.
- **Optimistic concurrency.** A writer reads version N, writes its data files (invisible until committed), then tries to create N+1. If another writer got there first, it checks whether the winning commits conflict with what it read (for example, a file it read was removed). If they don't conflict, it retries as N+2. If they do, it fails.
- **Checkpoints.** A Parquet snapshot of the replayed state, written every 10 commits by default in OSS, so readers don't replay thousands of JSON files. V2 checkpoints add sidecar files, and optional log compaction files help with very large logs.
- **Catalog-managed tables (Preview in Delta 4.0, production-supported since Delta 4.1).** The catalog, rather than the filesystem, becomes "the source of truth for whether a given commit attempt succeeded." This is Delta moving toward the Iceberg model of catalog arbitration.
- **Table features** (deletion vectors, column mapping, row tracking, variant, type widening, icebergCompat) gate which clients may read or write. Enabling one can lock out older readers.

### Apache Iceberg: a metadata tree with a catalog pointer

```
Catalog: db.orders ─► metadata/00042-<uuid>.metadata.json   ← the ONLY mutable thing
                         schemas[], partition-specs[], snapshots[], current-snapshot-id
                              │
                              ▼
                      snap-<id>.avro  (manifest list: one per snapshot, partition ranges per manifest)
                              ▼
                      manifest-*.avro (data/delete files + per-file column stats)
                              ▼
                      data/*.parquet · v3 deletion vectors (Puffin)
```

- **Commit = compare-and-swap in the catalog.** The writer writes data files, manifests, a manifest list and a new `metadata.json`. None of it is visible yet. It then asks the catalog to move the pointer from 00042 to 00043 *only if the pointer is still 00042*. In the spec's words: "An atomic swap of one table metadata file for another provides the basis for serializable isolation." The loser re-reads, re-validates and retries. The atomicity lives in the catalog, not in the file system.
- **Planning without LIST.** The manifest list holds partition ranges per manifest and each manifest holds per-file stats. The planner prunes at two levels before it opens any data file (P4). Unchanged manifests are reused across snapshots, so a commit writes metadata proportional to the change, not to the table.
- **Hidden partitioning.** A partition is a *transform* of a column (`day(ts)`, `bucket(16, id)`, `truncate`). Users filter on `ts`, and Iceberg derives the partition filter itself, so a query can't silently miss pruning by forgetting a `ts_date` column.
- **Partition evolution.** Each manifest records the spec it was written with, and pruning uses that spec. You can change `day` to `hour` without rewriting old data.
- **Schema evolution by field ID.** Columns are tracked by ID, not name or position. Rename, drop and reorder are metadata-only and safe.
- **Format versions.** v1 handles analytic tables. v2 adds row-level deletes (position and equality delete files). **v3 (GA in both Snowflake and Databricks as of May 2026)** adds deletion vectors, row lineage (`_row_id`, `_last_updated_sequence_number`), `variant`, geometry/geography, nanosecond timestamps, default values, multi-argument transforms and encryption keys. **v4** is still under development.

### Apache Hudi: a timeline built for upserts

Hudi ("Hadoop Upserts Deletes and Incrementals") came out of Uber. The problem was applying a continuous stream of database changes (CDC) to a huge lake table without rewriting it on every batch.

- **Timeline.** `.hoodie/` holds *instants* (`<time>.<action>.<state>`, moving from requested to inflight to completed) for commits, delta commits, compactions, cleans and rollbacks. A write is visible once its instant completes. Hudi 1.0 moved the timeline to an LSM layout and added **non-blocking concurrency control** for multiple concurrent streaming writers.
- **Record keys and indexes.** Every row has a key. An index (bloom, bucket, or the **record-level index** in the metadata table) maps key to file group, so an upsert touches only the files that hold those keys. This is Hudi's core differentiator. Hudi 1.0 adds secondary indexes and expression indexes, and 1.1 adds a partitioned record index.
- **Table types.** *Copy-on-write*: an update rewrites the base Parquet file. *Merge-on-read*: an update appends to row-based log files next to the base file, readers merge them, and async **compaction** folds the logs into a new base file.
- **Incremental queries.** "Give me rows changed since instant T" is a first-class read, which makes downstream pipelines cheap (P5).

### Copy-on-write, merge-on-read, deletion vectors

Files are immutable (P3), so changing one row means picking who pays:

| Strategy | Write cost | Read cost | Needs |
|---|---|---|---|
| Copy-on-write | Rewrite every touched file (write amplification) | None extra | Nothing |
| Merge-on-read (delta/log files) | Append small delta | Merge at read | Compaction |
| Deletion vector | Write a small bitmap of deleted row positions + new rows elsewhere | Filter by bitmap (cheap) | Periodic purge/rewrite |

Where each format stands:

- **Delta:** copy-on-write by default. Deletion vectors (RoaringBitmap, stored inline or in side files) turn DELETE, UPDATE and MERGE into merge-on-read.
- **Iceberg:** v2 uses position and equality delete files. In v3, "position delete files must not be added," and you get at most one deletion vector per data file, stored in Puffin.
- **Hudi:** COW or MOR table types, chosen per table.

Rule of thumb: frequent small updates favor MOR or DVs. Read-heavy tables with rare bulk rewrites favor COW. Merge-on-read without compaction degrades reads steadily.

### Time travel and cleanup

Time travel is free because old files and metadata stay on storage (P3), and it is bounded only by cleanup: Delta `VACUUM` (default file retention 7 days, log retention 30 days), Iceberg `expire_snapshots` / `remove_orphan_files`, Hudi cleaning and archival. **Small files** from streaming and frequent commits inflate metadata and per-file overhead, so compaction (`OPTIMIZE`, `rewrite_data_files`, Hudi compaction/clustering) is routine maintenance, not an extra.

### Catalogs: the pointer and the policy

A catalog maps `catalog.schema.table` to the current metadata location, arbitrates commits (the Iceberg CAS, Delta catalog-managed commits), and increasingly holds governance: grants, row and column policies, lineage, audit, and **credential vending** (short-lived storage credentials scoped to one table, so engines never hold bucket-wide keys). Whoever holds the pointer decides what "current" means. Whoever holds the policy decides who sees it (P9).

- **Hive Metastore (HMS).** The Hadoop-era default: a Thrift service over an RDBMS that maps tables to directories and partitions to sub-directories. It tracks partitions, not files, so it can't make a multi-file write atomic. It still survives as a compatibility API.
- **Iceberg REST Catalog spec.** An OpenAPI HTTP protocol and today's interop standard. `updateTable` sends *requirements* (for example `assert-ref-snapshot-id`) plus *updates*, so the server performs the CAS. `commitTransaction` commits multiple tables atomically. The spec also covers credential vending and server-side scan planning (`planTableScan`). Any engine that speaks REST can work with any catalog that implements it.
- **Unity Catalog.** Databricks' governance catalog. It exposes an Iceberg REST endpoint for managed Iceberg tables and for Delta tables with UniForm. **Unity Catalog OSS** is Apache-2.0, an LF AI & Data sandbox project, and compatible with the HMS and Iceberg REST APIs. The OSS version has fewer features than the managed one.
- **Apache Polaris.** An open-source Iceberg REST catalog, co-created by Snowflake and Dremio. **It graduated to an Apache top-level project in February 2026.** **Snowflake Open Catalog** is Snowflake's managed Polaris service. Snowflake's docs state that Polaris is integrated into **Horizon Catalog**.
- **Snowflake Horizon Catalog.** Snowflake's built-in governance catalog. It now exposes an Iceberg REST API so external engines can read **(GA)** and write **(GA, May 2026)** Snowflake-managed Iceberg tables, using Snowflake roles and policies and vended credentials.
- **AWS Glue Data Catalog.** The AWS-native catalog, with an Iceberg REST endpoint. Snowflake can also connect directly to the **S3 Tables** Iceberg REST endpoint **(GA, Aug 2026)**.

**Why the catalog is the new lock-in point (P11).** Once the formats are open and interchangeable, value moves to whoever controls commits, policies, lineage, sharing and maintenance. Two catalogs can each think they own a table, so the architecture rule is **one catalog of record per table**: the only place commits are arbitrated. Other catalogs *federate* to it (read through, or route writes through). Many engines may write, but all of them must commit through that one catalog.

### Interop: one copy, two engines

- **Delta UniForm.** Generates Iceberg metadata (Hudi in older OSS releases) for a Delta table, asynchronously after each Delta commit, on the writer's compute. Several Delta commits may batch into one Iceberg commit. Iceberg clients get **read-only** access. The docs warn that an external Iceberg writer "may destroy your Delta table." UniForm with Iceberg v2 can't coexist with deletion vectors. The Iceberg v3 variant (`IcebergCompatV3`) supports them **(GA on Databricks, May 2026; experimental in OSS Delta 4.3)**.
- **Databricks managed Iceberg tables (GA, May 2026 per release notes).** Native Iceberg tables in Unity Catalog, with liquid clustering and predictive optimization. External engines read and write through UC's Iceberg REST endpoint. **Foreign Iceberg tables** (Iceberg tables whose catalog of record is Glue, Snowflake or similar, read from Databricks) are also GA.
- **Snowflake Iceberg tables.** *Snowflake-managed* tables use Snowflake as the catalog and Snowflake runs maintenance. *Externally managed* tables use a catalog integration (Glue, Open Catalog/Polaris, any Iceberg REST catalog) as the catalog of record. **Catalog-linked databases** auto-discover a remote REST catalog's namespaces and tables, with DML on externally managed tables **(GA, Oct 2025)**.
- **Apache XTable** (incubating since 2024). Translates metadata between Delta, Iceberg and Hudi without copying data. It syncs only copy-on-write or read-optimized views, so Delta/Iceberg deletion vectors and Hudi log files aren't captured.

Failure modes of "one copy, two engines":

1. **Feature lag.** A writer enables Iceberg v3 deletion vectors, variant or a new Delta table feature, and a reader on an older spec fails or, worse, misreads.
2. **Concurrent writers across catalogs** (split brain), or path-based writes that bypass the catalog.
3. **Stats and optimization mismatch.** Engine A writes files without the stats, sort order or file sizes engine B relies on, so B's pruning collapses.
4. **Maintenance races.** Two engines both compact, or one engine's snapshot expiry deletes files another engine's time travel or streaming reads still need.
5. **Governance split.** Row filters and masks defined in one catalog don't follow the data when the other engine reads raw files via storage credentials (P9).
6. **Freshness lag.** Asynchronous metadata conversion (UniForm) means the second engine sees data seconds to minutes late.

## Comparison

| | Delta Lake | Apache Iceberg | Apache Hudi |
|---|---|---|---|
| Origin | Databricks, open-sourced 2019 (Linux Foundation) | Netflix, Apache TLP | Uber, Apache TLP |
| Metadata | Ordered JSON log + Parquet checkpoints | metadata.json → manifest list → manifests | Timeline of instants + metadata table |
| Atomic commit | Put-if-absent of `N+1.json` (or catalog-managed) | Catalog CAS of metadata pointer | Instant completion; lock provider / NBCC for multi-writer |
| Partitioning | Hive-style columns; liquid clustering | Hidden transforms; partition evolution | Hive-style partition paths |
| Row-level change | COW; deletion vectors | v2 delete files; v3 deletion vectors | COW or MOR table types; record-level index |
| Schema evolution | Add/widen; rename/drop needs column mapping | Field IDs: rename/drop/reorder safe | Backward-compatible (add/promote) on write; rename/drop via experimental schema-on-read |
| Time travel | Version/timestamp, bounded by VACUUM and log retention | Snapshot ID/timestamp, branches and tags | Instant time; incremental queries |
| Sweet spot | Spark/Databricks-centric batch + streaming | Multi-engine, catalog-centric interop | High-rate upserts/CDC, incremental pulls |

| Catalog | Formats | Open protocol | Typical role |
|---|---|---|---|
| Hive Metastore | Directories/partitions | Thrift HMS API | Legacy compatibility |
| Unity Catalog | Delta, Iceberg (+ others) | Iceberg REST, UC API; OSS version | Databricks-centric catalog of record |
| Apache Polaris / Snowflake Open Catalog | Iceberg | Iceberg REST | Neutral Iceberg catalog of record |
| Snowflake Horizon | Snowflake + Iceberg | Iceberg REST endpoint | Snowflake-centric catalog of record |
| AWS Glue / S3 Tables | Iceberg (+ Hive) | Iceberg REST, Glue API | AWS-native catalog of record |

## Interview questions

**Q: What does a table format add over Parquet?**

Parquet describes one file. A table format describes which set of files is the table at version N, and publishes each new version atomically. That gives you atomic multi-file commits, snapshot isolation for readers, schema enforcement, time travel, and file-level stats that let planners prune without LIST calls (P3, P4). Without it, a failed job leaves partial data visible and an UPDATE means hand-rewriting directories.

**Q: Delta vs Iceberg: how do you choose?**

The mechanisms have converged: both have optimistic commits, deletion vectors, variant and row tracking or lineage. So choose by ecosystem and catalog, not features. If Databricks is the primary writer and Spark streaming dominates, Delta is the native path and UniForm can expose it to Iceberg readers. If several engines must write, or Snowflake, Trino or Flink are first-class, Iceberg with a REST catalog of record is the neutral choice. The real decision is which catalog owns commits and governance. The format follows from that.

**Q: How does Iceberg achieve atomic commits?**

The writer creates all new data and metadata files, which nobody can see yet, and then asks the catalog to compare-and-swap the table's metadata pointer from the version it started from to the new one. If another writer committed first, the CAS fails. The writer re-reads, checks for conflicts, rewrites its metadata on the new base and retries. Atomicity therefore depends on the catalog. A catalog with weak CAS semantics, or a writer that bypasses it, breaks isolation.

**Q: How does Delta commit on object storage, and why was S3 a problem?**

A commit is the creation of the next numbered JSON file in `_delta_log`, and it must fail if that file already exists. S3 didn't originally offer put-if-absent, so two clusters could both "win" version N+1. OSS Delta solved this with a DynamoDB-backed log store (still marked experimental), and Databricks with its own commit service. Catalog-managed tables (Preview in Delta 4.0, production-supported since 4.1) hand commit arbitration to the catalog, which is the same direction Iceberg took.

**Q: Copy-on-write vs merge-on-read?**

COW rewrites every file an update touches. Writes are expensive and reads are clean, which suits read-heavy tables with batch updates. MOR writes small deltas (log files, delete files, deletion vectors) and merges them at read time. Writes are cheap, but reads degrade until compaction runs, which suits CDC and frequent MERGE. The failure mode of MOR is skipping compaction, so read latency creeps up week by week.

**Q: What are deletion vectors, and where does each format stand?**

A deletion vector is a bitmap of row positions marked deleted in an immutable data file. A DELETE writes kilobytes instead of rewriting gigabytes, and an UPDATE becomes a DV plus a new row in a new file. Delta has had them as a table feature for years. Iceberg added them in v3, stored in Puffin, and v3 tables may no longer add position delete files. Hudi gets the same effect through MOR log files. The catch is interop: readers on an older spec (Iceberg v2 clients, UniForm v2) can't read tables that use them.

**Q: Why do hidden partitioning and partition evolution matter?**

In Hive-style tables users must filter on the physical partition column, and a query that filters on `ts` instead of `ts_date` silently scans everything. Iceberg derives partitions from transforms of real columns, so pruning happens no matter how the user writes the filter. Because each manifest remembers its spec, you can change granularity as volumes grow without rewriting history. Delta's answer to the same problem is liquid clustering rather than partitioning.

**Q: What's the role of the catalog, and why is everyone fighting over it?**

It maps names to the current metadata pointer, arbitrates commits, and holds grants, policies, lineage and credential vending. Once formats are open, the catalog is the remaining control point: whoever owns it owns what "current" means and who may see it (P9, P11). That's why Unity Catalog, Polaris/Open Catalog, Horizon and Glue all now speak the Iceberg REST protocol. The architect's job is to pick one catalog of record per table and federate the rest.

**Q: How would you make one dataset readable by both Snowflake and Databricks?**

Pick the catalog of record by who writes most. If Databricks writes, use a UC managed Iceberg table, or Delta with UniForm. Snowflake reads it through a catalog integration or catalog-linked database pointing at UC's Iceberg REST endpoint. If Snowflake writes, use a Snowflake-managed Iceberg table, and Databricks or Spark access it through Horizon's Iceberg REST API. Then pin the spec version both engines support, assign maintenance to one side, and re-implement row and column policies wherever data is read outside the owning catalog. Don't do this when one engine only needs a small aggregate; a shared or exported gold table is simpler.

**Q: What breaks with two engines writing the same table?**

Committing through two different catalogs creates two "current" versions, which is split brain. Committing through one catalog is safe, but problems remain. One engine can enable a spec feature the other can't read. One engine's files may lack the stats or layout the other's optimizer expects. Two maintenance jobs can race, or one can expire snapshots the other still needs. Policies may be enforced by one engine and bypassed by the other. Keep one primary writer per table, with other engines writing only through the same catalog, under agreed feature flags.

**Q: When would you pick Hudi?**

Pick Hudi for very high-rate upserts from CDC into large tables, where you need key-based routing to touch only affected files, plus incremental "changed since T" reads for downstream jobs. Its record-level index and MOR table type exist for exactly that. The costs are more knobs to tune (index type, compaction, cleaning) and less native support in Snowflake and Databricks, so you'd usually expose it to those engines via XTable or keep it inside a Spark/Flink-centric stack.

**Q: What does VACUUM or snapshot expiry trade off?**

Every rewrite leaves the old files in place, so storage grows and time travel keeps working until cleanup removes them. Aggressive retention saves cost but breaks long time travel, and it can break streaming readers or other engines that are still behind. Long retention costs storage and slows metadata operations. Set retention from the longest legitimate consumer lag plus your recovery-point needs, not from the default.

## Takeaways
- A table format is "immutable files plus an atomically swapped metadata version" (P3). Everything else follows from that.
- Delta's commit lives in the log's put-if-absent (moving to catalogs). Iceberg's lives in the catalog's CAS.
- COW pays at write time, MOR pays at read time, and deletion vectors split the difference. Compaction is mandatory either way.
- As of 2026 the formats have mostly converged on features. The catalog decides control, governance and lock-in.
- One catalog of record per table. Many engines are fine as long as they all commit through it.
- Interop failures come from spec-version skew, maintenance ownership and governance gaps, rarely from bytes on disk.

## Sources
- Apache Parquet format specification: https://github.com/apache/parquet-format/blob/master/README.md
- Delta Lake protocol: https://github.com/delta-io/delta/blob/master/PROTOCOL.md
- Delta table property defaults (`checkpointInterval`, retention): https://github.com/delta-io/delta/blob/master/spark/src/main/scala/org/apache/spark/sql/delta/DeltaConfig.scala
- Delta Lake 4.0 release: https://delta.io/blog/2025-09-25-delta-lake-40/
- Apache Iceberg table spec: https://iceberg.apache.org/spec/ (read via https://github.com/apache/iceberg/blob/main/format/spec.md)
- Iceberg REST Catalog OpenAPI: https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml
- Hudi 1.0 announcement: https://hudi.apache.org/blog/2024/12/16/announcing-hudi-1-0-0
- Hudi 1.1 announcement: https://hudi.apache.org/blog/2025/11/25/apache-hudi-release-1-1-announcement
- Polaris graduation: https://polaris.apache.org/blog/2026/02/19/apache-polaris-graduates-to-top-level-project/
- Unity Catalog OSS: https://github.com/unitycatalog/unitycatalog
- Databricks UniForm: https://docs.databricks.com/aws/en/delta/uniform
- Databricks Iceberg overview and v3: https://docs.databricks.com/aws/en/iceberg/ · https://docs.databricks.com/aws/en/iceberg/iceberg-v3
- Databricks platform release notes (managed/foreign Iceberg and Iceberg v3 GA, May 2026): https://docs.databricks.com/aws/en/release-notes/product/
- Snowflake catalog-linked databases and external writes GA: https://docs.snowflake.com/release-notes/2025/other/2025-10-17-iceberg-external-writes-cld-ga
- Snowflake Iceberg v3 GA: https://docs.snowflake.com/release-notes/2026/other/2026-05-07-iceberg-v3-ga
- Snowflake Horizon Iceberg REST access: https://docs.snowflake.com/en/user-guide/tables-iceberg-query-using-external-query-engine-snowflake-horizon · https://docs.snowflake.com/release-notes/2026/other/2026-05-26-tables-iceberg-query-using-external-query-engine-snowflake-horizon-writes-ga
- Snowflake S3 Tables REST integration GA: https://docs.snowflake.com/release-notes/2026/other/2026-08-10-amazon-s3-tables-iceberg-rest-catalog-integration-ga
- Apache XTable: https://xtable.apache.org/ · https://incubator.apache.org/projects/xtable.html
