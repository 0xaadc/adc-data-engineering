# Data Architect Primer: Index

A principles-first crash course for data architect interviews on **Snowflake and Databricks**, including Kafka and streaming. It is written for engineers who know one platform (usually Databricks) and need to reason confidently about the other, about streaming, and about the tradeoffs between them.

**How to use it:** read [§01 First Principles](01-first-principles.md) first. It defines eleven principles (P1–P11), and every other section derives its facts from them. When a section writes "(P3)", it means "this follows from immutable files + a metadata log". If you understand why something works, you rarely need to memorize what it is.

> As of October 2026. Snowflake and Databricks rename and ship features constantly. Status tags: **(GA)**, **(Preview)**, and **(⚠ verify)** for anything not confirmed against official docs. Always check the docs before quoting a limit in production work.

## The principles at a glance

| ID | Principle | One-line mechanism |
|---|---|---|
| P1 | Write-optimized vs. read-optimized | OLTP rows and OLAP columns are opposite layouts, so data must move between them |
| P2 | Separate storage from compute | Cheap durable object storage plus elastic, disposable, isolated compute |
| P3 | Immutable files + metadata log = transactions | Never update in place: write new files, then atomically swap metadata (gives time travel and clones) |
| P4 | Skip work instead of doing it faster | Pruning, statistics, clustering, and caching; the fastest byte is never read |
| P5 | The log is the source of truth | An ordered, replayable change history; tables are derived snapshots |
| P6 | Idempotent, replayable steps | Retries are inevitable, so running twice must equal running once |
| P7 | Latency is a cost dial | Buy only the freshness the decision needs |
| P8 | Schema is a contract | Decide where it's enforced and who owns change |
| P9 | Govern the data, not the tool | Policies live in the catalog, not in each engine |
| P10 | Abstraction trades control for operability | Managed/SQL-first vs. configurable/code-first |
| P11 | Open formats decouple data from engines | Less lock-in; the catalog becomes the new lock-in point |

## Contents

★ = highest interview weight

| # | Section | What it answers |
|---|---|---|
| **Part 1: Foundations** | | |
| 01 | [First Principles](01-first-principles.md) ★ | Why data engineering exists; why Snowflake, why Databricks |
| 02 | [Storage Paradigms and Architectures](02-storage-paradigms.md) | Warehouse vs. lake vs. lakehouse; medallion; Lambda vs. Kappa |
| 03 | [Data Modeling](03-data-modeling.md) | Kimball/Inmon/Data Vault; star vs. OBT; grain; SCDs; semantic layer |
| 04 | [Open Table Formats and Catalogs](04-table-formats-catalogs.md) | Delta vs. Iceberg vs. Hudi; catalogs; one copy, two engines |
| **Part 2: Platforms** | | |
| 05 | [Snowflake I: Architecture, Performance, Cost](05-snowflake-architecture.md) ★ | Three layers, micro-partitions, warehouses, caching, credits |
| 06 | [Snowflake II: Data Features](06-snowflake-features.md) ★ | Time Travel, cloning, Streams/Tasks, Dynamic Tables, Snowpipe, sharing, Snowpark |
| 07 | [Databricks Gap-Fill](07-databricks-gap-fill.md) | What's new or renamed: Lakeflow, liquid clustering, serverless, Lakebase |
| 08 | [Snowflake vs. Databricks](08-snowflake-vs-databricks.md) ★ | Why each, running both, cost models, interop, Fabric |
| **Part 3: Data Movement** | | |
| 09 | [Batch Pipelines, CDC, Orchestration](09-batch-pipelines.md) | ETL/ELT, incremental loads, CDC, idempotency, dbt vs. native |
| 10 | [Kafka and Event Streaming, From Zero](10-kafka.md) ★ | Partitions, consumer groups, replication, delivery semantics, Connect, Debezium |
| 11 | [Stream Processing](11-stream-processing.md) ★ | Event time, windows, watermarks, state, exactly-once, Flink vs. Spark |
| **Part 4: Operating the Platform** | | |
| 12 | [Governance and Security](12-governance-security.md) | RBAC/ABAC, masking, row policies, lineage, data contracts |
| 13 | [Performance and Cost (FinOps)](13-performance-cost.md) | Small files, caching, chargeback, guardrails |
| 14 | [Quality, Reliability, DataOps](14-reliability-dataops.md) | Testing, observability, DR, CI/CD, environments |
| **Part 5: Architect and Co-Lead Layer** | | |
| 15 | [Why X over Y](15-why-x-over-y.md) ★ | The tradeoff bank, argued in both directions |
| 16 | [System Design Walkthroughs](16-system-design.md) | Framework plus three end-to-end designs |
| 17 | [Leadership and Behavioral](17-leadership-behavioral.md) | Design reviews, disagreement, co-leading, STAR template |
| 18 | [AI on the Data Platform](18-ai-on-data-platform.md) | Feature stores, vector search, governed RAG, Cortex vs. Mosaic AI |
| **Review** | | |
| — | [Cheat Sheet](cheat-sheet.md) | One page for the last 15 minutes |
| — | [Question Bank](question-bank.md) | Every interview question, with answers hidden for self-testing |

## Reading paths

**About 5 hours (interview later today):**
1. §01 First Principles (25 min). Everything else hangs on it.
2. §05 → §06 Snowflake (60 min). Read the "Coming from Databricks" mappings closely.
3. §08 Snowflake vs. Databricks (30 min)
4. §10 → §11 Kafka and stream processing (60 min)
5. §15 Why X over Y (25 min). Practice saying the answers out loud.
6. §04 Table formats and §02 Storage paradigms (30 min)
7. Skim §16 System design: just the framework and one walkthrough (20 min)
8. Self-test with the [Question Bank](question-bank.md) on the starred sections (rest of the time)
9. [Cheat Sheet](cheat-sheet.md) right before the interview (10 min)

**About 2 hours:** §01 → §05 → §08 → §10 → §15 → Cheat Sheet.

**30 minutes:** Cheat Sheet → §15 → the Takeaways sections of §05, §08, §10, §11.

## Section template

Every section follows the same shape, so you can skim consistently:

1. **Why this exists:** the problem that forced the thing into being
2. **Core concepts:** the mechanism, and why it produces the properties it has
3. **Comparison:** tables
4. **Interview questions:** 2–5 sentence model answers covering tradeoffs, when *not* to use something, and failure modes
5. **Takeaways:** one-liners
6. **Sources:** official documentation
