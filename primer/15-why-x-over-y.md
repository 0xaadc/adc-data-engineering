# 15. Why X over Y: The Tradeoff Bank

> As of October 2026. Tags: **(GA)** generally available · **(Preview)** public/private preview · **(⚠ verify)** not confirmed against official docs.

## Why this exists

"Why X over Y?" is the architect question. The interviewer rarely cares which option you pick. They are testing whether you can:

1. find the tension underneath the choice
2. argue both sides from mechanism
3. tie the decision to context
4. name what would make you reverse it

A memorized answer fails the moment they change one assumption. An answer reasoned from principles survives it.

Most of these pairs come down to a handful of principles in tension:

- **control vs. operability** (P10)
- **openness vs. co-designed optimization** (P11)
- **freshness vs. cost** (P7)
- **write cost vs. read cost** (P1, P3)
- **flexibility vs. contract** (P8)

## Core concepts

### The five-step answer

1. **Reframe as "what are we optimizing?"** Name the principle in tension. *"This is really a control-vs-operability question."*
2. **State the context you're assuming.** Team skills, workload mix, scale, latency need, compliance, existing estate. *"Assuming a SQL-heavy analytics team of five…"*
3. **Argue for X from mechanism.** Explain why it works, not just that it does.
4. **Steelman Y.** Describe the context in which Y wins. This step separates architects from advocates.
5. **Decide, then name the failure mode you'd monitor and the cost of reversing.** Say whether it's a one-way or two-way door. *"I'd pick X; the risk is Z; if we see W, we revisit. Reversal is cheap because the data stays in open format."*

### Phrases that signal architect thinking

- "It depends on X. Here's how each answer changes with it."
- "The real constraint here is the team, not the technology."
- "I'd start with the simpler option and earn the complexity."
- "That's a two-way door, so I'd bias toward speed. This other one is a one-way door, so I'd prototype first."
- "What decision consumes this data, and how fast is that decision made?" (P7)
- "Who is the single writer, and which catalog is the source of truth?" (P11)

### Anti-patterns that cost points

- Picking a side without stating assumptions.
- Feature-list comparisons ("X has feature A"). Features converge every quarter; mechanisms and tradeoffs don't.
- "Never" and "always" without context.
- Ignoring people and operating cost (P10). Total cost of ownership includes the on-call rotation.

## Comparison

**Decision cues at a glance**

| Pair | Underlying tension | Lean toward the first when… | Lean toward the second when… |
|---|---|---|---|
| Snowflake vs. Databricks | P10, P11 | SQL/BI-centric work, many concurrent users, small ops capacity, heavy external sharing | Diverse workloads (engineering, streaming, ML), code-first teams, an open-format mandate |
| Warehouse vs. lakehouse | P11 vs. co-design | Governed SQL analytics, minimal ops | Mixed workloads on one open copy |
| Delta vs. Iceberg | Catalog and engine reach | Databricks/Spark is the primary writer | Several engines must write, or Snowflake is primary |
| ETL vs. ELT | Where compute and risk sit | Data must be scrubbed before it lands; heavy non-SQL transforms | Elastic compute in the platform; keep raw for replay |
| Streaming vs. batch | P7 | Value decays in seconds to minutes | The consumer refreshes hourly or daily |
| Kafka vs. a managed bus | P10 | Ecosystem, portability, scale, many consumers | Low ops on one cloud, modest scale |
| Flink vs. Spark Streaming | Latency/state vs. unification | ms latency, complex state/timers | Streaming feeds the lakehouse; team already on Spark |
| Mesh vs. central platform | Autonomy vs. consistency | Many mature domains; the central team is a bottleneck | Small or immature org |
| Kimball vs. Data Vault vs. OBT | Usability vs. auditability vs. simplicity | Business-facing marts | Many changing sources needing an audit trail / single-use consumption |
| dbt vs. native transforms | Portability vs. integration | Multi-platform, analyst-heavy | Managed incrementality/streaming on one platform |
| Serverless vs. provisioned | Unit price vs. idle cost | Bursty, interactive, spiky | Steady, high-utilization |
| Build vs. buy | Differentiation vs. maintenance | Unique source or logic, scale economics | Commodity connectors, small team |

## Interview questions

**Q: Why choose Snowflake when Databricks exists?**

Choose Snowflake when the center of gravity is SQL analytics and BI with many concurrent consumers, and the team is small relative to what it supports. Separate virtual warehouses over shared storage give each workload hard isolation (P2), and the managed service removes most tuning (P10). Sharing and the marketplace are mature, and Iceberg tables have narrowed the old openness gap (P11). I wouldn't make it the primary engine for heavy custom Python/ML or high-volume stateful streaming, where Spark-native tooling is deeper. I'd also govern cost early: per-second credits suit bursty SQL, but constant heavy compute with no guardrails gets expensive.

**Q: Why choose Databricks when Snowflake exists?**

Choose Databricks when the workloads are diverse (ingestion, streaming, ML, AI) and you want one open copy of the data in your own storage to serve all of them (P11, P3). Code-first teams get control over compute, libraries, and engines. Unity Catalog governs tables, models, and features in one place. The tradeoff is more surface area to operate and govern, which serverless has reduced but not eliminated. For a small SQL-only BI team that wants zero operations, it is usually more platform than they need.

**Q: Lakehouse or warehouse?**

The real question is whether you want one open copy served to many engines and workload types (P11, P3), or storage and engine co-designed and managed for SQL (P10). The warehouse wins for governed, SQL-centric analytics with minimal ops. The lakehouse wins for mixed workloads, ML on the same data, very large raw data, or an open-format requirement. In 2026 the line is blurry: Snowflake reads and writes Iceberg, and Databricks runs serverless SQL warehouses. So the decision that still matters is who owns the storage format and catalog, and which engines need to write.

**Q: Delta or Iceberg?**

Both implement transactions on object storage (P3) with different metadata designs. Delta uses an ordered transaction log. Iceberg uses a tree of metadata and manifests, with the atomic commit happening in the catalog, plus hidden partitioning and partition evolution. Iceberg has the broadest multi-engine write support and an open REST catalog spec. Delta has the deepest integration and the fastest feature delivery on Databricks. Decide by which engines must *write*: Databricks-centric means Delta (exposing Iceberg metadata to readers via UniForm); multi-engine or Snowflake-primary means Iceberg. Either way, the format matters less than choosing one catalog of record and one writer per table ([§04](04-table-formats-catalogs.md)).

**Q: ETL or ELT?**

ELT became the default because elastic in-platform compute made transforming after load cheap (P2), and keeping raw data lets you replay when the logic changes (P5). ETL still wins when data must be filtered or masked before it lands (PII, residency), when transforms are heavy and non-SQL (binary parsing, ML enrichment), or when raw volume is too costly to keep. Most real platforms are really "EtLT": validate and scrub lightly in flight, then model heavily inside the platform.

**Q: Streaming or batch?**

Start from how quickly the consuming decision is made (P7). Streaming pays for always-on compute, state management, small files, and harder reprocessing and on-call. Batch is cheaper, simpler, and easier to backfill. Stream when value decays in seconds to minutes: fraud, alerting, operational triggers. The pragmatic middle is to write the logic with incremental, streaming semantics and run it on a schedule (e.g., an `availableNow` trigger, or a Dynamic Table with a target lag), then tighten the latency only when a real consumer needs it.

**Q: A stakeholder asks for "real-time." How do you respond?**

Translate the request into a requirement by asking what action someone takes on the data, how quickly, and what a five-minute or one-hour delay would actually cost. Then price the options: daily, hourly, minutes, seconds, each with its compute, complexity, and on-call cost (P7). Often "real-time" means "fresher than yesterday", and a 15-minute incremental pipeline meets the need at a fraction of the cost. When seconds genuinely matter (fraud blocking), build streaming for that narrow path only, not for the whole platform.

**Q: Kafka or a managed event bus (Event Hubs, Kinesis, Pub/Sub)?**

Kafka, whether self-managed, Confluent, or MSK, offers the richest ecosystem (Connect, Schema Registry, Kafka Streams, transactions, compaction, tiered storage) and portability across clouds. Managed buses offer less to operate and native cloud integration, but less control, tier-based limits on retention and partitions, and only partial Kafka feature compatibility. On Azure at modest scale, Event Hubs with its Kafka-compatible endpoint is often enough. At high scale, multi-cloud, or with heavy CDC/Connect use, I'd choose managed Kafka. I would never self-manage Kafka without a team staffed to run it (P10) ([§10](10-kafka.md)).

**Q: Do you even need Kafka for CDC into the lakehouse?**

Not always. If the only goal is replicating an operational database into the lakehouse, a managed connector (Lakeflow Connect, Fivetran, Openflow-style tooling) or Debezium writing straight to storage may be simpler and cheaper. Kafka earns its operational cost when several consumers need the same change stream, when event-driven services consume it, or when you need replay and fan-out as a shared backbone (P5). Adding Kafka for a single consumer is complexity without payoff.

**Q: Flink or Spark Structured Streaming?**

Flink processes record by record, with first-class event time, rich keyed state and timers, and millisecond latency. It suits complex event processing and large stateful applications where streaming *is* the product. Spark Structured Streaming defaults to micro-batches and shares its API with batch Spark. Its latency is good enough for most analytics, and it fits naturally when the stream feeds Delta tables on Databricks. Choose Flink when latency or state complexity is the core requirement and you can staff another engine. Choose Spark when streaming is a means of keeping the lakehouse fresh ([§11](11-stream-processing.md)).

**Q: Data mesh or a centralized platform team?**

Data mesh means domain ownership, data as a product, a self-serve platform, and federated governance. It removes the central-team bottleneck at scale, because domains understand their own data best. It only works with mature domain teams, a strong platform, and enforced standards; without them it fragments into inconsistent silos with duplicated effort. For small or medium organizations a central team is more efficient. The realistic middle is "mesh-lite": a central platform team owns the paved roads and governance (P9), and domains own their data products on top.

**Q: Kimball, Data Vault, or One Big Table?**

They optimize for different things:
- **Kimball star schemas** optimize for business comprehension and query performance, using conformed dimensions.
- **Data Vault** optimizes for auditable, insert-only integration of many changing sources with parallel loads, but needs a presentation layer (usually Kimball marts) on top.
- **OBT** optimizes for simple consumption on columnar engines, where repeated values compress well (P4), but it duplicates logic and handles history poorly.

They layer rather than compete: normalized or Vault-style integration in silver, Kimball marts in gold, and OBTs as consumption views for specific dashboards or ML features ([§03](03-data-modeling.md)).

**Q: dbt or native transforms (Dynamic Tables, Lakeflow Spark Declarative Pipelines)?**

dbt gives you version-controlled SQL with tests, docs, lineage, and a large ecosystem, and it is portable across engines. Native declarative transforms give you managed incremental refresh, dependency scheduling, and streaming semantics without an external tool, at the cost of platform lock-in (P10). Choose dbt for multi-platform estates and analyst-heavy teams; choose native for tight incremental or streaming pipelines on one platform. They aren't exclusive: dbt adapters can materialize models as platform-native objects such as dynamic tables or streaming tables (⚠ verify the current adapter support).

**Q: Serverless or provisioned compute?**

Serverless means the vendor holds capacity: fast start, no idle cost visible to you, nothing to tune. The price is a higher unit cost and less control over instance types, networking, and libraries. Provisioned or reserved compute is cheaper per unit at sustained high utilization, but you pay for idle time and for operating it. Bursty, interactive, or unpredictable workloads favor serverless; steady, long-running, high-utilization jobs favor provisioned. Whichever you choose, put budgets and attribution on serverless spend, because "no clusters to manage" also means "no cluster to notice is expensive" ([§13](13-performance-cost.md)).

**Q: Build or buy ingestion?**

Buy the commodity, build the differentiator. Standard SaaS sources (CRM, ERP, ads platforms) are solved problems, and maintaining their API changes yourself is pure cost. Build when the source is unusual, when volume makes per-row pricing explode (high-churn tables under monthly-active-row pricing), or when latency or control requirements exceed what the vendor offers. Count the people cost of maintenance, the security review, and the exit cost, not just the license ([§09](09-batch-pipelines.md)).

**Q: One platform or two (Snowflake + Databricks)?**

One platform minimizes governance surface, data copies, and the skills split. Two can be justified by clear workload specialization (e.g., Databricks for engineering and ML, Snowflake for BI and sharing) or by organizational history such as an acquisition. If you run both, define the rules explicitly:
- a single writer per table
- one catalog of record
- interop through open formats or sharing rather than copies
- identity federated from one IdP
- cost attributed per platform

Running two because two teams each prefer one is an organizational problem disguised as an architecture ([§08](08-snowflake-vs-databricks.md)).

**Q: Partitioning or clustering?**

Hive-style partitioning physically separates data by directory. That works for coarse, low-cardinality columns that every query filters on (e.g., date on very large tables). On high-cardinality columns it produces small files, and a bad choice is expensive to undo. Clustering sorts data within files so that min/max ranges are narrow and skipping works (P4), and modern implementations (liquid clustering, Snowflake automatic clustering) can be changed and maintained incrementally. Today's default is to cluster rather than partition, except for very large tables with an obvious, stable partition column.

**Q: `MERGE` into current state, or append and deduplicate on read?**

Append is cheap to write and keeps full history (P5). `MERGE` gives consumers clean current state but rewrites files on every update (P3 write amplification). Append-plus-dedup-in-a-view moves that cost to every read. Decide by the read-to-write ratio: write-heavy, rarely read data favors append; widely read data favors `MERGE`, ideally with deletion vectors to cheapen the rewrites. A common hybrid keeps an append-only history table and a merged current-state table derived from it.

**Q: Precompute (materialized views, aggregates) or compute at query time (views)?**

Precomputing trades storage and refresh compute for read latency and per-query cost. Query-time views keep one definition and always-fresh results, but pay compute on every read. Precompute when reads vastly outnumber refreshes and the freshness tolerance allows it (P7); keep views when results are rarely read or must be exact at query time. Incremental refresh (Dynamic Tables, materialized views over streaming tables) shifts the balance toward precompute.

**Q: On Azure, where does Microsoft Fabric fit versus Databricks or Snowflake?**

Fabric is a SaaS bundle with capacity-based pricing, centered on Power BI and OneLake, with Delta as its native table format. It fits Microsoft-centric organizations that are BI-led and have moderate engineering and ML needs. Databricks or Snowflake fit when engineering, ML, or SQL-warehouse requirements are heavier, or the platform needs multi-cloud portability. Coexistence is common and cheap thanks to OneLake shortcuts and mirroring, so the question is usually "where does each workload live" rather than either/or ([§08](08-snowflake-vs-databricks.md)).

## Takeaways

- Every "why X over Y" hides a principle in tension. Name it first.
- State your assumptions, argue X from mechanism, steelman Y, then decide and name the reversal cost.
- Feature comparisons expire every quarter; tradeoffs and mechanisms don't.
- The team and the operating cost are part of the architecture (P10).
- Freshness, openness, and control are purchases, not defaults (P7, P11, P10).
- When two platforms coexist, set a single writer per table and one catalog of record, or the platform will drift.

## Sources

- Databricks: When to partition tables — https://docs.databricks.com/aws/en/tables/partitions
- Databricks: Liquid clustering — https://docs.databricks.com/aws/en/delta/clustering
- Snowflake: Apache Iceberg tables — https://docs.snowflake.com/en/user-guide/tables-iceberg
- Snowflake: Dynamic tables — https://docs.snowflake.com/en/user-guide/dynamic-tables-about
- Azure Event Hubs for Apache Kafka — https://learn.microsoft.com/en-us/azure/event-hubs/azure-event-hubs-apache-kafka-overview
- Dehghani, "Data Mesh Principles and Logical Architecture" — https://martinfowler.com/articles/data-mesh-principles.html
- See also the per-topic sections linked above for platform-specific sources.
