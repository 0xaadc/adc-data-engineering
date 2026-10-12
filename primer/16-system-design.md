# 16. System Design Walkthroughs

> As of October 2026. Tags: **(GA)** generally available · **(Preview)** public/private preview · **(⚠ verify)** not confirmed against official docs.

## Why this exists

A design round has no single correct diagram, so interviewers grade how you get to yours. They check whether you pin down requirements before naming tools, whether each component has a reason behind it, whether you name the tradeoff you're accepting, and whether you know how the design breaks. A repeatable framework keeps you from jumping straight to products. It also makes your reasoning visible, so the interviewer can steer you toward the part they care about.

## Core concepts

### The answer framework

| Step | What you do | Time in a 45-min round |
|---|---|---|
| 1. Clarify | Consumers and decisions, freshness SLAs, volume, correctness, compliance, team skills, budget. State assumptions out loud. | 5–8 min |
| 2. High-level design | Sources → ingest → store → transform → serve → govern/observe. One box per stage, then fill each one in. | 8–10 min |
| 3. Deep dive | The 1–2 hardest parts: usually correctness in streaming, CDC, or migration cutover. | 10–15 min |
| 4. Tradeoffs | What you chose, what you rejected, and why. | 5 min |
| 5. Failure modes and ops | What breaks, how you detect it, how you recover (replay, backfill, rollback). | 5 min |
| 6. Evolution | What changes at 10x volume, with a new team, or under a new regulation. | 3 min |

The order matters. Requirements decide everything downstream, because latency and correctness are bought with money and complexity (P7). The deep dive shows you have depth. Failure modes are what separate an architect from someone drawing boxes.

### Requirements questions to ask

- **Consumers and decisions:** Who uses each output, and what decision does it drive (dashboard, finance close, ML feature, operational alert, external customer)?
- **Freshness:** How stale can each output be: seconds, minutes, hours, next day? Different consumers usually give different answers.
- **Correctness:** Is approximate acceptable (product metrics), or must it reconcile to the cent (finance)? Is at-least-once delivery plus dedup enough?
- **Volume and velocity:** Average and peak events/sec, row counts, growth rate, burstiness (sales, month-end).
- **Sources:** Which systems? Can they emit CDC? Who owns them, and how often does their schema change (P8)?
- **History and retention:** How far back? Are there legal retention rules or deletion (right-to-be-forgotten) requirements?
- **Compliance:** PII/PCI/HIPAA, data residency, audit evidence.
- **Access patterns:** Ad-hoc SQL, BI concurrency, millisecond API lookups, ML training.
- **Team:** SQL-first or Spark/Python-first? Team size and on-call capacity?
- **Budget and constraints:** Existing platform contracts, cloud provider, deadline.
- **Availability:** RPO/RTO, multi-region needs.

### Numbers to estimate

Do the arithmetic out loud and round hard.

- Events/sec, average and peak (retail peaks are often 3–10x average).
- Event size: 0.5–2 KB as JSON. Avro/Protobuf are typically several times smaller.
- **GB/day raw** = average events/sec × 86,400 × event size.
- Columnar size: Parquet is often 5–10x smaller than raw JSON.
- **Kafka disk** = GB/day × retention days × replication factor.
- Partitions ≈ peak MB/s ÷ per-partition consumer throughput, plus headroom.
- Lake storage growth per year, split into hot and cold.
- BI concurrency: peak users × queries per minute.

*Worked example:* 5,000 events/s average, 30,000 peak, 1 KB each. That's about 430 GB/day raw and roughly 50–80 GB/day as Parquet. Seven-day Kafka retention at RF=3 needs about 9 TB of broker disk, and peak ingress is about 30 MB/s. Conclusion: this is a mid-sized system. Ingest is not the cost risk; BI concurrency and full-refresh transforms are.

### Walkthrough 1: Real-time + batch analytics (e-commerce)

**Requirements (assumed).**
- Clickstream runs about 5k events/s on average and 30k at peak.
- Orders and payments live in a Postgres OLTP database.
- Fraud scoring must flag a checkout within seconds. The decision is "hold this order".
- Ops metrics (GMV for the last 5 minutes) must be under one minute stale.
- Finance reports run daily and must reconcile to the payment processor exactly.
- Card data is tokenized upstream, but PII still flows through the platform.
- The engineering team knows Databricks, and BI users already work in Snowflake.

```
 Web/App SDK ─────────────────────► Kafka: clickstream (Avro + schema registry)
 Postgres OLTP ─► Debezium (WAL) ─► Kafka: cdc.orders, cdc.payments
                                        │
          ┌─────────────────────────────┴──────────────────────────────┐
          │ HOT PATH (seconds)                                         │ LAKEHOUSE PATH (minutes → daily)
          ▼                                                            ▼
  Stream processor (Flink or Spark SS)                  BRONZE (Delta): raw topic copies,
   - join order + session context                        append-only, long retention = replay log
   - online feature lookup + fraud model                               │ streaming ingest
   - emit decision ─► Kafka: fraud.decisions                           ▼
   - 1-min GMV windows ─► ops dashboard store           SILVER: typed, deduped, CDC applied
          │                                              (MERGE on PK, ordered by LSN)
          ▼                                                            │ batch / SQL models
  Online feature store (low-latency)                    GOLD: conformed facts/dims, finance marts
                                                                       │ Iceberg (UniForm) via UC REST
                                                                       ▼
                                                        SNOWFLAKE: BI serving, semantic layer,
                                                        finance reporting, external sharing
  Cross-cutting: identity, tags, policies, lineage, audit (Walkthrough 3) · observability on freshness/lag/cost
```

**Component choices and why.**
- **Kafka as the backbone (P5):** One durable, replayable log feeds both paths, and consumers are decoupled from producers. After a bug, any downstream can be rebuilt by replaying. Schema registry compatibility rules turn the event schema into an enforced contract (P8). See [§10](10-kafka.md).
- **Log-based CDC via Debezium (P1):** It reads the WAL, so it captures deletes and adds no query load on the OLTP primary. Treat its output as at-least-once and dedup on (primary key, log position). See [§09](09-batch-pipelines.md).
- **Separate hot and finance paths (P7):** Fraud needs seconds and can tolerate approximation. Finance needs exactness and can tolerate a day. One pipeline built to meet both requirements is expensive and fragile.
- **Lakehouse on Databricks (P2, P3):** Cheap object storage, with Delta MERGE and time travel for repair. Streaming and batch write the same tables.
- **Snowflake as the serving layer:** It's justified here by existing BI users, high-concurrency dashboards, and sharing. Gold tables can be exposed as Iceberg through the Unity Catalog Iceberg REST endpoint and read from a Snowflake catalog-linked database. Catalog-linked databases for external Iceberg REST catalogs are **(GA)** as of Oct 2025, but check whether UC is a supported endpoint for that path **(⚠ verify)**. The alternative is to copy gold into native Snowflake tables when query performance requires it (P11). See [§04](04-table-formats-catalogs.md).
- **Single-platform option:** If BI can run on Databricks SQL, you remove a hop, a second governance plane, and a second bill. Keep two platforms only for a concrete reason. See [§08](08-snowflake-vs-databricks.md).

**Deep dive: exactly-once.** In practice "exactly-once" means *effectively-once*: each event affects the output once, even if it's processed more than once. It has to hold at three links:
1. *Producer → Kafka:* the idempotent producer dedups retries by producer ID and sequence number, and transactions make writes across partitions atomic.
2. *Processing:* the engine checkpoints offsets and state together and replays from the last checkpoint on restart. Reprocessing is therefore expected, not exceptional.
3. *Sink:* the sink must be idempotent or transactional. A Structured Streaming Delta sink records the batch ID in the log so a replayed batch is skipped. A MERGE on a natural key (`order_id`, `event_id`) is idempotent. Flink uses two-phase-commit sinks. Kafka sinks need transactions, and their consumers must read with `read_committed`.

External side effects, such as emails or payment API calls, can't be exactly-once. Make them idempotent with request keys instead (P6). See [§11](11-stream-processing.md).

**Deep dive: late and out-of-order data.** Aggregate on event time, not processing time. A watermark, such as 10 minutes, tells the engine how long to keep each window's state open. Events that arrive after the watermark are dropped from the real-time aggregate but still land in bronze. On the batch path, silver MERGEs late rows by key, and gold recomputes a lookback window (for example the last 3 days) or only the partitions those rows touched. Finance closes at a defined cutoff, and later corrections post as adjustments in the next period instead of silently rewriting a closed one. Tell stakeholders explicitly: real-time numbers are *provisional*, and batch numbers are *of record*.

**Where the medallion layers sit.**
- **Bronze:** maps 1:1 to topics. Kafka keeps days of data; bronze keeps the long-term replay history.
- **Silver:** CDC is applied here (current state plus SCD2 history where needed), and PII is tagged or tokenized.
- **Gold:** business facts, dimensions, and metric definitions. This layer is the contract with Snowflake and BI.

The hot path reads Kafka directly and skips the medallion layers. Its decisions are written back to Kafka, so they also land in bronze for audit.

**Tradeoffs considered.**
- *Flink or Spark for the hot path:* Flink has lower per-event latency and richer event-time state. Spark keeps one engine and one skill set, and micro-batches usually reach seconds.
- *Dual paths:* two codebases can compute the same metric differently. Compute shared features once, and reconcile real-time against batch totals daily, alerting on drift.
- *Copy or zero-copy into Snowflake:* zero-copy avoids duplicate storage and sync jobs. A copy gives Snowflake-native performance and features.

**Failure modes and mitigations.**
- *Consumer lag at peak:* size partitions for peak, autoscale consumers, and alert on lag measured in seconds rather than offsets.
- *Upstream schema change:* enforce registry compatibility checks, run contract tests in the source team's CI, and keep raw data in bronze so you can reprocess.
- *Debezium replication slot falls behind:* Postgres retains WAL for the slot and can fill the primary's disk. Monitor slot lag and page both teams.
- *Bad deploy corrupts silver:* restore with Delta time travel, then replay from bronze (P3, P5).
- *Fraud service or feature store down:* fail-open versus fail-closed is a business decision. Keep a rules-engine fallback.
- *Stale Iceberg metadata on the Snowflake side:* monitor freshness at the gold → Snowflake boundary.

**Evolution.** Publish curated "gold streams" as Kafka data products. At 10x scale, add small-file compaction and clustering ([§13](13-performance-cost.md)). If the second platform's unique reason disappears, consolidate onto one.

### Walkthrough 2: Migrating a legacy on-prem warehouse

**Requirements (assumed).** An on-prem SQL Server (or Teradata/Oracle) warehouse of about 40 TB, with around 1,500 SSIS packages and stored procedures and about 300 reports. A nightly batch must finish by 06:00. A license or hardware renewal is due in 12–18 months. The target is Snowflake for warehousing and BI, Databricks for engineering and ML, or one of the two.

```
 LEGACY                                        TARGET
 Sources ─► SSIS ─► SQL Server DW ─► BI        Sources ─► CDC/connectors ─► Raw/Bronze
                         │                                                   │
                         │ (1) bulk history export (Parquet)                 ▼
                         └────────────────────────────────► Stage ─► Converted models (SQL/dbt/pipelines)
                                                                             │
        (2) PARALLEL RUN: same inputs, both sides, ≥1 month-end              ▼
        (3) RECONCILE: counts → sums → row hashes ◄────────────── Target marts ─► BI (repointed per domain)
        (4) CUT OVER by domain, legacy read-only   (5) DECOMMISSION after sign-off
```

**Phases.**
1. **Assess.** Inventory tables, procedures, packages, reports, and jobs, and mine query logs for real usage. A sizable share of objects is usually unused, and the cheapest migration is the one you skip. Build a dependency graph and score each object's complexity. Tooling: Databricks Lakebridge is free and covers profiling, assessment, SQL conversion, and reconciliation; its LLM-based agentic converter is **(Preview)** (Beta). On the Snowflake side, SnowConvert AI and the Migration Agent in the Cortex Code CLI cover similar ground **(⚠ verify)**.
2. **Choose lift-and-shift or re-architect per domain** (see Comparison).
3. **Build foundations once:** landing zone, network, identity and RBAC, CI/CD, orchestration, observability, cost guardrails. Waves stall when each one rebuilds plumbing.
4. **Migrate in waves by domain.** Start with a medium-value, low-complexity domain that has a supportive business owner. Each wave covers history load, logic conversion, pipelines, parallel run, reconciliation, BI repointing, and cutover.
5. **Parallel run and reconcile.** Run both systems on the same inputs for several cycles, including a month-end. Reconcile in layers:
   - row counts per partition
   - sums of key measures by day and dimension
   - row-level hashes on keys to pinpoint the exact rows that differ
   - top-report comparisons signed off by business owners

   Agree on tolerances up front: zero for money, explicit rules for floats. Typical causes of differences are collation and case sensitivity, NULL semantics, time zones, decimal precision, implicit casts, and `TOP` without `ORDER BY`.
6. **Cutover and rollback.** Legacy stays live but read-only for a fixed window. Rollback means repointing BI back to legacy, which is cheap if BI connects through an alias or semantic layer. Decommission only after the window closes and owners sign off.

**SQL dialect conversion.** Automated converters handle most syntax. What remains is procedural code (cursors, temp-table chains, dynamic SQL), and that's the moment to rewrite it as set-based SQL or Python instead of transliterating it. Validate converted code with data, not just by getting it to compile.

**Orchestration replacement.** Don't translate SSIS packages 1:1, because a package mixes extract, transform, and control flow. Split them:
- ingestion into managed connectors or CDC
- transforms into dbt, declarative pipelines, or Snowflake dynamic tables and tasks
- control flow into an orchestrator (Airflow, Lakeflow Jobs, or Snowflake tasks)

See [§09](09-batch-pipelines.md).

**BI repointing.** Publish compatibility views with the legacy names so reports change their connection, not their logic. Retire those views later.

**People and skills.** Pair legacy experts, who know the business rules buried in stored procedures, with cloud engineers. Those experts are your largest retention risk. Train analysts before their domain cuts over.

**Failure modes and mitigations.**
- *Big-bang cutover:* cut over by domain instead.
- *Migrating dead workloads:* prune using usage logs.
- *Endless reconciliation debates:* agree on tolerances and owners up front.
- *Hidden consumers* (spreadsheets pulling over ODBC): find them in connection logs before you shut anything off.
- *Cost blowout* from row-by-row procedures running on consumption pricing: re-architect the top cost offenders and set budgets and resource monitors from day one ([§13](13-performance-cost.md)).

**Evolution.** After cutover, make the expensive models incremental, add streaming only where a decision needs it, and retire the compatibility layer.

### Walkthrough 3: Unified governance across Snowflake and Databricks

**Requirements (assumed).** Both platforms hold PII and financial data. Auditors want a single answer to "who *can* see customer email, and who *did*?" Policies must be consistent across engines.

```
                 IdP (Entra ID / Okta): users, groups, service principals
                    │ SCIM                                   │ SCIM
                    ▼                                        ▼
        Databricks account + Unity Catalog          Snowflake account (roles)
                    ▲                                        ▲
                    │  Terraform/CI  ◄── policy-as-code repo: tag taxonomy, ──► Terraform/CI
                    │                    policy matrix, grants, owners
        UC: governed tags, ABAC row filters /       Snowflake: object tags, tag-based masking,
        column masks, data classification            row access policies, classification
                    │                                        │
                    └──── shared gold: Iceberg REST (UC → catalog-linked DB) ────┘
                    │                                        │
        system tables (audit, lineage)               ACCOUNT_USAGE (ACCESS_HISTORY, etc.)
                    └──────────► central audit + lineage store + drift detector ◄──────┘
```

- **Identity (P9):** The IdP is the source of truth for people and groups, and SCIM provisions both platforms. Grant to groups, never to individuals, and give each pipeline its own service principal. Joiners, movers, and leavers are then handled in one place.
- **Catalog of record (P11):** Every table has exactly one owning catalog, the one where it's written. For example, UC owns bronze through gold and Snowflake reads gold over Iceberg REST, while Snowflake owns its native marts. Never let both engines write the same table. The catalog is the new lock-in point, so choose it deliberately ([§04](04-table-formats-catalogs.md)).
- **Tag taxonomy:** a small, controlled vocabulary:
  - `classification` (public / internal / confidential / restricted)
  - `pii_type`
  - `domain`
  - `owner`
  - `retention`

  Define it once in a repo. Apply it through automation plus classifiers: UC data classification and governed tags are **(GA)**, and Snowflake has sensitive-data classification.
- **Policy enforcement:** Bind policies to tags, not tables, so a newly tagged column is protected without a ticket. In UC, use ABAC policies on governed tags for row filters and column masks **(GA)**. In Snowflake, use tag-based masking policies and row access policies. The intent is the same but there are two implementations, so keep one *policy matrix* (tag × group → clear / masked / hashed / denied) and generate both from it. See [§12](12-governance-security.md).
- **Sharing:** Zero-copy interop goes over Iceberg REST. Within the company it reaches Snowflake through catalog-linked databases or catalog integrations. Use Delta Sharing for open-protocol sharing with other organizations. The key gotcha is that the reading engine sees files through vended credentials, so the owning catalog's row filters and masks may not travel with them. Tables with UC row filters or masks may not be externally readable at all **(⚠ verify)**. Share only already-protected gold tables (masked, aggregated, or pre-filtered), or re-apply policy in the reader.
- **Lineage:** Each platform captures lineage within itself. Across the boundary it breaks unless you stitch it, by emitting OpenLineage events from the orchestrator or using an enterprise catalog that harvests both. Assume there's a gap until you've verified native cross-platform lineage **(⚠ verify)**.
- **Auditing:** Land both platforms' audit logs (Databricks system tables, Snowflake `ACCESS_HISTORY`) in one store. Then "who read restricted columns last quarter" is a single query.
- **Failure mode: drift.** Someone makes a console hotfix in one engine, and now a column is masked in Snowflake but clear in Databricks. Mitigations:
  - Policy-as-code only, with console changes caught through audit logs.
  - A nightly drift job that diffs the live tags, policies, and grants in both engines against the repo.
  - CI tests that run as a test analyst principal against both engines and assert, for example, that email comes back masked.

**Evolution.** Move toward one catalog of record as federation matures, and organize around data products with named owners and SLAs. Make access requests self-service but tag-driven, so new data is governed by default.

## Comparison

**Single platform vs two**

| Factor | Single platform | Two platforms |
|---|---|---|
| Governance | One policy plane | Two planes; needs policy-as-code and drift detection |
| Cost | One bill, no duplicated data | Risk of duplicate storage and compute, plus egress |
| Skills and on-call | One stack | Broader skills, more runbooks |
| Workload fit | Some workloads are a compromise | Each workload on its strongest engine |
| Failure surface | Fewer integration points | Catalog sync, metadata refresh, identity mapping |
| Choose when | Default | Existing estate, sharing ecosystem, or a specific workload need |

**Lift-and-shift vs re-architect**

| | Lift-and-shift | Re-architect |
|---|---|---|
| Speed | Faster | Slower |
| Semantic risk | Lower (same logic) | Higher (logic changes) |
| Run cost afterwards | Often high | Lower (incremental, set-based) |
| Use for | Stable domains, deadline-driven waves | Top cost offenders, domains being redesigned anyway |

## Interview questions

**Q: Walk me through how you'd approach a design question.**

I start by clarifying consumers, the decisions they make, freshness and correctness needs, volumes, compliance, and team skills, and I state my assumptions out loud. Next I sketch sources → ingest → store → transform → serve → govern/observe, then go deep on the one or two hardest parts, which are usually streaming correctness or cutover. I finish with tradeoffs, failure modes and recovery, and how the design evolves at 10x. Each component gets a one-line "why" tied to a requirement.

**Q: How do you decide between a single platform and two?**

My default is one platform, because a second one doubles governance planes, skills, and integration failure points. I add a second platform only for a concrete reason: an existing estate with real users, a sharing ecosystem, or a workload one engine serves clearly better. When I do, I keep data in an open format with exactly one catalog of record per table, and I put policy-as-code and drift detection in place from day one. I also revisit the decision yearly, because feature gaps close.

**Q: How do you de-risk a warehouse migration?**

I don't migrate unused workloads, which I identify from usage logs. Foundations get built once, and domains move in waves, starting with a low-complexity one. Every wave runs in parallel with legacy for several cycles, including a month-end, and is reconciled through counts, sums, and row hashes against tolerances agreed in advance. Cutover is per domain, legacy stays read-only for a rollback window, and rollback is a rehearsed BI repoint. Business owners sign off before decommission.

**Q: What would you monitor first in a new platform?**

I monitor data SLAs first, meaning freshness and completeness measured at the gold outputs people actually use, because that's what users feel. Next come pipeline failures and retries, Kafka consumer lag and CDC slot lag, daily cost per workload with anomaly alerts, and reads of restricted data. Infrastructure metrics come after those. Every alert needs an owner and a runbook ([§14](14-reliability-dataops.md)).

**Q: Your design must cut cost by 40%; what do you change?**

I start by attributing cost per workload, because a handful usually dominate. Then I work down a menu:
- Delete unused pipelines and dashboards.
- Relax freshness where the decision doesn't need it (P7).
- Convert full refreshes to incremental.
- Improve pruning through clustering (P4).
- Right-size compute and auto-suspend it.
- Remove duplicate copies between platforms.

For each item I show the savings and the freshness or latency it gives up, so stakeholders choose the tradeoff knowingly ([§13](13-performance-cost.md)).

**Q: How do you handle a source system with no CDC capability?**

First I ask for log access or native change tracking. Failing that, I use a high-watermark extract on a reliable `updated_at` column, with an overlap window and dedup by key. That approach misses hard deletes, so I add a periodic full snapshot and diff row hashes to detect them. Longer term, I push for an outbox or event emission in the application. Every extract is idempotent (P6) and reconciled against source counts.

**Q: How do you get exactly-once results from a streaming pipeline?**

I aim for effectively-once. On the producer side I use idempotent writes and transactions. The processor checkpoints offsets and state together, so replay after failure is safe. The sink is idempotent: a MERGE on a natural key, a Delta sink that skips replayed batch IDs, or transactional commit. External side effects get idempotency keys, because no framework can un-send an email.

**Q: How do you handle late data when the same events feed real-time dashboards and finance?**

The real-time path uses event-time windows and a watermark, and its numbers are labelled provisional. The batch path MERGEs late rows by key and recomputes a lookback window, and it's the system of record. Finance closes at a cutoff, and later corrections post as adjustments. I also reconcile the two paths daily to catch logic drift.

## Takeaways
- Clarify first: requirements decide the architecture, and the products follow from them.
- Separate paths by decision. Seconds-level fraud and cent-exact finance need different pipelines (P7).
- The log (Kafka plus bronze) is the replay button. Design every step to be idempotent (P5, P6).
- Exactly-once is effectively-once: it requires replayable sources, checkpointed state, and idempotent sinks.
- Migrations succeed through waves, parallel runs, layered reconciliation, and a rehearsed rollback.
- Govern via IdP groups and tags, with one policy matrix generated into both engines and a nightly drift check (P9).
- One catalog of record per table, and never two writers (P11).

## Sources
- https://docs.snowflake.com/release-notes/2025/other/2025-10-17-iceberg-external-writes-cld-ga
- https://docs.databricks.com/aws/en/external-access/iceberg
- https://www.databricks.com/blog/read-unity-catalog-tables-in-snowflake
- https://www.databricks.com/blog/abac-row-filtering-and-column-masking-policies-governed-tags-and-data-classification-are-now
- https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac
- https://www.databricks.com/blog/introducing-lakebridge-free-open-data-migration-databricks-sql
- https://docs.databricks.com/aws/en/migration/lakebridge-agentic-converter
- https://docs.snowflake.com/migrations/migration-skill/skill
- https://www.snowflake.com/en/blog/engineering/real-time-change-data-capture-openflow/
