# 16. System Design Walkthroughs

> As of October 2026. Tags: **(GA)** generally available · **(Preview)** public/private preview · **(⚠ verify)** not confirmed against official docs.

## Why this exists

A design round has no single correct diagram, so interviewers grade the reasoning. They check whether you pin down requirements before naming tools, give each component a reason, name the tradeoff you're accepting, and know how the design breaks. A repeatable framework keeps you away from product-first answers and makes your thinking visible, so the interviewer can steer.

## Core concepts

### The answer framework

| Step | What you do | Time (45 min) |
|---|---|---|
| 1. Clarify | Consumers and decisions, freshness, volume, correctness, compliance, team, budget. State assumptions aloud. | 5–8 min |
| 2. High-level design | Sources → ingest → store → transform → serve → govern/observe. | 8–10 min |
| 3. Deep dive | The 1–2 hardest parts (streaming correctness, CDC, cutover). | 10–15 min |
| 4. Tradeoffs | What you chose, what you rejected, and why. | 5 min |
| 5. Failure modes and ops | What breaks, how you detect it, how you recover (replay, backfill, rollback). | 5 min |
| 6. Evolution | 10x volume, a new team, a new regulation. | 3 min |

Requirements come first because freshness and correctness are bought with money and complexity (P7). Failure modes are what separate an architect from a diagrammer.

### Requirements questions to ask

- **Consumers and decisions:** Who uses each output, and what decision does it drive (dashboard, finance close, ML feature, alert, external customer)?
- **Freshness:** Seconds, minutes, hours, or next day? Ask per consumer, because the answers differ.
- **Correctness:** Is approximate acceptable, or must it reconcile to the cent? Is at-least-once plus dedup enough?
- **Volume:** Average and peak events/sec, row counts, growth, burstiness (sales, month-end).
- **Sources:** Can each system emit CDC? Who owns it, and how often does its schema change (P8)?
- **Retention and deletion:** How much history, legal holds, right-to-be-forgotten.
- **Compliance:** PII/PCI/HIPAA, residency, audit evidence.
- **Access patterns:** Ad-hoc SQL, BI concurrency, millisecond lookups, ML training.
- **Team and budget:** SQL-first or Spark/Python-first? On-call capacity? Existing contracts, cloud, deadline?
- **Availability:** RPO/RTO, multi-region.

### Numbers to estimate

- Events/sec, average and peak (retail peaks are often 3–10x average). Event size: 0.5–2 KB as JSON, several times smaller as Avro/Protobuf.
- **GB/day raw** = average events/sec × 86,400 × size. Parquet is often 5–10x smaller than raw JSON.
- **Kafka disk** = GB/day × retention days × replication factor. **Partitions** ≈ peak MB/s ÷ per-partition consumer throughput, plus headroom.
- Yearly storage growth (hot vs cold) and BI concurrency (peak users × queries/min).

*Example:* 5k events/s on average, 30k at peak, 1 KB each. That's about 430 GB/day raw and roughly 50–80 GB/day as Parquet. Seven-day retention at RF=3 needs about 9 TB of Kafka disk, and peak ingress is about 30 MB/s. Ingest is not the cost risk here. BI concurrency and full-refresh transforms are.

### Walkthrough 1: Real-time + batch analytics (e-commerce)

**Assumptions.**
- Clickstream runs at 5k events/s on average and 30k at peak. Orders and payments live in Postgres.
- Fraud must flag a checkout within seconds; the decision is "hold the order". Ops GMV must be under one minute stale.
- Finance runs daily and must reconcile to the payment processor exactly.
- Cards are tokenized upstream, but PII still flows through the platform.
- Engineers know Databricks, and BI users already work in Snowflake.

```
 Web/App SDK ─────────────────────► Kafka: clickstream (Avro + schema registry)
 Postgres OLTP ─► Debezium (WAL) ─► Kafka: cdc.orders, cdc.payments
                                        │
          ┌─────────────────────────────┴───────────────────────────┐
          │ HOT PATH (seconds)                                      │ LAKEHOUSE PATH (minutes → daily)
          ▼                                                         ▼
  Stream processor (Flink or Spark SS)               BRONZE (Delta): raw topic copies,
   - join order + session context                      append-only, long retention = replay log
   - online features + fraud model                                  │
   - decision ─► Kafka: fraud.decisions                             ▼
   - 1-min GMV windows ─► ops dashboard               SILVER: typed, deduped, CDC applied
          │                                            (MERGE on PK, ordered by log position)
          ▼                                                         ▼
  Online feature store                               GOLD: conformed facts/dims, finance marts
                                                                    │ Iceberg (UniForm) via UC REST
                                                                    ▼
                                                     SNOWFLAKE: BI serving, semantic layer, sharing
  Cross-cutting: identity, tags, policies, lineage, audit (Walkthrough 3) · freshness/lag/cost monitors
```

**Choices and why.**
- **Kafka backbone (P5).** One replayable log feeds both paths and decouples producers from consumers, so any downstream can be rebuilt after a bug. Registry compatibility rules make schemas an enforced contract (P8). See [§10](10-kafka.md).
- **Debezium log-based CDC (P1).** It reads the WAL, so it captures deletes and adds no query load on the primary. Treat its output as at-least-once and dedup on (PK, log position). See [§09](09-batch-pipelines.md).
- **Separate hot and finance paths (P7).** Fraud needs seconds and tolerates approximation; finance needs exactness and tolerates a day. One pipeline that serves both is expensive and fragile.
- **Databricks lakehouse (P2, P3).** Object storage plus Delta MERGE and time travel for repair, with streaming and batch writing the same tables.
- **Snowflake serving.** Justified by existing BI users, high-concurrency dashboards, and sharing. Gold is exposed as Iceberg via the UC Iceberg REST endpoint and read through a Snowflake catalog-linked database. Catalog-linked databases for external Iceberg REST catalogs are **(GA)**; whether UC is a supported endpoint is **(⚠ verify)**. Copy into native tables only where performance needs it (P11, [§04](04-table-formats-catalogs.md)).
- **Or one platform.** If BI can run on Databricks SQL, you drop a hop, a governance plane, and a bill ([§08](08-snowflake-vs-databricks.md)).

**Deep dive: exactly-once.** In practice this means *effectively-once*: each event affects the output once, even if it's processed more than once. It must hold at three links:
1. **Producer → Kafka.** The idempotent producer dedups retries by producer ID and sequence. Transactions make multi-partition writes atomic.
2. **Processing.** The engine checkpoints offsets and state together and replays from the last checkpoint, so reprocessing is normal, not exceptional.
3. **Sink.** It must be idempotent or transactional:
   - A Structured Streaming Delta sink records batch IDs and skips replays.
   - A MERGE on `order_id`/`event_id` is idempotent.
   - Flink uses two-phase-commit sinks.
   - Kafka sinks need transactions, with consumers reading in `read_committed` mode.

External side effects (emails, payment calls) can't be exactly-once, so give them idempotency keys (P6). See [§11](11-stream-processing.md).

**Deep dive: late data.**
- Aggregate on event time. A watermark (say 10 minutes) bounds window state; events later than that are dropped from the live aggregate but still land in bronze.
- Silver MERGEs late rows by key. Gold recomputes a lookback window or only the partitions those rows touched.
- Finance closes at a cutoff, and later corrections post as next-period adjustments rather than silent rewrites.
- Tell stakeholders explicitly: real-time numbers are *provisional*, and batch numbers are *of record*.

**Medallion placement.**
- **Bronze** maps 1:1 to topics. Kafka keeps days; bronze keeps long-term replay.
- **Silver** applies CDC (current state plus SCD2 where needed) and tags or tokenizes PII.
- **Gold** holds facts, dimensions, and metric definitions, and is the contract with BI and Snowflake.
- The hot path reads Kafka directly. Its decisions go back to Kafka, so they also land in bronze for audit.

**Tradeoffs.**
- *Flink or Spark for the hot path:* Flink has lower per-event latency and richer event-time state. Spark keeps one engine and skill set, and micro-batches usually reach seconds.
- *Dual paths can compute a metric two ways:* compute shared features once, and reconcile real-time against batch daily.
- *Zero-copy or copy into Snowflake:* zero-copy avoids duplicate storage and sync jobs. A copy buys native performance.

**Failure modes.**
- *Consumer lag at peak:* size partitions for peak, autoscale consumers, and alert on lag in seconds.
- *Upstream schema change:* registry compatibility checks and source-side contract tests, with raw data in bronze for reprocessing.
- *Debezium slot falls behind:* Postgres retains WAL and can fill the primary's disk. Monitor slot lag and page both teams.
- *Bad deploy corrupts silver:* time-travel restore, then replay from bronze (P3, P5).
- *Fraud service down:* fail-open versus fail-closed is a business decision; keep a rules-engine fallback.
- *Stale Iceberg metadata in Snowflake:* monitor freshness at the boundary.

**Evolution.** Publish curated "gold streams" as Kafka data products. At 10x, add compaction and clustering ([§13](13-performance-cost.md)). Consolidate to one platform if the second one's reason disappears.

### Walkthrough 2: Migrating a legacy on-prem warehouse

**Assumptions.** SQL Server (or Teradata/Oracle), about 40 TB, around 1,500 SSIS packages and stored procedures, about 300 reports, a 06:00 nightly SLA, and a renewal deadline in 12–18 months. The target is Snowflake and/or Databricks.

```
 LEGACY                                      TARGET
 Sources ─► SSIS ─► SQL Server DW ─► BI      Sources ─► CDC/connectors ─► Raw/Bronze
                         │                                                 │
                         │ (1) bulk history export (Parquet)               ▼
                         └──────────────────────────────► Stage ─► Converted models (SQL/dbt/pipelines)
                                                                           │
     (2) PARALLEL RUN on the same inputs, ≥1 month-end                     ▼
     (3) RECONCILE: counts → sums → row hashes ◄──────────── Target marts ─► BI (repointed per domain)
     (4) CUT OVER by domain, legacy read-only    (5) DECOMMISSION after sign-off
```

1. **Assess.**
   - Inventory objects and mine query logs for real usage; a sizable share is usually dead, and the cheapest migration is the one you skip.
   - Build a dependency graph and score each object's complexity.
   - Tooling: Databricks Lakebridge is free and covers profiling, assessment, conversion, and reconciliation; its agentic converter is **(Preview)** (Beta). On the Snowflake side, SnowConvert AI and the Migration Agent in Cortex Code CLI cover similar ground **(⚠ verify)**.
2. **Lift-and-shift or re-architect, per domain** (see Comparison).
3. **Build foundations once:** network, identity and RBAC, CI/CD, orchestration, observability, budgets. Waves stall if each one rebuilds plumbing.
4. **Migrate in waves by domain.** Start with a medium-value, low-complexity domain that has a supportive owner. Each wave covers history load, conversion, pipelines, parallel run, reconciliation, BI repoint, and cutover.
5. **Parallel run and reconcile.** Run both systems on the same inputs for several cycles, including a month-end. Reconcile in layers:
   - row counts per partition
   - sums of key measures by day and dimension
   - row hashes on keys, to find the exact rows that differ
   - owner sign-off on the top reports

   Agree on tolerances first (zero for money). Usual culprits: collation, NULL semantics, time zones, decimal precision, implicit casts, `TOP` without `ORDER BY`.
6. **Cutover and rollback.** Legacy stays read-only for a fixed window. Rollback is a rehearsed BI repoint, which is cheap if BI connects through an alias or semantic layer. Decommission after sign-off.

**Dialect conversion.** Converters handle most syntax. Procedural code (cursors, temp-table chains, dynamic SQL) is the moment to rewrite into set-based SQL or Python. Validate with data, not just compilation.

**Orchestration.** Don't translate SSIS 1:1, because a package mixes extract, transform, and control flow. Split it:
- ingestion into connectors or CDC
- transforms into dbt, declarative pipelines, or dynamic tables
- control flow into an orchestrator (Airflow, Lakeflow Jobs, Snowflake tasks)

See [§09](09-batch-pipelines.md).

**BI repointing.** Compatibility views with the legacy names let reports change their connection, not their logic.

**People.** Pair legacy experts, who hold the business rules buried in procedures, with cloud engineers. They're the largest retention risk.

**Failure modes.**
- *Big-bang cutover:* cut over by domain instead.
- *Migrating dead workloads:* prune using usage logs.
- *Endless reconciliation debates:* agree tolerances up front.
- *Hidden ODBC/spreadsheet consumers:* find them in connection logs before shutting anything off.
- *Cost blowout from row-by-row procedures on consumption pricing:* re-architect the top offenders and set budgets from day one ([§13](13-performance-cost.md)).

**Evolution.** After cutover, make expensive models incremental, add streaming only where a decision needs it, and retire the compatibility views.

### Walkthrough 3: Unified governance across Snowflake and Databricks

**Assumptions.** Both platforms hold PII and financial data. Auditors want one answer to "who *can* see customer email, and who *did*?"

```
                 IdP (Entra ID / Okta): users, groups, service principals
                    │ SCIM                                   │ SCIM
                    ▼                                        ▼
        Databricks account + Unity Catalog          Snowflake account (roles)
                    ▲  Terraform/CI ◄── policy-as-code repo: tags, ──► Terraform/CI ▲
                    │                   policy matrix, grants, owners               │
        UC: governed tags, ABAC row filters /       Snowflake: object tags, tag-based masking,
        column masks, data classification            row access policies, classification
                    └──── shared gold: Iceberg REST (UC → catalog-linked DB) ────┘
        system tables (audit, lineage)               ACCOUNT_USAGE (ACCESS_HISTORY, etc.)
                    └──────────► central audit + lineage store + drift detector ◄──────┘
```

- **Identity (P9).** The IdP is the source of truth, and SCIM provisions both platforms. Grant to groups, never individuals, with one service principal per pipeline. Joiners, movers, and leavers are handled once.
- **Catalog of record (P11).** Each table has exactly one owning catalog, the one where it's written. For example, UC owns bronze through gold and Snowflake reads gold, while Snowflake owns its native marts. Never two writers. The catalog is the lock-in point, so choose deliberately ([§04](04-table-formats-catalogs.md)).
- **Tag taxonomy.** A small controlled vocabulary, defined once in a repo and applied by automation plus classifiers:
  - `classification` (public / internal / confidential / restricted)
  - `pii_type`
  - `domain`
  - `owner`
  - `retention`

  UC governed tags and data classification are **(GA)**, and Snowflake has sensitive-data classification.
- **Policy enforcement.** Bind policies to tags, not tables, so newly tagged columns are protected automatically. UC uses ABAC policies on governed tags for row filters and masks **(GA)**; Snowflake uses tag-based masking and row access policies. Keep one *policy matrix* (tag × group → clear / masked / hashed / denied) and generate both implementations from it ([§12](12-governance-security.md)).
- **Sharing.** Within the company, Iceberg REST via catalog-linked databases or catalog integrations. Delta Sharing for open-protocol sharing to other organizations. The gotcha: readers access files with vended credentials, so the owning catalog's row filters and masks may not travel, and filtered or masked UC tables may not be externally readable at all **(⚠ verify)**. Share already-protected gold (masked, aggregated, pre-filtered), or re-apply policy in the reader.
- **Lineage.** Each engine captures lineage within itself. Across the boundary, stitch it with OpenLineage events from the orchestrator or an enterprise catalog that harvests both. Assume a gap until you've verified native cross-platform lineage **(⚠ verify)**.
- **Auditing.** Land Databricks system-table audit logs and Snowflake `ACCESS_HISTORY` in one store, so "who read restricted columns" is a single query.
- **Failure mode: drift.** A console hotfix leaves a column masked in Snowflake but clear in Databricks. Mitigations:
  - Policy-as-code only, with console changes caught in audit logs.
  - A nightly job diffing live tags, policies, and grants against the repo.
  - CI tests that run as a test analyst on both engines and assert email comes back masked.

**Evolution.** Move toward one catalog of record as federation matures. Organize around data products with named owners and SLAs, and make access requests self-service but tag-driven, so new data is governed by default.

## Comparison

**Single platform vs two**

| Factor | Single platform | Two platforms |
|---|---|---|
| Governance | One policy plane | Two; needs policy-as-code and drift detection |
| Cost | One bill, no duplicated data | Possible duplicate storage and compute, plus egress |
| Skills and on-call | One stack | Broader skills, more runbooks |
| Workload fit | Some workloads are a compromise | Each workload on its strongest engine |
| Failure surface | Fewer integrations | Catalog sync, metadata refresh, identity mapping |
| Choose when | Default | Existing estate, sharing ecosystem, or a specific workload need |

**Lift-and-shift vs re-architect**

| | Lift-and-shift | Re-architect |
|---|---|---|
| Speed | Faster | Slower |
| Semantic risk | Lower (same logic) | Higher (logic changes) |
| Run cost afterwards | Often high | Lower (incremental, set-based) |
| Use for | Stable domains, deadline-driven waves | Top cost offenders, domains being redesigned anyway |

## Interview questions

**Q: Can you walk me through how you'd approach a design question?**

I clarify consumers, the decisions they make, freshness and correctness needs, volumes, compliance, and team skills, and I state my assumptions aloud. I sketch sources → ingest → store → transform → serve → govern, then go deep on the hardest one or two parts. I finish with tradeoffs, failure modes and recovery, and evolution at 10x. Each component gets a one-line "why" tied to a requirement.

**Q: How do you decide between a single platform and two?**

My default is one, because a second platform doubles governance planes, skills, and integration failure points. I add one only for a concrete reason: an existing estate with real users, a sharing ecosystem, or a workload one engine serves clearly better. If I do, data stays in open formats with one catalog of record per table, and policy-as-code plus drift detection start on day one. I revisit the decision yearly, because feature gaps close.

**Q: How do you de-risk a warehouse migration?**

I skip unused workloads, build foundations once, and migrate in domain waves starting with a low-complexity one. Each wave runs in parallel with legacy through at least one month-end and is reconciled through counts, sums, and row hashes against pre-agreed tolerances. Cutover is per domain, with legacy read-only and a rehearsed BI-repoint rollback. Decommission waits for owner sign-off.

**Q: What would you monitor first in a new platform?**

Data SLAs first: freshness and completeness at the gold outputs people use, because that's what users feel. Then pipeline failures, Kafka consumer lag and CDC slot lag, daily cost per workload with anomaly alerts, and reads of restricted data. Infrastructure metrics come after. Every alert needs an owner and a runbook ([§14](14-reliability-dataops.md)).

**Q: Your design must cut cost by 40%; what do you change?**

First I attribute cost per workload, since a few usually dominate. Then I work through a menu:
- Delete unused pipelines and dashboards.
- Relax freshness the decision doesn't need (P7).
- Convert full refreshes to incremental.
- Improve pruning with clustering (P4).
- Right-size compute and auto-suspend it.
- Remove duplicate copies between platforms.

Each item shows its savings and what it costs in freshness, so stakeholders choose knowingly ([§13](13-performance-cost.md)).

**Q: How do you handle a source system with no CDC capability?**

First I ask for log access or native change tracking. Otherwise I use a high-watermark extract on a reliable `updated_at`, with an overlap window and dedup by key. That misses hard deletes, so I add periodic full snapshots and diff row hashes to detect them. Longer term, I push for an outbox or event emission in the application. Every extract is idempotent (P6) and reconciled against source counts.

**Q: How do you get exactly-once results from a streaming pipeline?**

I aim for effectively-once. Producers use idempotent writes and transactions. The processor checkpoints offsets and state together, so replay is safe. Sinks are idempotent: a MERGE on a natural key, a Delta sink that skips replayed batches, or a transactional commit. External side effects get idempotency keys.

**Q: How do you handle late data when the same events feed real-time dashboards and finance?**

The real-time path uses event-time windows and a watermark, and its numbers are labelled provisional. The batch path MERGEs late rows and recomputes a lookback window, and it's the system of record. Finance closes at a cutoff, and later corrections post as adjustments. The two paths are reconciled daily to catch logic drift.

## Takeaways
- Requirements decide the architecture; products follow.
- Separate paths by decision: seconds-level fraud and cent-exact finance need different pipelines (P7).
- Kafka plus bronze is the replay button, so make every step idempotent (P5, P6).
- Exactly-once means effectively-once: replayable sources, checkpointed state, idempotent sinks.
- Migrations succeed through waves, parallel runs, layered reconciliation, and a rehearsed rollback.
- Govern via IdP groups and tags, with one policy matrix generated into both engines and a nightly drift check (P9).
- One catalog of record per table, never two writers (P11).

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
