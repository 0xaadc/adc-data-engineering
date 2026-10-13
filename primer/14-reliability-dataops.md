# 14. Quality, Reliability, and DataOps

> As of October 2026. Tags: **(GA)** generally available · **(Preview)** public/private preview · **(⚠ verify)** not confirmed against official docs.

## Why this exists

Data fails silently. A job "succeeds" while loading zero rows, double-loading a day, or switching cents to dollars. Orchestrators check that code ran, not that data is correct. So correctness needs explicit checks, measurable SLAs and an owner who gets paged. DataOps applies software delivery discipline to data: version control, CI, environments, automated deploys, observability and incident process. The difference from software is that state, meaning the data, can't simply be rebuilt from source.

## Core concepts

### The data testing pyramid

```
   anomaly detection (volume/freshness/distribution drift)  <- unknown unknowns, noisy
  reconciliation (row counts, sums vs source)
 data tests (not null, unique, FK, accepted values)          <- every load
unit tests of transforms (fixture in -> expected out)       <- CI, no prod data
schema / contract tests (columns, types, compatibility)     <- cheapest, earliest
```

Lower layers are cheap, deterministic and run early. Upper layers catch problems nobody anticipated, but they produce false positives.

- **dbt:** generic and singular tests, unit tests, `store_failures`.
- **Lakeflow pipelines** (formerly DLT): expectations with three actions.
  - `expect` keeps rows and records metrics.
  - `expect_or_drop` drops violating rows.
  - `expect_or_fail` fails the update and rolls back atomically.
- **Great Expectations / Soda:** engine-agnostic checks, useful across both platforms.
- **Snowflake Data Metric Functions:** system DMFs (nulls, duplicates, freshness, row count) and custom ones. They run on a schedule against expectations, write results to an event table, and send notifications. Anomaly detection covers volume and freshness only **(Preview)**. Edition: Enterprise.
- **Databricks data quality monitoring** has two parts:
  - **Anomaly detection** builds per-table models of freshness and completeness, enabled per schema **(Preview)**.
  - **Data profiling** (formerly Lakehouse Monitoring) produces profile and drift metric tables.

### Block, warn or quarantine

Choose by the cost of bad data downstream.

- **Block** when wrong is worse than late, for example financial close or features feeding automated decisions. Fail the run and publish nothing.
- **Quarantine** when bad rows shouldn't hold back good ones. Route them to a quarantine table with a reason, then fix and replay (P6).
- **Warn** for soft signals such as distribution shift or a slight volume dip. Record the metric and notify the owner without stopping the run.

Place checks at the boundaries: contract and schema at ingestion, business rules before publish, freshness on serving tables.

### Observability and SLOs

The five pillars are **freshness, volume, schema, distribution and lineage**. The first four detect problems, and lineage scopes them (root cause upstream, blast radius downstream).

- **SLI:** what you measure, e.g. minutes between 06:00 and the successful publish of `orders_daily`.
- **SLO:** the internal target, e.g. published by 07:00 on 99% of days.
- **SLA:** the promise to consumers, with consequences if it's missed.

To avoid alert fatigue:
- Alert on SLO risk, not on every check.
- Route alerts to the owning team.
- Suppress downstream alerts when an upstream root cause already fired.
- Tier tables by consumer impact, and only page for tier 1.

**Incident flow:**
1. **Detect.** A check or a consumer report.
2. **Triage via lineage.** Find the upstream root cause and the downstream blast radius.
3. **Communicate early.** Post a status update, banner affected dashboards, and pause downstream publishes.
4. **Fix and backfill.** Use idempotent, partition-scoped reruns (P6).
5. **Blameless postmortem.** Its main output is the test that would have caught the issue.

### DR and multi-region

**RPO** is how much data you can afford to lose: the age of your last good copy. **RTO** is how long until service resumes. There are two different threats:

| Threat | Snowflake | Databricks |
|---|---|---|
| **Logical error** (bad DELETE, bad deploy) | Time Travel, `UNDROP`, Fail-safe (support-assisted recovery only); WORM **backups** **(GA, Dec 2025)**: point-in-time backups on all editions; with retention lock (Business Critical), not even ACCOUNTADMIN or ORGADMIN can delete them early. Details: [§06](06-snowflake-features.md) | Delta time travel and `RESTORE` (bounded by log retention and `VACUUM`); `UNDROP` for UC managed and external tables (7-day default recovery period) |
| **Regional outage** | Replication groups (databases and shares on all editions); failover groups (account objects plus promotion: **Business Critical**); Client Redirect connection URL (**Business Critical**) | **Managed disaster recovery** **(Preview, June 2026, gated)**: replicates UC metadata, managed table data and workspace assets; stable URL; failover reverses replication and pauses jobs in the old primary. Requirements on both workspaces: Enterprise plan (AWS) or Premium (Azure), plus the Mission Critical add-on (Preview, gated) and serverless compute. Otherwise DIY: deep clone or replication jobs, IaC to rebuild workspaces, re-create UC objects and grants. |

Points that come up in interviews:
- **Replication is not backup.** Replication faithfully copies a bad DELETE to the secondary. You need both.
- **Replication schedules set RPO.** A Snowflake secondary is read-only until you promote it. Cost is replicated storage, refresh compute and transfer.
- **Raw bucket replication can break Delta tables.** Copying a Delta table with storage-level replication alone doesn't preserve commit order, so a replica can reference files that haven't arrived yet. Prefer deep clone or managed DR, or validate the replica.
- **DR is more than data.** The IdP, private endpoints in the secondary region, secrets, the orchestrator and BI connection strings must fail over too. Rehearse with game days, or the RTO is fiction.

### CI/CD for data

**Environments.**
- *Snowflake:* separate accounts or databases per environment, and per-developer schemas. Use zero-copy clones of prod for realistic test data at metadata cost (P3), with masking still applied.
- *Databricks:* a catalog per environment, bound to its workspace.
  - **Shallow clone** copies metadata only and is cheap, but breaks if the source is VACUUMed.
  - **Deep clone** is an independent full copy, used for DR or a frozen test set.

**PR checks.**
- SQL lint.
- Unit tests.
- Build only what changed: `dbt build --select state:modified+ --defer --state <prod-manifest>`.
- Data diff against prod for critical models.

**Deployment.**
- *Databricks:* **Declarative Automation Bundles** (formerly Databricks Asset Bundles; renamed March 2026, with CLI and file names unchanged). YAML defines jobs, pipelines and per-target settings, deployed with `databricks bundle deploy -t prod`. Deep dive: [§20](20-declarative-automation-bundles.md).
- *Snowflake:* **DCM Projects** **(GA, Aug 2026)**: declarative `DEFINE` statements, plan then deploy, Jinja templating per environment. Alternatives are `schemachange` (versioned imperative migrations) and dbt for transformations.
- *Both:* Terraform providers for platform objects (workspaces, warehouses, roles, grants, policies). Split by rate of change: Terraform for slow-moving platform infrastructure; bundles, DCM or dbt for fast-moving data assets.

**Release patterns.**
- **Blue/green.** Build the new version alongside the old one, validate it, then switch consumers atomically. Snowflake uses `ALTER TABLE … SWAP WITH` or a view repoint; Databricks uses a view repoint.
- **Write-audit-publish (WAP).** Write to a location consumers can't see (staging table, clone, or an Iceberg branch), audit it with tests, then publish atomically by swap, MERGE or fast-forward. Failures never reach consumers.

**Schema migrations: expand/contract (P8).**
1. **Expand:** add the new column or table, nullable and backward compatible.
2. **Migrate:** backfill, dual-write, and move consumers over with notice.
3. **Contract:** drop the old structure after the deprecation window.

A breaking change is a new contract version, not an edit to the current one.

## Comparison

| Capability | Snowflake | Databricks |
|---|---|---|
| Native quality checks | Data Metric Functions + expectations | Pipeline expectations; data quality monitoring |
| Anomaly detection | DMF anomaly detection (volume, freshness) **(Preview)** | Anomaly detection (freshness, completeness) **(Preview)** |
| Test data | Zero-copy clone | Shallow / deep clone |
| Deploy tooling | DCM Projects **(GA)**, schemachange, Terraform, dbt | Declarative Automation Bundles, Terraform, dbt |
| Logical recovery | Time Travel, UNDROP, WORM backups **(GA)** | Time travel, RESTORE, UNDROP |
| Regional DR | Replication/failover groups + Client Redirect | Managed DR **(Preview)** or DIY |

## Interview questions

**Q: How do you ensure data quality in a pipeline?**

Use layers that match the pyramid. Contract and schema checks at ingestion, unit tests on transforms in CI, data tests on every load, reconciliation against the source, and anomaly detection for what nobody anticipated. Each check gets an action (block, quarantine or warn) chosen by the cost of bad data downstream, and every table has an owner who receives its alerts. Tests without owners and actions are just logs.

**Q: What is write-audit-publish?**

Write new data somewhere consumers can't see it, audit it with quality checks, and only then publish atomically. Mechanisms include a staging table, a clone or an Iceberg branch, published by swap, MERGE or fast-forward. Readers see either the old good state or the new validated state, never a half-written or failed one. The costs are extra storage and latency, so reserve WAP for tables where wrong data is expensive.

**Q: Design DR for a Snowflake + Databricks platform.**

Start from RPO and RTO per data tier, not one number for everything. Snowflake: replication groups for databases, failover groups (Business Critical) for account objects and promotion, and Client Redirect so clients follow the primary. Databricks: managed DR where it's available and fits, otherwise deep-clone replication plus IaC to rebuild workspaces and UC objects. Replicate the dependencies too (IdP, private endpoints, secrets, orchestrator), keep immutable backups for logical errors because replication copies mistakes, and run failover drills.

**Q: How do you structure dev/test/prod for data?**

Isolate environments by account or database (Snowflake) or by workspace-bound catalog (Databricks). Give each developer a schema. CI builds changed models against clones or deferred prod references. Test data comes from zero-copy or shallow clones so it's realistic and cheap, with masking enforced so non-prod doesn't become a PII leak. Deploys go through automation only (bundles, DCM, Terraform), so prod has no manual drift.

**Q: A dashboard showed wrong numbers this morning. Walk through your response.**

First, contain: banner the dashboard, notify consumers and pause downstream publishes. Then triage with lineage. Walk upstream from the dashboard and compare freshness, volume and schema at each hop until the first bad node, and walk downstream to find everything else affected. Fix the cause, backfill the affected partitions with idempotent reruns, and verify by reconciliation before lifting the banner. In the postmortem, add the check that would have caught it, and ask why the monitoring didn't.

**Q: How do you deploy a breaking schema change safely?**

Turn it into a sequence of non-breaking steps with expand/contract. Add the new structure alongside the old, backfill and dual-write, move consumers with notice using lineage to find them, and drop the old structure after a published deprecation window. Version the contract and gate producer CI on compatibility checks. When dual-write isn't possible, use a versioned table or view plus blue/green cutover.

**Q: Is replication a backup?**

No. Replication copies the current state, including your mistakes, usually within minutes. A backup is an independent point-in-time copy that can't be altered by the incident it protects against. Time Travel covers short-window logical errors. WORM backups or deep clones cover ransomware, insiders and longer retention.

**Q: How do you avoid alert fatigue in data observability?**

Alert on SLO risk for tables that have real consumers, not on every failed check. Route alerts to the owner, and suppress downstream alerts once an upstream root cause has fired. Tune anomaly detectors on history before turning on paging. Send warn-level signals to a digest instead of a pager.

## Takeaways

- Successful jobs can produce wrong data; correctness needs explicit checks and owners.
- The test pyramid runs from cheap and deterministic at the bottom to broad and noisy at the top; use every layer.
- Every check needs an action: block, quarantine or warn, chosen by downstream cost.
- RPO and RTO per tier. Replication handles regional outage; backups handle logical error.
- Databricks now has managed DR in preview; DIY is still the general case.
- Clones make realistic test environments cheap (P3).
- WAP and blue/green: consumers only ever see validated data.
- Breaking changes ship as expand → migrate → contract (P8).

## Sources

- https://docs.snowflake.com/en/user-guide/data-quality-intro
- https://docs.snowflake.com/en/user-guide/data-quality-notifications
- https://docs.databricks.com/aws/en/data-quality-monitoring
- https://docs.databricks.com/aws/en/data-quality-monitoring/anomaly-detection/
- https://docs.databricks.com/aws/en/ldp/expectations
- https://docs.snowflake.com/en/user-guide/replication-intro
- https://docs.snowflake.com/en/user-guide/client-redirect
- https://docs.snowflake.com/en/user-guide/backups
- https://docs.snowflake.com/en/release-notes/2025/other/2025-12-10-worm-backups
- https://docs.databricks.com/aws/en/admin/disaster-recovery
- https://docs.databricks.com/aws/en/admin/managed-disaster-recovery
- https://learn.microsoft.com/en-us/azure/databricks/release-notes/product/
- https://docs.snowflake.com/release-notes/2026/other/2026-08-07-dcm-projects-ga
- https://docs.databricks.com/aws/pt/release-notes/dev-tools/bundles
- https://docs.snowflake.com/en/user-guide/data-quality-anomaly
- https://docs.snowflake.com/en/release-notes/2025/9_36
- https://docs.databricks.com/aws/en/release-notes/product/2026/june
- https://learn.microsoft.com/en-us/azure/databricks/admin/managed-disaster-recovery
- https://docs.databricks.com/aws/en/release-notes/dev-tools/bundles
- https://docs.databricks.com/aws/en/dev-tools/bundles/faqs
- https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-undrop-table
