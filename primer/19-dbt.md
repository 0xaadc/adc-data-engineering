# 19. dbt: Analytics Engineering as Code

> As of October 2026. Tags: **(GA)** generally available · **(Preview)** public/private preview · **(⚠ verify)** not confirmed against official docs.

## Why this exists

Once elastic warehouses made ELT the default (P2), business logic moved into SQL inside the platform: stored procedures, scheduled scripts, BI-tool queries. Nothing recorded which table depended on which, so run order was hand-maintained. There were no tests, no code review, no dev/prod separation and no docs. Transformation became the team's largest codebase, with none of the practices of software engineering.

dbt's answer is a simple contract. Each transform is a `SELECT` in a file, and dependencies are declared inside the SQL with `ref()`, so the dependency graph (DAG) is derived from the code. Git, pull requests, tests, docs and environments then apply as they would to any code.

The key point for an architect: **dbt moves no data.** It is a compiler and runner. It turns SQL plus Jinja into DDL/DML, sends it to the engine in dependency order, and waits. Cost and performance come from the warehouse and from the SQL dbt emits. Choosing where to run dbt means choosing where a small control process lives and who operates its surroundings (P10).

## Core concepts

### Project anatomy and the DAG

```
my_project/
├── dbt_project.yml          # project name, folder-level default configs
├── profiles.yml             # connection targets (dev, ci, prod); often outside the repo
├── packages.yml             # dependencies, installed by `dbt deps`
├── models/
│   ├── staging/stg_orders.sql
│   ├── marts/fct_orders.sql
│   └── marts/_marts.yml     # descriptions, tests, contracts
├── snapshots/  seeds/  macros/  tests/
```

A model is a `SELECT`. At compile time, `{{ ref('stg_orders') }}` and `{{ source('shop', 'orders') }}` resolve to a fully qualified name for the current **target** and record a DAG edge. The graph is derived from the code, so it can't drift from it. And because names resolve per target, the same code builds into `dev_<user>.fct_orders`, a CI schema or `prod.fct_orders` (via `generate_schema_name`). A hardcoded table name defeats both mechanisms.

### Compile, execute, artifacts

1. **Parse.** Read YAML and SQL and build the graph into `manifest.json`: every node, its config and a checksum of its code.
2. **Compile.** Render Jinja and replace each `ref()` with a real relation name.
3. **Materialize.** Wrap the compiled `SELECT` in its materialization's DDL/DML. The **adapter** (dbt-snowflake, dbt-databricks…) speaks the platform's dialect and submits statements in DAG order, N threads at a time.
4. **Record.** `run_results.json` (status, timing), `catalog.json` (columns read back from the warehouse), `sources.json` (freshness).

These artifacts are dbt's interface to everything else: slim CI, docs, lineage tools and orchestrators read them. dbt v2 adds **static analysis** between compile and execute: it builds a logical plan for every query without touching the warehouse, which catches type and column errors early and yields column-level lineage.

### Materializations: what SQL dbt emits

| Materialization | What dbt runs | Use when |
|---|---|---|
| `view` | `CREATE VIEW` | Cheap to build; you pay on every read. Staging layers |
| `table` | `CREATE OR REPLACE TABLE AS SELECT` | Full rebuild each run; correct by construction; cost O(table) |
| `incremental` | First run: CTAS. Later: filtered SELECT, then MERGE/INSERT/DELETE into the target | Large facts where a rebuild is too expensive |
| `ephemeral` | Nothing; inlined as a CTE into downstream models | Reusable logic with no object of its own |
| `dynamic_table` (dbt-snowflake) | `CREATE DYNAMIC TABLE … TARGET_LAG` | The engine handles incremental refresh ([§06](06-snowflake-features.md)) |
| `materialized_view`, `streaming_table` (dbt-databricks) | Databricks SQL MV/ST, run by Lakeflow pipelines | The engine handles incremental refresh; needs Unity Catalog and serverless SQL warehouses |
| `metric_view` (dbt-databricks) | `CREATE VIEW … WITH METRICS LANGUAGE YAML` | Unity Catalog metric views managed from dbt |

The platform-native materializations hand incrementality to the engine. dbt then becomes the authoring, testing and deployment layer. One subtlety: if you don't set `target_lag` on a dbt `dynamic_table`, dbt creates it with `scheduler = DISABLE` and refreshes it during `dbt run`. That is the opposite of Snowflake's native default.

### Incremental models

`is_incremental()` is true only when the target already exists, the model is `materialized='incremental'`, and `--full-refresh` was not passed. The SQL must therefore be valid both ways: a full build or a delta. `{{ this }}` refers to the existing target.

```sql
{{ config(
    materialized         = 'incremental',
    unique_key           = 'order_id',
    incremental_strategy = 'merge',
    on_schema_change     = 'append_new_columns'
) }}
select order_id, customer_id, status, amount, updated_at
from {{ ref('stg_orders') }}
{% if is_incremental() %}
  -- 3-day lookback re-reads late or updated rows; MERGE on unique_key makes the overlap harmless (P6)
  where updated_at > (select dateadd(day, -3, max(updated_at)) from {{ this }})
{% endif %}
```

The model's filter finds the delta. The **strategy** decides how the delta is applied:

| Strategy | Mechanism | Snowflake | Databricks | Failure mode |
|---|---|---|---|---|
| `append` | INSERT | ✓ | ✓ | Retries duplicate rows (not idempotent) |
| `merge` | MERGE on `unique_key` | ✓ default | ✓ default | Duplicate keys in a batch: Snowflake raises "nondeterministic merge" |
| `delete+insert` | Delete matching keys, then insert | ✓ | ✓ | Two statements; tolerates duplicate keys |
| `insert_overwrite` | Overwrite | Replaces the **whole table** | Replaces only the partitions/clusters present in the batch (`INSERT … REPLACE ON`) | Same name, different semantics across platforms |
| `replace_where` | Replace rows matching predicates | n/a | ✓ (Delta) | Inserts by column position |
| `microbatch` | One query per `event_time` batch, each replacing its window | ✓ | ✓ (via `replace_where`) | Full scans if upstream models lack `event_time` |

**Microbatch** (dbt v1.9+) moves the "which window?" question from your SQL into config: `event_time`, `batch_size`, `begin` and `lookback` (default one batch). Because each batch replaces exactly its own window, retries and backfills (`--event-time-start/--event-time-end`) are the same idempotent operation (P6), and failed batches can be retried on their own.

**Schema drift** is handled by `on_schema_change`: `ignore` (the default), `fail`, `append_new_columns` or `sync_all_columns`. None of them backfill the new column for old rows (P8). `--full-refresh` drops and rebuilds the table. Setting `full_refresh: false` on huge tables protects them from a stray flag.

### Snapshots

A snapshot builds SCD2 history by comparing source rows with the last snapshot: the `timestamp` strategy trusts `updated_at`, while `check` compares `check_cols` when no timestamp can be trusted. It sees only the state at run time, so changes between runs are lost; log-based CDC is the fix ([§09](09-batch-pipelines.md)). Modeling details are in [§03](03-data-modeling.md).

### Tests, contracts and versions (P8)

- **Data tests** compile to a `SELECT` returning failing rows; zero rows means pass. Generic tests (`unique`, `not_null`, `accepted_values`, `relationships`) are declared in YAML; singular tests are SQL files. `severity` and `store_failures` make them actionable.
- **Unit tests** (v1.8+) mock a model's inputs in YAML and assert its output. They test logic, not data, and can force `is_incremental()` to test both modes. Run them in CI, not prod.
- **Model contracts** (v1.5+, `contract: {enforced: true}`) make dbt check column names and types *before* building and fail on a mismatch; constraints go into DDL where the platform supports them. A test checks data after the build.
- **Model versions** (v1.5+) keep `ref('dim_customer', v=2)` beside v1 through a deprecation window: expand/contract ([§14](14-reliability-dataops.md)) for models. **Access** modifiers (`private`, `protected`, `public`) and **groups** control who may `ref` a model.

### Seeds, macros, packages and docs

**Seeds** are small version-controlled CSVs (mappings, code lists), not an ingestion tool. **Macros** are Jinja functions that emit SQL: dbt's abstraction mechanism and its main source of unreadable code. **Packages** (`dbt_utils` and others) install with `dbt deps`. **Docs** combine YAML descriptions with the manifest and catalog into a lineage site; in v2 it is a static site with column-level lineage.

### State, defer and slim CI

Because the manifest stores a checksum per node, dbt can diff the project against prod's manifest. `--select state:modified+` picks changed nodes and everything downstream. `--defer --state <prod-artifacts>` resolves refs to *unselected* nodes against prod relations, so a PR builds only what changed, in its own schema, reading upstream data from prod (P4). See [§14](14-reliability-dataops.md).

**dbt State** **(GA, Sept 2026)**, the successor to "state-aware orchestration", decides per node at run time. It compares each node's logic hash and upstream data freshness with earlier builds in any environment. It then skips the node, clones an identical build from another schema (P3), or rebuilds it. `lag_tolerance` caps how often a node rebuilds when its inputs keep changing. dbt State works with v1.7+, v2 and external orchestrators, but needs a dbt platform login and is billed per daily active target table.

### The ways to run dbt

Every option has two parts: a control process that parses, compiles, orders the DAG and submits SQL, and an engine that runs the SQL. In all four options below the SQL runs on your warehouse. What differs is where the control process runs, who operates its surroundings (scheduling, CI, secrets, docs, artifacts), and what that costs.

**1. Self-run CLI (dbt v1, dbt v2 or dbt OSS).** You install dbt in your own container, VM or CI runner: `pip install dbt-core dbt-<adapter>` for v1, or `pip install dbt` for v2, a single Rust binary with built-in adapters on ADBC drivers and no Python. Anything can orchestrate it: Airflow, Dagster, GitHub Actions, a Lakeflow Jobs task, a Snowflake Task. You own scheduling, retries, the prod-artifact store for slim CI, docs hosting, secrets and upgrades. The runner is cheap; operating time is the real cost. Lock-in is lowest, but check the license: v1 and dbt OSS are Apache 2.0, while dbt v2 is free but proprietary.

**2. dbt platform (formerly dbt Cloud).** dbt Labs runs the control process on its multi-tenant or cell-based infrastructure and connects to your warehouse over the network (IP allowlists or private connectivity). You get:
- **Studio IDE**, a browser IDE, and **Canvas** (Enterprise plans), visual drag-and-drop models that compile to ordinary SQL in Git.
- **Jobs and environments**, with release tracks that manage the dbt version.
- **CI jobs**: builds in a per-PR schema (`dbt_cloud_pr_<job_id>_<pr_id>`, dropped on close), with deferral and linting.
- **Catalog** (formerly Explorer), with column-level lineage.
- **Semantic Layer** (MetricFlow metrics served over APIs) and **Mesh** cross-project `ref()` (Enterprise plans).
- **dbt State**, plus AI: **Copilot** (inline generation) and **dbt Wizard** (agent in Studio, **Preview**).

Pricing has three parts: **seats**, **usage** and metered **add-ons**. Usage is *Successful Models Built* in deployment environments (dev runs, tests, seeds, snapshots and ephemeral models don't count) plus Semantic Layer *Queried Metrics*. dbt State is billed per daily active target table and Wizard per token. The free Developer plan includes 3,000 models a month, Starter 15,000, and Enterprise terms are negotiated. Your code stays portable; the workflow (CI, Catalog, metric serving, cross-project refs) does not.

**3. dbt on Databricks.**
- **The adapter.** `dbt-databricks` lives in the Databricks GitHub org and is a Databricks-tuned fork of dbt-spark; dbt v2 ships a built-in Databricks adapter **(GA)**. It connects through an `http_path` to a SQL warehouse or all-purpose cluster (compute selectable per model) and maps dbt's `database` to a Unity Catalog catalog.
  - Configs: `liquid_clustered_by`/`auto_liquid_cluster` (dbt runs `OPTIMIZE` after each build unless skipped), `tblproperties`, `databricks_tags` (applied by ALTER; dbt can't remove them), `column_mask`, `row_filter`, `query_tags` (v1.11+).
  - MV/ST materializations run as Lakeflow pipelines with `schedule` options (cron, every, on update). Python models run on clusters or serverless.
- **The dbt task in Lakeflow Jobs.** Databricks runs the dbt CLI on "dbt CLI compute": serverless jobs compute by default when UC and serverless jobs are enabled, otherwise a single-node job cluster. The SQL runs on a serverless or pro SQL warehouse, or on an all-purpose cluster through a custom `profiles.yml`; job compute can't be the target.
  - The project comes from a Git provider or a workspace folder. Commands run in sequence (`dbt deps`, `seed`, `run` by default).
  - Databricks generates the profile from the chosen warehouse, catalog and schema, or reads yours, with `env_var('DBT_ACCESS_TOKEN')` supplying a short-lived token for the *Run As* principal.
  - Each run archives its artifacts (`dbt-output.tar.gz` via `runs/get-output`).
  - The default library is `dbt-databricks>=1.0.0,<2.0.0`, the Python v1 runtime, so pin an exact version. Running dbt v2 in this task isn't documented **(⚠ verify)**.
  - You still own slim-CI state and docs hosting. For the task as code, see [§20](20-declarative-automation-bundles.md).
- **The dbt platform connected to Databricks.** Partner Connect creates a service principal, a token and a `DBT_CLOUD_ENDPOINT` SQL warehouse, or you configure OAuth by hand. The **dbt platform task** in Lakeflow Jobs **(Preview)** triggers an existing dbt platform job through a Unity Catalog connection, with retries and monitoring. Databricks orchestrates; the dbt platform executes.
- **No native project object.** As of October 2026, Databricks has no equivalent of Snowflake's DBT PROJECT object or a dbt-specific workspace IDE. Its integration is the adapter plus these two task types.

**4. dbt Projects on Snowflake** **(GA, Nov 2025).** You develop in **Snowflake Workspaces** or Git. The project is deployed as a schema-level, RBAC-governed `DBT PROJECT` object (`CREATE DBT PROJECT … FROM <workspace|stage|Git repo>` or `snow dbt deploy`) and run in a Snowflake-managed runtime:

```sql
EXECUTE DBT PROJECT analytics.ops.core_models ARGS = 'build --target prod';
```

- **Version.** `DBT_VERSION` picks the engine: dbt Core 1.9.4, 1.10.15, 1.11.11 or 1.12.3, or dbt Fusion/v2 2.0.0. `DEFAULT_DBT_VERSION` sets the account default.
- **Cost.** Models run on the `profiles.yml` warehouse. Up to two warehouses bill per run (the calling session's and the profile's), with no license fee.
- **Scheduling.** Tasks on a user-managed warehouse (serverless tasks can't run it) or Airflow.
- **Observability and CI.** Logs go to the event table. Slim CI imports the last successful prod artifacts (`SYSTEM$DBT_GET_LAST_SUCCESSFUL_RUN_TARGET`).
- **Limits.**
  - Only dbt Core and Fusion projects run; dbt platform features don't.
  - Only the listed commands are supported (`build`, `run`, `test`, `seed`, `snapshot`, `compile`, `deps`, `docs generate`, `retry`, `show`…).
  - Each project folder holds at most 100,000 files.
  - Workspaces show stdout only when the command completes.
  - The DAG view shows 300 models at a time.

### dbt v2 (formerly Fusion), licensing and the merger

Fusion, the Rust rewrite, is now **dbt v2** **(GA, Sept 2026)** on Snowflake, Databricks, BigQuery and Redshift; Spark is Beta (CLI only). dbt Core v1.x is now "dbt v1" and still supported.

What v2 changes:
- **Strict parsing.** Misspelled configs and unknown keys, silently ignored in v1, are now errors.
- **Faster parsing** on large projects.
- **New tooling:** static analysis (baseline by default; `strict` needs `dbt login`), `dbt lint`, project checks, batched tests and model freshness.
- **Compatible state.** It writes a v12 manifest that v1 reads, so v1 and v2 jobs can defer to each other during a migration.

**Licensing:**
- **dbt v1:** Apache 2.0.
- **dbt OSS** (`dbt-oss`): the Apache 2.0 v2 runtime (language, DAG semantics, commands).
- **dbt v2** (`dbt`): free but proprietary. It adds SQL comprehension and static analysis, the LSP, `dbt lint` and the VS Code extension.

The Fivetran and dbt Labs merger closed June 1, 2026, the same day dbt State was announced ([§09](09-batch-pipelines.md)).

### dbt vs native declarative transforms

With a dbt incremental, *you* write the delta logic. With Dynamic Tables and Lakeflow pipelines, the *engine* derives the delta from the query and the source's change tracking, and also owns scheduling. Native means lower latency and fewer moving parts; dbt means portability, tests, docs and a vendor-neutral DAG (P10, P11). The common hybrid: author in dbt, materialize latency-sensitive models as `dynamic_table` or `streaming_table`/`materialized_view`. The dbt platform counts such a model once at creation, and later engine refreshes aren't "models built". See [§09](09-batch-pipelines.md) and [§15](15-why-x-over-y.md).

### Failure modes

- **Incremental logic misses late or updated rows.** A `max(updated_at)` filter skips late commits and backdated updates. Fix: lookback plus idempotent `merge` (or microbatch `lookback`), and periodic reconciliation.
- **Surprise full-refresh cost.** `--full-refresh -s model+` rebuilds every large table downstream, and Snowflake `insert_overwrite` rewrites the whole table. Fix: `full_refresh: false` and `on_schema_change: fail` on big tables.
- **Model sprawl and slow DAGs.** Thousands of one-off models, long view chains recomputed on every read, slow parses. Fix: enforce layering (staging, intermediate, marts), lint the project's structure, split by domain.
- **Jinja spaghetti.** Macros calling macros until the SQL is unreadable. Fix: keep logic in SQL, use macros for repeated patterns only, review the *compiled* SQL.
- **Tests that are all noise, or none at all.** Ignored warnings train people to ignore alerts. Fix: test keys, sources and invariants, and give each test an owner and a severity.
- **Hardcoded names that bypass `ref()`.** The DAG loses an edge (wrong build order), CI touches prod, and lineage has gaps. Fix: lint rules or v2 checks.
- **Unattributable spend.** Every dbt query looks alike in query history. Fix: Snowflake `query_tag` (session-level and reset after each model, so a mid-model failure can mis-tag later queries), Databricks `query_tags` (`@@dbt_model_name` added automatically), and a dedicated warehouse per job.
- **One giant project vs Mesh boundaries.** One repo for every team means ownership collisions, slow CI and a large blast radius. Mesh splits by domain with `public` models, contracts and versions as the interface. But cross-project refs are an Enterprise dbt platform feature, and too many projects bring coordination overhead.

## Comparison

**Ways to run dbt**

| | Self-run CLI (v1 / v2 / OSS) | dbt platform | Databricks dbt task | dbt Projects on Snowflake |
|---|---|---|---|---|
| dbt process runs on | Your container, VM or CI runner | dbt Labs infrastructure | Databricks jobs compute (serverless or single node) | Snowflake managed runtime |
| SQL runs on | Any supported warehouse | Any supported warehouse | SQL warehouse (serverless or pro) or all-purpose cluster | Warehouse in `profiles.yml` |
| Scheduling | Yours (Airflow, Dagster, Actions…) | Built-in jobs; triggerable from Lakeflow Jobs or Airflow | Lakeflow Jobs triggers, repair runs | Tasks (user-managed warehouse) or Airflow |
| Development | Local IDE, VS Code extension | Studio IDE, Canvas, extension | Local CLI against a SQL warehouse; Git folders | Snowflake Workspaces |
| Slim CI / state | Build it; store prod manifest | CI jobs, PR schemas, dbt State | Build it | Import last prod artifacts via system functions |
| Docs and lineage | Self-hosted dbt docs | Catalog (column level) | UC lineage on outputs; dbt docs self-hosted | Project page, Horizon column lineage |
| Credentials | Yours to manage | Stored in platform | Short-lived token for the Run As principal | Runs as the calling role |
| dbt version | You pin | Release tracks | You pin `dbt-databricks` (default <2.0) | `DBT_VERSION` per object (1.9–1.12, 2.0) |
| Extra cost beyond warehouse | Runner plus ops time | Seats + models built (+ metrics, State, AI) | DBUs for dbt CLI compute | None |
| Semantic Layer serving, Mesh refs | Not built in | Yes (plan dependent) | No | No |
| Multi-platform | Yes | Yes | Databricks only | Snowflake only |
| Lock-in | Lowest | Workflow and metadata | Orchestration | Orchestration and object model |

**Choose when**
- **Self-run**: a strong platform team with an existing orchestrator, or strict license, network or residency rules. You own CI, docs and state.
- **dbt platform**: many analysts, several warehouses, or a need for the Semantic Layer, Mesh, managed CI or dbt State without platform-team effort. You pay seats and usage.
- **Databricks dbt task**: dbt is one step in a Databricks-centric DAG (ingest, transform, ML), with Databricks-native identity and no need for platform features.
- **dbt Projects on Snowflake**: a Snowflake-only estate that wants no extra vendor or infrastructure and schedules with Tasks.

## Interview questions

**Q: What is dbt and what problem does it solve?**

dbt is a compiler and runner for SQL transformations: each model is a `SELECT`, `ref()` builds the DAG, and dbt emits DDL/DML for the engine to run in order. It exists because ELT (P2) moved business logic into warehouse SQL with no dependency graph, tests, review, environments or docs. It moves no data, so cost and performance belong to the warehouse. It is not an ingestion tool, a stream processor or a cross-system orchestrator.

**Q: dbt platform vs dbt Core: when would you pay?**

You pay to stop operating the surroundings: IDE, scheduler, CI with PR schemas and deferral, a hosted catalog and dbt State. You also get features with no self-run equivalent, such as Semantic Layer serving and Mesh cross-project refs (P10). Usage pricing grows with model count and run frequency, so high-frequency jobs need watching. Self-run wins when a platform team already runs an orchestrator and CI, or when license or network rules forbid a SaaS control plane. The code is portable either way; the lock-in is in the workflow.

**Q: How would you run dbt on Databricks, and what are the options?**

All options use the Databricks adapter against a SQL warehouse, with Unity Catalog as the namespace. One: the dbt task in Lakeflow Jobs, which runs the CLI on jobs compute and sends SQL to a serverless or pro warehouse with a short-lived Run As token. Two: the dbt platform connected to Databricks, optionally triggered by the dbt platform task in Lakeflow Jobs (Preview). Three: self-run from Airflow or CI. Choose by where orchestration and governance should live: the dbt task if dbt is one step in a Databricks pipeline, the dbt platform if many analysts or several engines are involved.

**Q: dbt task in Lakeflow Jobs vs dbt platform jobs?**

The dbt task keeps everything in Databricks: one DAG with ingestion and ML, repair runs, Run As identity, DBU billing and archived artifacts. But you build slim CI, state storage and docs yourself, and the default runtime is Python v1. dbt platform jobs bring managed CI, deferral, dbt State and Catalog, at the price of seats, per-model usage and a second control plane that needs network access to your warehouse. The dbt platform task bridges the two. Make sure two schedulers never trigger the same models.

**Q: dbt Projects on Snowflake vs dbt platform?**

dbt Projects on Snowflake runs dbt inside Snowflake. The project is an RBAC-governed object, it runs as the calling role with no external credentials, Tasks schedule it, and the only cost is warehouse credits, which is strong for Snowflake-only teams and security reviews (P9). You give up dbt platform features (Semantic Layer serving, cross-project Mesh, Catalog), get only the listed commands, and tie orchestration to one platform. Pick it for a lean, single-platform estate; pick the dbt platform when you span engines or need those features.

**Q: How do incremental models work, and how do they fail?**

On later runs, `is_incremental()` enables a filter that selects the delta, and the strategy applies it: merge on `unique_key`, delete+insert, partition overwrite or microbatch windows. The common failure is a filter that misses late or backdated rows; the fix is a lookback, which is safe only because merge or window replacement is idempotent (P6). Other failures: duplicate keys break merge, `append` duplicates on retry, Snowflake's `insert_overwrite` replaces the whole table, and `on_schema_change: ignore` drops new columns quietly. Periodically reconcile against a full rebuild.

**Q: Which incremental strategy would you choose for a large event fact vs a mutable entity table?**

For event facts, replace whole time windows: microbatch, or on Databricks `insert_overwrite` by partition/cluster or `replace_where`. Retries, late data (via lookback) and backfills then become the same operation (P6). For entities that change anywhere in history, `merge` on the business key, with `incremental_predicates` to prune the target scan (P4). Use `delete+insert` if keys can repeat within a batch, and `append` only for immutable, deduplicated data.

**Q: How do you do CI for dbt?**

On each PR, build `state:modified+ --defer` against prod artifacts into an isolated per-PR schema or clone, run unit tests, data tests and lint, data-diff critical models, and drop the schema on merge ([§14](14-reliability-dataops.md)). The dbt platform provides this as CI jobs. Self-run needs a durable prod-manifest store; dbt Projects on Snowflake can import the last successful run's artifacts. Failure modes: a stale prod manifest that selects the wrong models, and a custom `generate_schema_name` that lets PR builds write to shared schemas.

**Q: dbt vs Dynamic Tables / Lakeflow pipelines?**

Native declarative tables let the engine compute increments and schedule refreshes. That means lower latency, less code and engine-level correctness, in exchange for lock-in and weaker test and docs workflows (P10, P11). dbt gives portability, tests, contracts and environments, but you own the delta logic. Usually you want both: author in dbt and materialize latency-sensitive models as `dynamic_table` or `streaming_table`/`materialized_view`. On Snowflake, pin the refresh mode so a fallback to full refresh fails loudly.

**Q: When would you NOT use dbt?**

Don't use it for sub-minute streaming, stateful event processing or ordered CDC application; use Lakeflow pipelines, Dynamic Tables or Flink ([§11](11-stream-processing.md)). It also fits poorly for mostly non-SQL transforms (documents, ML features, binary formats), and for a few simple native pipelines owned by engineers, where another tool is pure overhead. It isn't an ingestion tool or a cross-system orchestrator, and seeds aren't a loader.

**Q: How does dbt fit with the semantic layer?**

Models produce clean, tested tables. The dbt Semantic Layer defines entities, dimensions and metrics in YAML in the same repo, and MetricFlow (Apache 2.0) compiles each metric request into warehouse SQL, so metrics are reviewed like models. Serving them to BI tools and agents is a dbt platform feature, billed per queried metric. Alternatively, dbt can deploy platform-native layers: Databricks `metric_view`, or Snowflake semantic views via a package. Choose by where consumption happens ([§03](03-data-modeling.md)). The failure mode is BI that bypasses the layer, leaving one more definition instead of one.

**Q: What changed with Fusion/dbt v2 and the Fivetran merger, and should it change your choice?**

dbt v2 (formerly Fusion) is a Rust engine with strict parsing, static analysis and column lineage, GA on Snowflake, Databricks, BigQuery and Redshift. v1 is still supported and Apache 2.0. The license split matters for policy: dbt OSS is Apache 2.0, full v2 is free but proprietary, and strict analysis and dbt State need a platform login. The merger puts ingestion and transformation under one vendor. That doesn't change how dbt works, but it does raise bundling and lock-in questions at contract time (P11). Decide on mechanics: check v2 compatibility for your adapters and packages, and keep code in plain SQL in Git.

**Q: How do you attribute and control dbt warehouse cost?**

dbt's cost is the SQL it submits, so tag it and isolate it. Use `query_tag` on Snowflake or `query_tags` on Databricks, give prod jobs dedicated warehouses, and join query history to `run_results.json`. Then skip work (P4): incremental models, slim CI, dbt State skip and clone, and `full_refresh: false` on large tables. Watch for view chains that push cost onto every reader.

**Q: When should you split a dbt project with Mesh?**

Split when ownership is the bottleneck, not size alone: teams colliding in one repo, slow CI, or different security boundaries. Each project publishes `public` models with contracts and versions as its API (P8). The costs: Mesh is an Enterprise dbt platform feature, version deprecations need coordinating, and lineage can fragment across too many projects. First check whether groups, access modifiers and better selectors already solve the problem.

## Takeaways
- dbt compiles SELECTs into DDL/DML. The warehouse does the work and owns the cost.
- `ref()` is the DAG, the environment switch and the lineage. Bypassing it breaks all three.
- An incremental model is your delta logic plus a strategy. Make it idempotent and add a lookback.
- `insert_overwrite` means the whole table on Snowflake and partitions on Databricks.
- Contracts and versions are schema-as-contract (P8) for models.
- The run options differ in where the control process lives and who operates its surroundings, not in where the SQL runs.
- The dbt platform sells operability and platform features; Projects on Snowflake and the Databricks dbt task sell staying on one platform.
- dbt v2 is the Rust engine (Apache 2.0 as dbt OSS, proprietary as full v2). State-aware orchestration is now dbt State.

## Sources
- https://docs.getdbt.com/docs/dbt-versions/release-notes
- https://docs.getdbt.com/docs/dbt-versions/dbt-upgrade/upgrading-to-v2
- https://docs.getdbt.com/docs/dbt-licensing
- https://docs.getdbt.com/docs/local/install-dbt-v2
- https://docs.getdbt.com/docs/supported-data-platforms
- https://docs.getdbt.com/docs/fusion/fusion-readiness
- https://docs.getdbt.com/docs/deploy/dbt-state-about
- https://docs.getdbt.com/docs/deploy/continuous-integration
- https://docs.getdbt.com/docs/platform/billing/how-pricing-works
- https://docs.getdbt.com/docs/platform/billing/plans-and-billing
- https://docs.getdbt.com/docs/platform/canvas
- https://docs.getdbt.com/docs/explore/explore-projects
- https://docs.getdbt.com/docs/dbt-ai/dbt-ai-faqs
- https://docs.getdbt.com/docs/mesh/about-mesh
- https://docs.getdbt.com/docs/use-dbt-semantic-layer/dbt-sl
- https://docs.getdbt.com/docs/build/incremental-models
- https://docs.getdbt.com/docs/build/incremental-strategy
- https://docs.getdbt.com/docs/build/incremental-microbatch
- https://docs.getdbt.com/docs/build/unit-tests
- https://docs.getdbt.com/docs/build/snapshots
- https://docs.getdbt.com/docs/mesh/govern/model-contracts
- https://docs.getdbt.com/docs/dbt-versions/dbt-upgrade/Older%20versions/upgrading-to-v1.5
- https://docs.getdbt.com/reference/resource-configs/databricks-configs
- https://docs.getdbt.com/reference/resource-configs/snowflake-configs
- https://github.com/databricks/dbt-databricks
- https://docs.databricks.com/aws/en/jobs/dbt
- https://docs.databricks.com/aws/en/jobs/how-to/use-dbt-in-workflows
- https://docs.databricks.com/aws/en/jobs/dbt-platform
- https://docs.databricks.com/aws/en/jobs/configure-task
- https://docs.databricks.com/aws/en/partners/prep/dbt
- https://docs.databricks.com/aws/en/partners/prep/dbt-cloud
- https://docs.databricks.com/aws/en/release-notes/product/2026/august
- https://www.getdbt.com/blog/dbt-databricks-jobs
- https://docs.snowflake.com/en/user-guide/data-engineering/dbt-projects-on-snowflake
- https://docs.snowflake.com/en/user-guide/data-engineering/dbt-projects-on-snowflake-understanding-dbt-project-objects
- https://docs.snowflake.com/en/user-guide/data-engineering/dbt-projects-on-snowflake-dbt-core-versions
- https://docs.snowflake.com/en/user-guide/data-engineering/dbt-projects-on-snowflake-limitations
- https://docs.snowflake.com/en/user-guide/data-engineering/dbt-projects-on-snowflake-supported-commands
- https://docs.snowflake.com/en/user-guide/data-engineering/dbt-projects-on-snowflake-cost
- https://docs.snowflake.com/en/sql-reference/sql/execute-dbt-project
- https://docs.snowflake.com/en/release-notes/2025/other/2025-11-06-dbt-projects-on-snowflake-ga
- https://www.fivetran.com/press/fivetran-dbt-labs-complete-merger-to-create-the-data-infrastructure-for-trusted-ai-agents
