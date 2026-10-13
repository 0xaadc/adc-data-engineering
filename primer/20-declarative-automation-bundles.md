# 20. Declarative Automation Bundles: Deployment as Code

> As of October 2026. Tags: **(GA)** generally available · **(Preview)** public/private preview · **(⚠ verify)** not confirmed against official docs.

## Why this exists

A Lakeflow Job built by clicking in the UI is mutable state with no history. Its definition exists only in one workspace, nobody reviewed it, and promoting it means someone re-clicks it in prod, slightly differently. Over months, environments drift, and nobody can say which notebook version prod runs or who changed its schedule. This is **ClickOps**: the workspace is the only copy of the spec, so it can't be diffed, reviewed, reproduced or rolled back.

Bundles move the spec into Git next to the code it runs. A bundle declares jobs, pipelines, dashboards, schemas and other project assets as YAML (or Python), and the CLI reconciles a target workspace to match. Three properties follow:

- **Idempotent deploys (P6).** Deploying the same commit twice changes nothing. Deploying it to another target gives the same shape with different parameters.
- **Reviewable contracts (P8).** A schedule change is a pull request diff with an owner, not an undocumented click.
- **Identity separated from people (P9).** Prod is deployed by a service principal, so no human holds prod credentials and the pipeline is the only way in.

Modes, presets and templates are a P10 trade: opinionated defaults in exchange for some per-object control.

Bundles have been **(GA)** since April 2024. They were renamed from Databricks Asset Bundles on March 16, 2026, and the rename was non-breaking: the `databricks bundle` CLI and `databricks.yml` are unchanged ([§07](07-databricks-gap-fill.md)).

## Core concepts

### The mental model: desired state, a state file, reconcile

`databricks bundle deploy -t <target>` resolves the config for that target (overrides, variables), diffs it against the last deployed snapshot, calls the REST APIs to create, update or delete objects, and records their IDs in a **state file** in the workspace. Two identity facts explain most bundle behavior:

- **Resources are tracked by ID, not by name.** A resource in config but not in state is created. One in both is updated. One previously deployed but now gone from config is **deleted**: removing a YAML block is a delete.
- **A deployment's identity is its `root_path` in a workspace.** The default is `~/.bundle/${bundle.name}/${bundle.target}`, in the *deployer's* home folder. Changing the bundle name, target, workspace or (with the default path) the deploying identity creates a fresh deployment: new copies appear and the old resources are orphaned.

Under `root_path`, deploy writes `files/` (synced source), `artifacts/` (built wheels and JARs) and `state/`, and a lock file stops concurrent deploys from interleaving. This is P5 in miniature: Git holds intent, the state file records what was applied, and the workspace is the materialized result.

### `databricks.yml` anatomy

A bundle has exactly one `databricks.yml` at its root. The top-level mappings are:

- **`bundle`**: `name` (required), plus `engine`, `git`, `deployment.lock` and `databricks_cli_version` (pins a compatible CLI range).
- **`include`**: globs for more config files, typically `resources/*.yml`.
- **`variables`**: used as `${var.name}`.
  - Resolution order, first match wins: `--var`, then `BUNDLE_VAR_<name>` environment variables, then `variable-overrides.json`, then the target's `variables`, then the `default`.
  - A **lookup** resolves a *name* to an *ID* at deploy time, so the warehouse `dbt-wh` maps to a different ID in each workspace. Lookups cover warehouses, clusters, policies, jobs, pipelines, service principals and a few more.
  - `type: complex` variables hold whole maps, such as a shared cluster spec.
- **`targets`**: named environments, each with `workspace.host`, optional `root_path`, `mode`, `presets` and overrides of any other mapping.
- **`resources`**: the objects to deploy, keyed by a logical name. Their fields mirror each object's REST API create payload.
- **`artifacts`**: things built at deploy time (`type: whl`, `build: uv build --wheel`), uploaded to `artifact_path` and referenced by jobs as libraries.
- **`permissions`**: workspace ACLs on every resource (top-level levels: `CAN_VIEW`, `CAN_RUN`, `CAN_MANAGE`). These govern the tool's objects; data access still comes from UC grants (P9, [§12](12-governance-security.md)).
- **`run_as`**: the identity jobs and pipelines execute as. If it differs from the deployer, only jobs and pipelines are supported.
- **`sync`**, **`python`** and **`scripts`** are covered below.

### What a bundle can deploy

The resource list has grown well beyond jobs. As of CLI 1.20 (October 2026):

| Group | Resource keys |
|---|---|
| Workloads | `jobs`, `pipelines`, `alerts`, `dashboards`, `apps`, `genie_spaces` (Genie Agents), `job_runs` |
| ML/AI | `experiments`, `registered_models` (UC), `models` (legacy), `model_serving_endpoints`, `model_services`, `model_provider_services`, `mcp_services`, `vector_search_endpoints`, `vector_search_indexes`, `quality_monitors` |
| UC containers | `catalogs`, `schemas`, `volumes`, `external_locations`, `secrets` (UC) |
| Compute | `clusters`, `cluster_policies`, `instance_pools`, `sql_warehouses` |
| Lakebase | `database_instances`, `database_catalogs`, `synced_database_tables`, `postgres_projects`, `postgres_branches`, `postgres_endpoints`, `postgres_databases`, `postgres_roles`, `postgres_catalogs`, `postgres_synced_tables`, `postgres_snapshot_schedules` |
| Other | `secret_scopes` |

Several of these require the direct engine: catalogs, external locations, cluster policies, instance pools, UC secrets, Genie spaces, job runs, vector search endpoints, MCP services, model services, model provider services and Postgres snapshot schedules. Being *able* to manage catalogs from a bundle doesn't mean you *should* (see the rate-of-change section below).

### Deployment modes

Modes are bundles of presets. Presets override a mode's defaults, and an explicit setting on a resource overrides both.

| Behavior | `mode: development` | `mode: production` |
|---|---|---|
| Names | Prefixes resource names with `[dev <short_name>]`; tags jobs and pipelines `dev` | Unchanged |
| Schedules and triggers | All paused | As declared |
| Job concurrency | Concurrent runs enabled | As declared |
| Pipelines | Sets `development: true` | Fails validation unless `development: false` |
| Deployment lock | Disabled | Enabled |
| Cluster override | `--cluster-id` allowed | Not allowed |
| Git check | None | If `git.branch` is set, deploy fails from any other branch (unless `--force`) |
| Identity checks | None | Without a service principal: paths can't be user-specific, and `run_as` and `permissions` must be set |

Development mode gives each developer a private, harmless copy; production mode guards against the common ways prod gets corrupted.

### The lifecycle commands

- `bundle init [template]` scaffolds a project. Built-in templates: `default-minimal`, `default-python`, `default-scala`, `default-sql`, `dbt-sql`, `mlops-stacks`, `pydabs`. Custom templates encode org standards (permissions, service principals, CI files).
- `bundle validate` checks syntax and substitution and warns on unknown fields. `--output json` shows the resolved config.
- `bundle plan` previews creates, updates, deletes and recreates. `plan -o json` produces a file that `deploy --plan` applies exactly, so the reviewed plan is what reaches prod.
- `bundle deploy -t <target>` reconciles. Flags worth knowing:
  - `--auto-approve` skips prompts, including pipeline recreation.
  - `--fail-on-active-runs` refuses to deploy while jobs or pipelines are running.
  - `--force-lock` breaks a stale lock.
  - `--select` deploys a subset, for dev only.
- `bundle run <key>` triggers a job, pipeline or script. `bundle summary` and `bundle open` show the deployment identity and resource links.
- `bundle destroy` permanently deletes everything deployed. `lifecycle.prevent_destroy: true` protects a resource.
- **Adopting UI-built objects:** `bundle generate job|pipeline|dashboard|app|genie-space` writes the YAML and downloads the referenced files. `bundle deployment bind <key> <id>` links that YAML to the existing object; without it, the next deploy creates a **duplicate** (`generate --bind` does both). `deployment unbind` releases a resource.

### The engine: from Terraform to direct

Bundles originally wrapped Terraform: the CLI generated Terraform config, ran the Databricks provider and kept `terraform.tfstate` in the workspace. That inherited Terraform's binary downloads, proxy problems and resource-coverage lag.

The **direct deployment engine** calls the Databricks Go SDK instead.

- **(GA)** June 10, 2026 (CLI 1.3.0), and the default for new bundles.
- Terraform-state bundles have been auto-migrated on deploy since CLI 1.14.
- **CLI 1.20.0 (October 7, 2026) removed the Terraform engine.** `engine: terraform` now errors; pinning CLI 1.19 is the only way to stay on it.

Its diff semantics matter operationally:

- The state file (`resources.json`, with a serial number and lineage) records the *local config* that was deployed.
- A plan compares local config with that snapshot, then the remote object with the snapshot.
- **Removing a field resets it to the API default**, where Terraform left the old value in place. To keep a value, declare it.
- It also adds plan replay, the direct-only resources above, and `immutable_folder` **(Preview)**, which deploys code to a read-only snapshot folder so non-admins can't edit it and a deploy can't change files under a running job.

### Files, artifacts and Python

Deploy syncs project files to `root_path/files`, honoring `.gitignore` and `sync.include`/`exclude`/`paths`, and rewrites relative paths in resources (`../src/etl.py`) to workspace paths. A job therefore runs code from the same commit as its definition. `bundle sync --watch` does the file push alone during development.

Resources can also be defined in Python with the `databricks-bundles` package **(GA, Oct 2025)**: `python: resources: ["resources:load_resources"]` in `databricks.yml` points at a function that returns them. It pays off when resources come from metadata (one job per source) or every job needs an enforced mutation, such as tags or a budget policy ([§13](13-performance-cost.md)). The Python runs at deploy time and emits config. Bundles can also be edited and deployed from the workspace UI **(GA, Oct 2025)**.

### Drift controls

- **Jobs** deployed by a bundle are **read-only in the Jobs UI by default**. Editing requires *Disconnect from source*; UI edits are never written back, and the next deploy reconnects and overwrites them.
- **Dashboards:** deploy fails if the dashboard was edited in the workspace since the last deploy, unless you pass `--force`.
- **Deployment history** **(Preview)** records who deployed which commit and what changed.

The control that actually works is permissions: in prod, humans get `CAN_VIEW` or `CAN_RUN`, and only the deploying service principal can manage. Then the pipeline is the only path to prod, not just the recommended one.

### The CI/CD pattern

1. **On a PR:** unit tests, `bundle validate` and `bundle plan -t staging`, then deploy to staging and run integration tests there.
2. **On merge to `main`:** deploy to prod as a service principal, gated by a protected environment that needs approval.
3. **Authentication:** **workload identity federation (OIDC)**, which Databricks recommends for CI. GitHub mints a short-lived token per run, and the CLI exchanges it for a Databricks OAuth token for a service principal whose federation policy trusts one subject, such as `repo:<org>/<repo>:environment:Prod`. No Databricks secret is stored anywhere.

```yaml
# .github/workflows/deploy-prod.yml (fragment)
permissions: { id-token: write, contents: read }   # allow the runner to mint an OIDC token
jobs:
  deploy:
    environment: Prod          # must match the federation policy subject
    concurrency: prod-deploy   # serialize prod deploys; the bundle lock is the backstop
    runs-on: ubuntu-latest
    env:
      DATABRICKS_AUTH_TYPE: github-oidc
      DATABRICKS_HOST: ${{ vars.DATABRICKS_HOST }}
      DATABRICKS_CLIENT_ID: ${{ secrets.DATABRICKS_CLIENT_ID }}   # SP application ID
    steps:
      - uses: actions/checkout@v4
      - uses: databricks/setup-cli@main   # pin a version in real use
      - run: databricks bundle deploy -t prod
```

Ephemeral runners are fine: authoritative state lives in the workspace under `root_path/state`, and the local `.databricks/` folder is only a cache (keep it out of Git). What breaks is *identity*. If a human once deployed prod from a laptop with the default `root_path`, CI's service principal creates a second, independent prod copy, with duplicate schedules running duplicate loads. Pin `root_path` in the prod target.

### Example: a Lakeflow Job running a pipeline and dbt

A minimal project: a Lakeflow pipeline ingests data, then a dbt task builds marts on a SQL warehouse. dbt itself is covered in [§19](19-dbt.md); the bundle only deploys the *job* that runs dbt.

```yaml
bundle:
  name: orders

variables:
  catalog:
    description: UC catalog for this target
  schema:
    default: orders
  warehouse_id:
    lookup:
      warehouse: dbt-wh            # same name in every workspace, different ID

resources:
  pipelines:
    orders_ingest:
      name: orders_ingest
      catalog: ${var.catalog}
      schema: ${var.schema}
      serverless: true
      root_path: ./src/pipeline
      libraries:
        - glob:
            include: ./src/pipeline/transformations/**

  jobs:
    orders_daily:
      name: orders_daily
      schedule:
        quartz_cron_expression: "0 0 6 * * ?"   # paused automatically in dev mode
        timezone_id: UTC
      tasks:
        - task_key: ingest
          pipeline_task:
            pipeline_id: ${resources.pipelines.orders_ingest.id}
        - task_key: dbt_build
          depends_on: [{ task_key: ingest }]
          environment_key: dbt
          dbt_task:
            project_directory: ./dbt
            commands: ["dbt deps", "dbt build"]
            warehouse_id: ${var.warehouse_id}
            catalog: ${var.catalog}
            schema: ${var.schema}
      environments:
        - environment_key: dbt
          spec:
            environment_version: "4"
            dependencies: ["dbt-databricks>=1.8.0,<2.0.0"]

targets:
  dev:
    mode: development
    default: true
    workspace: { host: "https://<dev-workspace-host>" }
    variables:
      catalog: dev
      schema: ${workspace.current_user.short_name}   # per-developer tables, no collisions
  prod:
    mode: production
    git: { branch: main }
    workspace:
      host: "https://<prod-workspace-host>"
      root_path: /Workspace/Users/<sp-application-id>/.bundle/${bundle.name}/${bundle.target}
    variables: { catalog: prod }
    run_as: { service_principal_name: "<sp-application-id>" }
    permissions:
      - { group_name: data-eng, level: CAN_VIEW }
      - { group_name: data-oncall, level: CAN_RUN }
    resources:
      pipelines:
        orders_ingest:
          lifecycle: { prevent_destroy: true }
```

The same commit deploys to both targets; only catalog, schema and the looked-up warehouse ID differ. Prod adds a branch check, a fixed identity, least-privilege ACLs and destroy protection.

### Where bundles sit: the rate-of-change rule

Give each object to the tool whose change cadence and blast radius match its own ([§14](14-reliability-dataops.md)):

- **Terraform** (Databricks provider): slow, high-blast-radius platform objects, changed quarterly by a platform team. Workspaces, networking, metastore assignment, storage credentials, external locations, catalogs, account groups and service principals, cluster policies, and the UC grants that encode the access model.
- **Bundles:** project assets that change daily with the code. Jobs, pipelines, dashboards, apps, serving endpoints, project-scoped schemas and volumes.
- **dbt:** transformation logic, meaning models, tests and contracts ([§19](19-dbt.md)). The bundle deploys and schedules the job that runs dbt; it doesn't know what a model is.

Databricks' guidance draws the same line: bundles for data and AI workloads, Terraform for infrastructure and Unity Catalog. Overlap is the danger: if Terraform and a bundle both manage a catalog, each sees the other's changes as drift and reverts them. One owner per object.

### Failure modes

- **UI edits on managed resources.** A prod job is disconnected for a hotfix, and the next deploy silently reverts it. Fix: no human `CAN_MANAGE` in prod; every disconnect is followed by a PR.
- **Deploying as a user.** Ownership, run identity and `root_path` belong to a person, so prod breaks or keeps stale access when they leave. Fix: deploy as a service principal and set `run_as`.
- **Dev collisions and orphans.** Prefixes isolate *names*, not *data*: two developers writing the same dev schema overwrite each other's tables. Developers who never `destroy` leave paused jobs and stale schemas behind. Fix: per-developer schemas and scheduled cleanup of dev deployments.
- **Secrets in YAML.** YAML lands in Git and in the resolved config. Fix: reference `{{secrets/<scope>/<key>}}`, inject values from CI as `BUNDLE_VAR_*`, and use OIDC so CI holds no token.
- **Variable and target sprawl.** Twelve targets and forty variables make a config nobody can reason about, and an ad hoc `--var` in prod is an unreviewed change. Fix: three targets, lookups instead of hardcoded IDs, templates instead of copy-paste.
- **Deletion by declaration.** Deleting a YAML block, renaming a resource key, or running `destroy` removes prod objects. Destroying a pipeline can also delete its streaming tables and materialized views (`cascade_on_destroy: false` keeps them on the direct engine). Some changes *recreate* a pipeline, and CI's `--auto-approve` skips that prompt. Fix: `prevent_destroy`, plan review, and no `destroy` against prod from automation.
- **State and concurrency.** Changing the name, target, `root_path` or deployer orphans a deployment, and casual `--force-lock` lets two deploys interleave. Fix: pin `root_path`, serialize prod deploys with a CI concurrency group, and treat `--force-lock` as break-glass.
- **Wrong bundle size.** One monorepo-wide bundle couples teams' deploys, locks and blast radius. One bundle per job duplicates config and loses references like `${resources.pipelines.x.id}`. Fix: one bundle per unit that ships together under one owning team.
- **CLI version drift.** Engine and resource support move with CLI releases (Terraform removal in 1.20). Fix: pin the CLI in CI and set `bundle.databricks_cli_version`.

## Comparison

**Deployment-as-code tools across both platforms**

| Tool | Manages | Change model | State lives in | Cadence |
|---|---|---|---|---|
| Terraform (Databricks or Snowflake provider) | Accounts, workspaces, networking, catalogs/databases, roles, grants, policies | Declarative plan/apply | Remote backend you run (S3/GCS/Blob + lock) | Slow, platform team |
| Declarative Automation Bundles **(GA)** | Jobs, pipelines, dashboards, apps, ML endpoints, project schemas/volumes, plus files and artifacts | Declarative plan/deploy, replayable plan | `resources.json` in the workspace under `root_path/state` | Fast, product team |
| dbt (Core or platform) | SQL models, tests, contracts, docs | Declarative DAG of SELECTs, rebuilt per run | Warehouse objects + `manifest.json` artifacts | Fastest, analytics engineers |
| Snowflake DCM Projects **(GA, Aug 2026)** | Databases, schemas, tables, dynamic tables, tasks, warehouses, roles, grants | Declarative `DEFINE` + Jinja, PLAN then DEPLOY (deploy re-plans; no replay) | The DCM project object in Snowflake, one per environment | Fast to medium |
| Snowflake CLI (`snow`) project definitions | `snowflake.yml` entities: Snowpark functions/procedures, Streamlit, Native Apps (the same CLI also runs `snow dcm` and `snow dbt` in CI) | Declarative entities deployed by the CLI | Snowflake objects and stages | Fast |
| schemachange (community, not an official Snowflake product) | Any SQL, as ordered scripts | Imperative versioned (`V`), repeatable (`R`) and always (`A`) scripts | `CHANGE_HISTORY` table | Medium |
| dbt Projects on Snowflake **(GA, Nov 2025)** | dbt project as a schema-level `DBT PROJECT` object | dbt, deployed with `snow dbt deploy`, run with `EXECUTE DBT PROJECT` or tasks | Snowflake object + stored artifacts | Fastest |

**Bundles vs Snowflake DCM Projects, the closest analogs**

| Dimension | Bundles | DCM Projects |
|---|---|---|
| Definition language | YAML or Python over REST resource schemas | SQL `DEFINE` statements + Jinja |
| Center of gravity | Workloads (jobs, pipelines) plus their code | Database objects and in-database pipelines (dynamic tables, tasks) |
| Environment model | Targets in one file; each is a separate deployment identity | A separate DCM project object per environment; targets in `manifest.yml` |
| Plan fidelity | `deploy --plan` applies the reviewed plan | DEPLOY recomputes against current state; intervening changes can alter it |
| Removing a definition | Deletes the resource | Drops the object or revokes the grant |
| CI identity | Service principal via OIDC federation | Service user; Snowflake CLI supports workload identity federation |

## Interview questions

**Q: What are bundles, and why use them instead of the UI?**

A bundle is a declarative, version-controlled spec of a project's Databricks resources and code. The CLI reconciles a target workspace to match it, tracking resource IDs in a state file. The UI gives you ClickOps: unreviewed, unreproducible state that drifts between environments and can't be promoted. Bundles make deploys idempotent (P6), changes reviewable (P8) and prod identity non-human (P9). The UI is still fine for exploration; skip bundles for one-off notebooks nobody schedules.

**Q: How do you promote a job from dev to prod?**

You don't copy the job; you deploy the same commit to a different target. Dev is `mode: development`, a per-user prefixed copy with paused schedules. Prod is `mode: production` with a pinned `root_path`, `git.branch: main`, `run_as` a service principal, and least-privilege `permissions`. The differences between environments live only in target variables and lookups (catalog, warehouse ID), so what was tested is what ships. The common failure is environment-specific logic hidden in code or hardcoded IDs, which makes "same artifact" a lie.

**Q: Bundles vs Terraform: where's the line?**

Draw it by rate of change and blast radius, not by capability. Terraform owns slow platform objects that a platform team changes rarely and that would hurt many teams if wrong: workspaces, networking, metastore, catalogs, storage credentials, cluster policies, grants. Bundles own project assets that change with every feature PR: jobs, pipelines, dashboards, apps. Bundles can now create catalogs and cluster policies on the direct engine, but two tools managing one object revert each other forever, so each object gets exactly one owner.

**Q: How do bundles relate to dbt?**

They're different layers. dbt owns *what* the SQL builds: models, tests, contracts, the DAG ([§19](19-dbt.md)). The bundle owns *how and when it runs*: a Lakeflow Job with a `dbt_task`, its warehouse, schedule, dependencies on upstream pipelines, run identity and per-target catalog. CI usually runs dbt's slim CI checks and `bundle validate` in the same PR, then the merge deploys the job. Don't let the bundle reach into dbt's logic, and don't schedule dbt outside the job, or you'll have two orchestrators.

**Q: How do you prevent drift between the UI and the repo?**

Make the repo the only write path. Bundle-deployed jobs are read-only in the UI by default, and dashboards fail to deploy if they were edited remotely. Neither stops a user with `CAN_MANAGE` from disconnecting a job, so the real control is permissions: humans get `CAN_VIEW`/`CAN_RUN` in prod and only the deploying service principal manages resources. Then a regular redeploy or `bundle plan` surfaces any residual drift. Treat a disconnect as an incident, and follow it with a PR so the next deploy doesn't silently revert the hotfix.

**Q: How would you set up CI/CD for a Databricks project?**

On every PR: unit tests, `bundle validate`, `bundle plan` against staging, a deploy to an isolated dev/staging target, and integration tests against cloned or sampled data. On merge to main: a protected-environment job deploys `-t prod` as a service principal authenticated by GitHub OIDC workload identity federation, so no Databricks secret exists. Serialize prod deploys with a concurrency group (the bundle lock is the backstop), pin the CLI version, and gate destructive plans on human review. Optionally have the PR produce `plan -o json` and apply exactly that with `deploy --plan`.

**Q: What's the Snowflake equivalent?**

The closest is DCM Projects (GA Aug 2026): SQL `DEFINE` statements with Jinja, PLAN then DEPLOY, one project object per environment. Unlike bundles, DEPLOY recomputes the plan against current state instead of replaying a reviewed one. Snowflake CLI project definitions (`snowflake.yml`) cover Snowpark, Streamlit and Native Apps. dbt Projects on Snowflake deploys dbt as a native object. schemachange is the community, Flyway-style imperative option, and Terraform covers account plumbing on both platforms. The bigger difference is the center of gravity: bundles revolve around workloads and code, DCM around database objects ([§14](14-reliability-dataops.md)).

**Q: What can go wrong with bundle deployments?**

Most failures come from declarative semantics or identity. Deleting a YAML block or running `destroy` deletes prod objects, and destroying a pipeline can drop its tables, so use `prevent_destroy` and plan review. Changing the bundle name, target, `root_path` or deployer creates a parallel deployment and orphans the old one, which can mean double-running schedules. Deploying as a user ties ownership to a person. On the direct engine, removing a field resets it to the default rather than leaving it alone. Also watch for secrets in YAML, dev collisions in shared schemas, and casual `--force-lock` or `--auto-approve` in prod.

**Q: How does a bundle know which workspace objects it owns, and how do you adopt existing UI-built jobs?**

Ownership is the state file: it maps each resource key to an object ID under the deployment's `root_path`. Names play no part, which is why a name match doesn't adopt anything. To adopt a UI job, run `bundle generate job --existing-job-id <id>` to write its YAML and download its files, then `bundle deployment bind <key> <id>` (or `generate --bind`) so the next deploy updates that job instead of creating a duplicate. Binding a pipeline keeps its tables, so there's no recompute.

**Q: One bundle per repo, per team, or per job?**

One bundle per unit that deploys together and has one owning team. A bundle shares a lock, a state file and a blast radius, so a monorepo-wide bundle makes every team's deploy wait on and risk everyone else's. One bundle per job loses cross-resource references (a job that triggers its pipeline by `${resources.pipelines.x.id}`) and multiplies boilerplate. A monorepo can hold many bundles; standardize them with a custom `bundle init` template rather than a giant shared config.

## Takeaways

- Bundles exist to kill ClickOps: Git holds the spec, the CLI reconciles the workspace to it (P6).
- Resources are tracked by ID in a state file. Removing YAML deletes; renaming the bundle, target or `root_path` orphans.
- `mode: development` gives per-user, paused, lock-free copies; `mode: production` adds branch, identity and pipeline checks.
- The direct engine is GA (June 2026) and the only engine since CLI 1.20 (October 2026). Removed fields revert to defaults.
- Deploy prod as a service principal through OIDC workload identity federation, with pinned `root_path`, `run_as` and least-privilege ACLs.
- Bundle `permissions` are workspace ACLs; data access is still UC grants (P9).
- Split by rate of change: Terraform for platform, bundles for project workloads, dbt for SQL logic; one owner per object.
- Snowflake's closest analog is DCM Projects; its DEPLOY re-plans rather than replaying a reviewed plan.

## Sources

- https://docs.databricks.com/aws/en/dev-tools/bundles/
- https://docs.databricks.com/aws/en/dev-tools/bundles/settings
- https://docs.databricks.com/aws/en/dev-tools/bundles/reference
- https://docs.databricks.com/aws/en/dev-tools/bundles/resources
- https://docs.databricks.com/aws/en/dev-tools/bundles/deployment-modes
- https://docs.databricks.com/aws/en/dev-tools/bundles/variables
- https://docs.databricks.com/aws/en/dev-tools/bundles/run-as
- https://docs.databricks.com/aws/en/dev-tools/bundles/permissions
- https://docs.databricks.com/aws/en/dev-tools/bundles/direct
- https://docs.databricks.com/aws/en/dev-tools/bundles/templates
- https://docs.databricks.com/aws/en/dev-tools/bundles/python/
- https://docs.databricks.com/aws/en/dev-tools/bundles/job-task-types
- https://docs.databricks.com/aws/en/dev-tools/bundles/faqs
- https://docs.databricks.com/aws/en/dev-tools/bundles/ci-cd
- https://docs.databricks.com/aws/en/dev-tools/bundles/deployments
- https://docs.databricks.com/aws/en/dev-tools/cli/bundle-commands
- https://docs.databricks.com/aws/en/release-notes/dev-tools/bundles
- https://docs.databricks.com/aws/en/jobs/monitor
- https://docs.databricks.com/aws/en/dev-tools/ci-cd/github
- https://docs.databricks.com/aws/en/dev-tools/ci-cd/flows
- https://docs.databricks.com/aws/en/dev-tools/auth/oauth-federation
- https://docs.databricks.com/aws/en/lakehouse-architecture/deployment-guide/iac
- https://github.com/databricks/cli/blob/main/CHANGELOG.md
- https://github.com/databricks/cli/tree/main/libs/template/templates
- https://docs.snowflake.com/release-notes/2026/other/2026-08-07-dcm-projects-ga
- https://docs.snowflake.com/en/user-guide/dcm-projects/dcm-projects-overview
- https://docs.snowflake.com/en/user-guide/dcm-projects/dcm-projects-use
- https://docs.snowflake.com/en/developer-guide/snowflake-cli/command-reference/dcm-commands/plan
- https://docs.snowflake.com/en/developer-guide/snowflake-cli/project-definitions/about
- https://docs.snowflake.com/en/developer-guide/snowflake-cli/data-pipelines/dbt-projects
- https://docs.snowflake.com/en/user-guide/data-engineering/dbt-projects-on-snowflake
- https://docs.snowflake.com/en/release-notes/2025/other/2025-11-06-dbt-projects-on-snowflake-ga
- https://docs.snowflake.com/en/user-guide/terraform
- https://github.com/Snowflake-Labs/schemachange
