# 03. Data Modeling

> As of October 2026. Tags: **(GA)** generally available · **(Preview)** public/private preview · **(⚠ verify)** not confirmed against official docs.

## Why this exists

Source schemas encode *how the application writes*: normalized for fast single-row updates, named for developers, and holding only current state (P1). The business asks different questions, such as revenue by region last quarter, as the org chart looked *then*. Without a model, every analyst rebuilds the joins, picks a different grain, and decides alone how to treat history, and the numbers disagree. Modeling is the act of encoding **meaning** (entities and metrics), **grain** (what one row represents) and **history** (how change is recorded) *once*, so that every query, dashboard and LLM agrees. Physical tricks change with engines. The logical discipline doesn't.

## Core concepts

### Three methodologies, three bets

- **Kimball (bottom-up, dimensional).** Build star schemas one *business process* at a time (orders, shipments, payments). Processes share **conformed dimensions** (one `dim_customer`, one `dim_date`), so facts from different processes can be compared, a technique called "drilling across". The **bus matrix** (rows are business processes, columns are conformed dimensions) is the enterprise plan. The bet: deliver value quickly, and get integration through shared dimensions rather than a big upfront model.
- **Inmon (top-down, normalized).** First build a 3NF **enterprise data warehouse** that integrates every source into one non-redundant model, then feed dependent dimensional **data marts** from it. Normalization means each fact about the business lives in exactly one place, so integration and correction happen once. The bet: consistency first, at the cost of a long time to first value and a central bottleneck.
- **Data Vault 2.0 (integration layer built for change).** The model splits into three structures:
  - **Hubs**: unique business keys plus a hash key, load timestamp and record source.
  - **Links**: relationships between hubs, always many-to-many.
  - **Satellites**: descriptive attributes with history, change-detected by a `hashdiff`.

  Everything is **insert-only**, so you get full auditability and natural history. Keys are **hashes of business keys**, so every loader computes the same key on its own with no sequence lookup, and hubs, links and satellites load **in parallel**. A new source adds satellites without remodeling what exists. The cost is query complexity: rebuilding one entity takes many joins, so you need **point-in-time (PIT) and bridge tables** and a dimensional **presentation layer** (information marts) on top.

Many modern stacks run all three: a Data Vault or 3NF-ish integration layer in silver and Kimball stars in gold.

### Star vs snowflake vs One Big Table

A **snowflake schema** normalizes dimension hierarchies (product → category → department). A **star** flattens them into one wide dimension. On columnar engines the star wins. Repeated hierarchy values compress well (dictionary or run-length encoding, P4), so denormalizing costs almost nothing in storage, while every extra join costs CPU and shuffle and confuses BI tools. Optimizers also push filters on dimensions down into the fact scan (dynamic pruning), which works best with a single hop. Snowflake a dimension only when a sub-dimension is large, shared and maintained on its own (an "outrigger").

**One Big Table (OBT)** pre-joins facts and dimension attributes into a single wide table. It works on modern engines because columnar storage reads only the columns a query touches and compresses repeated values, so the width is cheap and there are no joins at query time. It is fast and simple for BI, ML features and AI agents. The costs:
- Join logic is duplicated across OBTs, so they drift.
- History is frozen at build time: you must pick as-was or as-is semantics, and a Type 1 attribute change rewrites millions of fact rows.
- Schemas grow to hundreds of columns.
- Grain gets blurred.

Treat OBTs as a *derived serving layer* built from a star, not as the system of record.

### Facts, measures and grain

Fact table types:
- **Transactional**: one row per event (an order line). Sparse and additive. The default.
- **Periodic snapshot**: one row per entity per period (a daily account balance). Its measures are **semi-additive**: you can sum across accounts but not across days, where you take the last value or the average.
- **Accumulating snapshot**: one row per process instance (an order's lifecycle), with milestone dates and lags that are *updated* as the instance moves forward. It is the only fact type that gets updated.
- **Factless**: events or coverage with no measures (attendance, or the products eligible for a promotion). You count rows, and you can find what *didn't* happen.

Store non-additive measures such as ratios as their additive components (numerator and denominator), and divide at query time.

**Grain** is the sentence "one row per ___", for example "one row per order line per shipment". Declare it *before* choosing dimensions or measures, because grain decides which dimensions are legal and which measures can be summed. Mixing grains causes double counting. A header-level `shipping_cost` repeated on every order line inflates `SUM(shipping_cost)` by the number of lines. Either allocate it down to lines or keep it in a header-grain fact.

**Fan-out trap**: joining a fact to anything at a finer grain (a one-to-many dimension, a bridge table, a second fact) multiplies rows before aggregation. **Chasm trap**: two facts joined through a shared dimension produce a cross product. The fix for both is **drill-across**: aggregate each fact to the common grain separately, then join the results on conformed dimension keys.

### Slowly changing dimensions

Every attribute needs an answer to one question: when it changes, should old facts show the old value or the new one?

- **Type 0, retain original**: never changes (original signup channel).
- **Type 1, overwrite**: no history. Past reports change retroactively. Use it for corrections and attributes nobody analyzes historically.
- **Type 2, new row per version**: a new **surrogate key** per version, plus `effective_from`/`effective_to`/`is_current`. Facts store the surrogate key of the version that was current *when the event happened*, so history is preserved by the join itself.
- **Type 3, previous-value column**: keeps one prior value (`prior_region`). Good for a planned reorg where both "as was" and "as is" views are needed. No deeper history.
- **Type 4**: split volatile attributes into a separate mini-dimension (or history table) so the main dimension doesn't explode. **Type 5**: Type 4 plus a Type 1 pointer from the main dimension to the current mini-dimension row.
- **Type 6** (1+2+3): Type 2 rows plus a `current_*` column overwritten on every version, giving as-was and as-is in one table.
- **Type 7**: the fact carries both the version surrogate key (as-was) and the durable natural key, which joins to a current-only view (as-is).

Type 2 example. The customer moves from Austin to Denver on 2026-03-01:

```
customer_sk | customer_id | city   | effective_from | effective_to | is_current
101         | C-42        | Austin | 2024-05-10     | 2026-03-01   | false
187         | C-42        | Denver | 2026-03-01     | 9999-12-31   | true
```

A February order joins to `sk 101` (Austin) and an April order to `sk 187` (Denver). The fact load looks up the version where `event_ts >= effective_from AND event_ts < effective_to`.

### Implementing Type 2 with MERGE

A single MERGE can't both update and insert the *same* source row, so the classic trick feeds each changed row in twice. One copy matches and closes the old version. The other copy has a `NULL` merge key, so it can't match and is inserted as the new version:

```sql
MERGE INTO dim_customer t
USING (
  SELECT s.customer_id AS merge_key, s.* FROM stg_customer s          -- closes old / inserts brand-new keys
  UNION ALL
  SELECT NULL AS merge_key, s.* FROM stg_customer s                   -- new version of changed keys
  JOIN dim_customer d ON d.customer_id = s.customer_id AND d.is_current
  WHERE d.attr_hash <> s.attr_hash
) s
ON t.customer_id = s.merge_key AND t.is_current
WHEN MATCHED AND t.attr_hash <> s.attr_hash THEN
  UPDATE SET is_current = FALSE, effective_to = s.changed_at
WHEN NOT MATCHED THEN
  INSERT (customer_id, city, attr_hash, effective_from, effective_to, is_current)
  VALUES (s.customer_id, s.city, s.attr_hash, s.changed_at, '9999-12-31', TRUE);
```

The surrogate key comes from an `IDENTITY` or sequence (Snowflake) or `GENERATED ALWAYS AS IDENTITY` (Delta). `attr_hash` is a hash over the Type 2 columns, which turns change detection into a single comparison. This runs on both Snowflake and Databricks.

Pitfalls:
- **Several changes per key in one batch** break the pattern. Order them by source sequence and build the versions with `LEAD()`, or process one change per key per run.
- **Late or out-of-order changes** require splitting an existing version, not just appending one.
- Deletes must be modeled explicitly, usually by closing the current version and flagging it.

Declarative alternatives:
- **Databricks**: Lakeflow Spark Declarative Pipelines **AUTO CDC** (which replaces `APPLY CHANGES`, same syntax) with `STORED AS SCD TYPE 2` (SQL) or `stored_as_scd_type=2` (Python). It sequences out-of-order events with `SEQUENCE BY` and writes `__START_AT`/`__END_AT`. It needs serverless or the Pro/Advanced edition. Syntax sketch **(⚠ verify)**: `CREATE FLOW f AS AUTO CDC INTO dim FROM stream(src) KEYS (id) APPLY AS DELETE WHEN op = 'd' SEQUENCE BY lsn COLUMNS * EXCEPT (op, lsn) STORED AS SCD TYPE 2;`
- **Snowflake** has no built-in SCD2 operator. Use Streams + MERGE (above) or dbt **snapshots**, which work on either platform.

Feeding SCD2 from CDC is covered in [§09](09-batch-pipelines.md).

### Surrogate vs natural keys

Natural (business) keys come from the source and carry its problems: they change (re-keyed accounts), get reused, collide across sources (two CRMs each with customer `1001`), are composite or wide, and can't represent "unknown". Surrogate keys are warehouse-owned and meaningless. They insulate the model from all of that, they are what make **Type 2 possible** (many versions per natural key), they allow `-1`/inferred members, and they join compactly. Keep the natural key as a *durable key* attribute regardless. Modern variant: **hash keys** (hash of the business key plus source) are deterministic, so loads parallelize and don't need lookups (Data Vault relies on this). The price is wider join columns and worse clustering than integers. Identity columns on distributed engines are unique but not gap-free.

### Semantic layer: one definition of each metric

Metrics drift because every BI tool, notebook and extract re-implements "active customer" or "net revenue" in its own SQL, with different filters, joins and grain. A **semantic layer** centralizes entities and join paths, dimensions, measures with their aggregation rules, and **metric definitions** (ratios, time windows, filters), then compiles queries against them. It also turns definitions into a governed, versioned contract (P8, P9).

- **dbt Semantic Layer / MetricFlow**: YAML semantic models and metrics in the dbt project, served through APIs to BI tools. MetricFlow was **open-sourced under Apache 2.0 (Oct 2025)**.
- **Snowflake semantic views** **(GA)**: schema-level objects created with `CREATE SEMANTIC VIEW` and queried with `SELECT ... FROM SEMANTIC_VIEW(...)`. Cortex Analyst uses them as context. Snowsight management and sharing went GA in late 2025.
- **Databricks Unity Catalog metric views**: YAML definitions registered in UC that follow UC permissions and are used by dashboards, Genie and external BI. Public Preview from May 2025. Databricks announced Unity Catalog business semantics, including metric views, as GA in 2026, with the core being open-sourced into Apache Spark **(⚠ verify date and scope)**.
- **Cube**: open-source, warehouse-agnostic semantic layer with SQL/REST/GraphQL APIs and pre-aggregation caching. **LookML**: Looker's modeling language, the long-standing BI-embedded option.
- **Open Semantic Interchange (OSI)** → **Apache Ossie (Incubating)**: a vendor-neutral YAML/JSON spec for metrics, dimensions and relationships. It launched in Sept 2025 (Snowflake, Salesforce, dbt Labs, BlackRock, RelationalAI and others), published an initial spec in early 2026, and entered the Apache Incubator under the new name in mid-2026, with converters for dbt and others. It applies the open-format idea to semantics (P11): define a metric once and move it between tools.

**Why this matters for AI.** An LLM writing SQL against raw tables has to guess join paths, grain and metric formulas, and it guesses differently each time, which is where the fan-out trap meets a confident wrong answer. A semantic layer gives it a governed vocabulary and pre-validated joins, so text-to-SQL becomes "choose a metric and dimensions" instead of "invent SQL" (see [§18](18-ai-on-data-platform.md)).

### Modeling across medallion layers

Bronze stays source-shaped. Silver is cleaned and conformed (3NF-ish or Data Vault). Gold holds dimensional stars, OBTs and semantic definitions. See [§02](02-storage-paradigms.md).

## Comparison

| | Kimball | Inmon | Data Vault 2.0 |
|---|---|---|---|
| Direction | Bottom-up, per business process | Top-down, enterprise first | Integration layer, incremental by source |
| Core structure | Star schemas + conformed dims | 3NF EDW → dependent marts | Hubs, links, satellites (insert-only) |
| Time to value | Fast | Slow | Medium (needs a presentation layer) |
| Handles source change | Moderate (remodel dims) | Poor (central model change) | Strong (add satellites) |
| Auditability | Moderate | Good | Excellent (raw, insert-only, record source) |
| Query ergonomics | Best | Poor for BI | Poor without PIT/bridge + marts |
| Fits | Analytics-first, few sources | Stable, centralized enterprise | Many changing sources, regulated, parallel teams |

| | Star | Snowflake schema | OBT |
|---|---|---|---|
| Joins per query | One hop | Multi-hop | None |
| Storage on columnar | Slightly more (compresses) | Least | Most (still compresses) |
| History (SCD2) | Natural | Natural | Hard (frozen at build time) |
| Logic reuse | Conformed dims | Conformed dims | Duplicated per table |
| Best role | Gold system of record | Shared large sub-dimensions | Serving layer for BI/ML/AI |

| SCD type | Mechanism | History | Use for |
|---|---|---|---|
| 0 | Never update | Original only | Immutable attributes |
| 1 | Overwrite | None | Corrections, non-analytic attributes |
| 2 | New row + surrogate key + dates | Full | Attributes analyzed over time |
| 3 | Prior-value column | One version | Planned reorgs, as-was vs as-is |
| 6 | Type 2 + overwritten current column | Full + current | Both views from one table |

## Interview questions

**Q: Kimball vs Inmon vs Data Vault?**

They solve different problems. Kimball optimizes delivery and query usability, integrating through conformed dimensions. Inmon optimizes enterprise consistency with a normalized core, but delivers slowly. Data Vault optimizes integration of many changing sources with full auditability and parallel loading, and pushes usability into a presentation layer. In practice they stack rather than compete: Data Vault or 3NF in silver for integration and history, Kimball stars in gold for consumption.

**Q: When would you choose Data Vault?**

When there are many sources that change often, regulation requires a provable, insert-only record of what arrived and when, and several teams need to load in parallel without coordinating key generation. Hash keys and insert-only satellites deliver exactly those properties. Don't choose it for a small team with a handful of stable sources: you pay the join complexity, PIT/bridge maintenance and a second modeling layer for flexibility you won't use.

**Q: Star vs snowflake vs OBT on a modern columnar engine?**

Star is the default. Columnar compression makes denormalized dimensions nearly free, and single-hop joins are what optimizers and BI tools handle best. A snowflake schema saves storage that no longer matters and adds joins. OBT removes joins entirely and is great for serving, but it duplicates logic and freezes history at build time, so build it *from* the star. Never make it the only model.

**Q: What is grain and why declare it first?**

Grain is what one row means, for example "one row per order line per shipment". It determines which dimensions can attach and which measures can be summed. Declare it first because every double count traces back to mixed grains: a header measure repeated on lines, or a join to a finer-grained table. If you can't state a table's grain in one sentence, the table is wrong.

**Q: A dashboard total is higher than finance's number. What do you check first?**

Fan-out: a join to a finer-grained table, a many-to-many bridge or a second fact multiplies rows before the `SUM`. Then check for mixed grains (header values on line rows), SCD2 joins missing the `is_current` or effective-date predicate (so every version matches), and duplicate loads from a non-idempotent pipeline. The structural fix is drill-across: aggregate each fact to a common grain, then join on conformed keys.

**Q: How would you implement SCD Type 2, end to end?**

Detect changes by comparing a hash of the tracked attributes against the current row. For changed keys, close the current version (set `effective_to`, `is_current = false`) and insert a new row with a new surrogate key, in one atomic MERGE using the NULL-merge-key union trick. Fact loads look up the surrogate key whose effective range contains the event time. Watch for multiple changes per key in a batch (order them and build versions with `LEAD`), late changes that require splitting a version, and deletes. On Databricks, AUTO CDC `STORED AS SCD TYPE 2` handles sequencing for you. On either platform, dbt snapshots are the portable option.

**Q: How do you choose between SCD Type 1, 2 and 3 per attribute?**

Ask whether past facts should keep the old value. If not, or if the change is a correction, use Type 1. If analysis needs the value as it was, use Type 2. If the business needs only a one-step "before and after" view of a planned change, Type 3 is cheaper. Choose per column, not per table, and avoid making high-churn attributes Type 2: move them to a mini-dimension (Type 4), or the dimension explodes.

**Q: Surrogate vs natural keys?**

Natural keys change, get reused and collide across sources, and they can't represent unknown members. Surrogates insulate the model from all of that, and they are what makes Type 2 history possible. Keep the natural key as a durable attribute for lineage and as-is joins. Hash-based surrogates trade integer compactness for deterministic, lookup-free parallel loading, which suits Data Vault and distributed ingestion.

**Q: How do you keep metric definitions consistent across tools?**

Define each metric once, in a governed semantic layer (dbt MetricFlow, Snowflake semantic views, UC metric views or Cube), version it like code, and make BI tools and agents query through it instead of hand-writing SQL. Pick the layer by where consumption happens: a platform-native layer if one platform dominates, a warehouse-agnostic one if BI spans engines. Watch Apache Ossie (formerly OSI) for portability between them. The failure mode is a semantic layer that BI tools bypass, which leaves you with one more definition instead of one fewer.

**Q: Why does a semantic layer matter for LLM text-to-SQL?**

Without one, the model infers joins, grain and formulas from column names, so answers differ between runs and fall into fan-out traps while sounding confident. With one, the model selects from governed metrics and dimensions, joins are pre-validated, and permissions are already applied. That turns an open-ended generation problem into a constrained selection problem with auditable outputs.

**Q: Is dimensional modeling dead in the lakehouse era?**

The *physical* tricks are fading: aggressive aggregate tables and snowflaked dimensions mattered on row stores and expensive storage. The *logical* discipline is not: declared grain, conformed dimensions, explicit history and business-process facts are about meaning, and cheap compute doesn't make meaning free. OBTs, semantic layers and AI agents all sit on top of well-defined dimensions and facts, so dimensional modeling has become the substrate rather than the final serving layer.

## Takeaways
- Source schemas are optimized for writes. Models are optimized for meaning: grain, entities, metrics, history.
- Kimball for consumption, Inmon for central consistency, Data Vault for many changing, audited sources. They stack.
- Star beats snowflake on columnar engines. OBT is a derived serving layer, not the system of record.
- Declare grain first. Double counts come from mixed grains and fan-out. Drill across instead of joining facts.
- SCD is a per-attribute decision. Type 2 needs surrogate keys and effective dates, implemented as an atomic MERGE.
- Surrogate keys decouple the model from source keys. Hash keys buy parallel loading.
- A semantic layer makes metrics a governed contract, and it is what makes text-to-SQL trustworthy.

## Sources
- https://docs.databricks.com/aws/en/ldp/cdc
- https://docs.databricks.com/aws/en/metric-views
- https://docs.databricks.com/aws/en/business-semantics/metric-views/feature-availability
- https://docs.databricks.com/gcp/release-notes/product/2025/may
- https://www.databricks.com/blog/redefining-semantics-data-layer-future-bi-and-ai
- https://docs.snowflake.com/en/release-notes/2025/june-summit
- https://docs.snowflake.com/en/user-guide/views-semantic/semantic-models-vs-views
- https://docs.snowflake.com/release-notes/2025/other/2025-10-02-semantic-views-in-snowsight
- https://docs.snowflake.com/En/release-notes/2025/other/2025-11-04-sharing-semantic-views
- https://www.getdbt.com/blog/open-source-metricflow-governed-metrics
- https://docs.getdbt.com/docs/build/about-metricflow
- https://www.getdbt.com/blog/the-osi-spec-updates
- https://thenewstack.io/snowflake-salesforce-launch-new-standard-to-unify-data-for-ai/
- https://ossie.apache.org/updates/ossie-enters-apache-incubator/
- https://snowflake.com/en/blog/apache-ossie-open-semantic-interchange-incubator
- https://www.dremio.com/blog/apache-ossie-the-open-semantic-interchange-joins-the-apache-incubator
