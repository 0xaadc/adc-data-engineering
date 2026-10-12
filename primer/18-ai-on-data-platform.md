# 18. AI on the Data Platform

> As of October 2026. Tags: **(GA)** generally available · **(Preview)** public/private preview · **(⚠ verify)** not confirmed against official docs.

## Why this exists

Models are commodities: frontier models from several providers are one function call away on both platforms. A competitor can't copy your governed, well-modeled, fresh data. In production, LLM apps fail on data problems more often than on model problems:
- a stale index
- a retriever that ignores permissions
- three meanings of "revenue"
- PII leaking into prompts

That makes AI a data-platform responsibility, and the architect owns it.

## Core concepts

### Feature stores: offline/online and point-in-time correctness

- **Two stores, one definition.** The *offline* store holds historical feature values in lakehouse tables for training and batch scoring. The *online* store holds the latest value per entity key for millisecond lookups at inference. A pipeline keeps the two in sync (P1).
- **Training-serving skew.** If training computes a feature in batch SQL and serving recomputes it in app code, the model sees different distributions in production. Define the feature once and materialize it to both stores.
- **Point-in-time correctness.** For a label at time *t*, join each feature's value *as of t*: the latest value with timestamp ≤ *t*. A plain join on entity key pulls in today's value, which leaks the future (label leakage). Offline metrics look great and the model then underperforms in production. The mechanism is an AS-OF join on (entity key, timestamp). Also model availability latency: if a feature lands 2 hours late in production, join as of *t* − 2h.
- **Databricks.** In Feature Engineering in Unity Catalog, any Delta table with a primary key (plus an optional timeseries key) is a feature table, and training sets use point-in-time lookups. Online serving is the **Online Feature Store powered by Lakebase**; `publish_table` syncs in TRIGGERED (default), CONTINUOUS, or SNAPSHOT mode, and the first two require Change Data Feed. It's **(GA)** on AWS and Azure and **(Preview)** (Beta) on GCP. Legacy online tables are no longer supported. Model Serving can look features up automatically.
- **Snowflake.** The Snowflake Feature Store **(GA)** has entities and feature views. Managed feature views are dynamic tables refreshed incrementally, and training data is point-in-time via ASOF JOIN. The Online Feature Store is **(Preview)** (public preview since July 2026) and offers batch, stream, and real-time feature views.

### Vector search

- **Embeddings** map content to vectors, so semantic similarity becomes geometric closeness. Exact kNN scans every vector, and ANN indexes trade a little recall for large speedups:
  - **HNSW** is a layered neighbor graph that's navigated greedily. It's fast with high recall but memory-heavy.
  - **IVF** clusters vectors around centroids and searches only the nearest lists. It's cheaper, recall depends on how many lists you probe, and it's often paired with quantization.
- **Hybrid search.** Embeddings miss exact tokens like SKUs, error codes, and names. Combine vector and keyword (BM25) results, fuse the rankings, then rerank the top candidates with a cross-encoder.
- **Databricks AI Search** (formerly Vector Search; renamed June 2026):
  - *Delta Sync* indexes update incrementally from a source Delta table, triggered or continuous.
  - *Direct Vector Access* indexes are written via API.
  - It supports managed or self-managed embeddings, hybrid queries, a reranker, filters, and full-text search **(Preview)** (Beta).
- **Snowflake Cortex Search (GA)** is a managed service over a query. Every request runs vector + keyword + semantic reranking with attribute filters, and the index refreshes against a target lag.
- **Freshness is a pipeline problem (P7).** Index lag = upstream pipeline lag + sync lag + embedding throughput. Changing the embedding model means re-embedding everything (a backfill), so version the model as part of the index identity.

### RAG over governed data

```
docs/tables ─► parse ─► chunk + metadata + ACL attrs ─► (redact PII) ─► embed ─► index
                                                                                 │
user ─► app (authenticated identity) ─► retrieve: mandatory ACL filter + hybrid + rerank
                                         └─► LLM answer with citations ─► traces ─► eval
```

- **Chunking.** Split on structure (headings, sections, table boundaries) rather than fixed character counts, with modest overlap. Prepend the title and section path, and keep chunk → document → source IDs for citations and deletion.
- **Metadata filters:** tenant, region, document type, effective date, and ACL attributes (allowed groups, classification).
- **Enforce security at retrieval time (P9).** Access to an index is usually granted at the object level, and the index is built with its owner's privileges. Source row filters and masks therefore don't automatically apply per querying user: Cortex Search runs with owner's rights, and Databricks AI Search doesn't support row- or column-level permissions. An index that ignores ACLs is a data-exfiltration API. Options:
  1. **Partition indexes by audience** (tenant or sensitivity tier). This is the simplest and strongest option.
  2. **Store ACL columns on each chunk** and apply a *mandatory* filter derived server-side from the authenticated identity, never from the prompt or the LLM.
  3. **Post-check** returned document IDs against source permissions.

  Chunks and vectors are copies of the data and inherit its classification. Redact before embedding, and propagate deletions.
- **Evaluation.**
  - Build a versioned eval set (question, expected sources, reference answer) from real queries.
  - Measure retrieval (recall@k, MRR) separately from generation (groundedness, correctness, citation accuracy).
  - Calibrate LLM judges against human labels.
  - Run the eval in CI on any change to chunking, embedding model, prompt, or model, and trace production.
  - MLflow 3 **(GA)** provides tracing, `mlflow.genai.evaluate()`, built-in and custom scorers, and human feedback.
- **Who owns what.**
  - The *data team* owns ingestion and parsing, chunk tables, metadata and ACL propagation, index freshness SLAs, PII handling, lineage, and eval sets as governed tables.
  - The *app team* owns prompts, orchestration, UX, model choice, and guardrails.
  - Both share the eval set and the incident process.

### Text-to-SQL agents need a semantic layer

LLMs write plausible SQL against raw schemas but guess the joins, the grain, and what "active customer" means. A semantic layer encodes entities, joins, metrics, synonyms, and verified queries, so the agent picks a defined metric instead of inventing one ([§03](03-data-modeling.md)). The agent should also run as the user, so that row and column policies apply.
- **Snowflake:** Cortex Analyst **(GA)** over semantic views **(GA)**; since Aug 2026 Snowflake recommends invoking it through Cortex Agents **(GA)**. Snowflake CoWork (formerly Snowflake Intelligence) **(GA)** is the business-user front end, built on Cortex Agents.
- **Databricks:** Genie uses table metadata, instructions, and example SQL. Genie spaces were renamed *Genie Agents* in July 2026, and Unity Catalog metric views **(GA)** provide governed metrics.

### AI functions in SQL

These run batch LLM inference as a SQL function: classify, extract, summarize, or filter millions of rows.
- **Snowflake Cortex AI Functions:**
  - **(GA):** AI_COMPLETE, AI_CLASSIFY, AI_EMBED, AI_SIMILARITY, AI_TRANSCRIBE, AI_EXTRACT, AI_PARSE_DOCUMENT, plus AI_FILTER and AI_AGG (GA since Jan 2026).
- **Databricks:** task-specific AI Functions include `ai_classify`, `ai_extract`, `ai_summarize`, and `ai_mask`. Also the general-purpose `ai_query` against serving endpoints, which manages parallelism and retries **(GA)**, and `ai_parse_document` **(GA)**.
- **Cost** scales with rows × tokens. Controls:
  - Apply cheap SQL predicates before the LLM.
  - Dedup inputs and send only the needed columns, truncated.
  - Use the smallest model that passes eval.
  - Process incrementally, storing results keyed by an input hash.
  - Price a sample before the full run, and set budget alerts.
- **Determinism.** Outputs vary across runs and model versions. Pin model versions, use temperature 0 where it's supported, and constrain output to enums or JSON schemas. Materialize results rather than recomputing them in views, and store the model and prompt version as columns for lineage. Treat this as a tested pipeline step, not an ad-hoc query.

### What "AI-ready data" means

- **Unstructured ingestion.** Files land in governed volumes or stages with metadata. They're parsed into text plus layout (AI_PARSE_DOCUMENT or `ai_parse_document`, both **(GA)**), keeping page references for citations.
- **Quality.** Dedup and near-dup removal, boilerplate stripping, document versioning, and effective dates ("which policy is current?").
- **Lineage.** Answer → retrieved chunks → document → source system.
- **PII.** Classify and redact before embedding or calling external models (AI_REDACT **(GA)** and `ai_mask`), and propagate tags.
- **Semantics.** Table and column comments and metric definitions are now agent prompts, so they need to be maintained.
- **Ownership.** Eval sets as versioned tables, plus freshness SLAs and named owners.

### MCP and agents on the platform

MCP standardizes how agents call tools.
- The Snowflake-managed MCP server **(GA)** exposes Cortex Search, Cortex Analyst, Cortex Agents, SQL, and custom tools under RBAC.
- Databricks-provided MCPs run through Unity Gateway as UC securables: the Genie One MCP **(GA)**, plus Databricks SQL, sandbox, and web-search MCPs **(Preview)** (Beta). The older workspace managed MCP endpoints for UC functions, AI Search indexes, Genie Agents, and Databricks SQL are now legacy.

An agent is a new principal, which has governance implications:
- Prefer on-behalf-of-user OAuth over a broad service identity, and grant least privilege.
- Audit every tool call.
- Assume prompt injection through retrieved content, so gate write tools behind approval.
- Put rate and cost limits in a gateway: Databricks Unity Gateway **(GA)**, which replaces AI Gateway (now legacy) and whose guardrail service policies are **(Preview)** (Beta).

## Comparison

| Dimension | Snowflake Cortex | Databricks (Mosaic AI / Agent Bricks) |
|---|---|---|
| Models | Hosted multi-provider LLMs via AI functions and REST; region-dependent | Foundation Model APIs (pay-per-token or provisioned throughput), external models via Unity Gateway |
| Custom serving | Model Registry + Snowpark Container Services | Model Serving **(GA)** |
| Vector search | Cortex Search **(GA)**: hybrid + rerank, target lag | AI Search (ex-Vector Search): Delta Sync / Direct Access, hybrid, reranker |
| Text-to-SQL | Cortex Analyst + semantic views **(GA)**, invoked via Cortex Agents | Genie Agents (ex-Genie spaces), metric views **(GA)** |
| Agents | Cortex Agents, Snowflake CoWork (ex-Snowflake Intelligence) **(GA)** | Agent Bricks (code-first; CLI **(Preview)** (Beta)), Genie Agents for no-code; Knowledge Assistant and Supervisor Agent now legacy |
| Features | Feature Store **(GA)**; online **(Preview)** | Feature Engineering in UC **(GA)**; online on Lakebase **(GA)** (Beta on GCP) |
| Governance | RBAC on services; Horizon policies on sources; MCP under RBAC | UC grants on models, indexes, functions; Unity Gateway limits, usage tracking, inference tables |
| Eval / observability | AI Observability **(GA)** | MLflow 3 **(GA)**: tracing, scorers, LLM judges |
| Pricing | Credits; token-metered AI functions; search serving charges | DBUs; per-token or provisioned throughput; endpoint-based search |

## Interview questions

**Q: How would you build RAG over governed enterprise data?**

Parse into structured chunks carrying metadata and ACL attributes, redact PII, then embed into a hybrid index synced at the freshness the use case needs. At query time, a mandatory filter derived from the authenticated identity, or a per-audience index, ensures retrieval returns only what the user could read at the source. Answers cite chunks, and a versioned eval set gates every change.

**Q: Feature store: why, and what's point-in-time correctness?**

It gives one feature definition materialized offline for training and online for serving, which removes training-serving skew. Point-in-time correctness means each training row gets feature values as of the label's timestamp (an AS-OF join). A naive key join leaks the future and inflates offline metrics.

**Q: Cortex vs Mosaic AI?**

Cortex is SQL-first and managed: AI functions, Cortex Search, Analyst, and Agents run next to warehouse data under Snowflake RBAC, with little infrastructure to own (P10). Databricks offers more control: custom model serving, a gateway across providers, MLflow 3 evaluation, and agents and indexes governed in Unity Catalog. Pick by where the data and the team already are, and how much model customization you need. Feature status changes monthly, so verify it.

**Q: How do you keep a vector index fresh and secure?**

Drive it from the governed source table with incremental sync, monitor end-to-end lag against an SLA, and treat embedding-model changes as versioned backfills. Because indexes don't inherit per-user row policies by default, partition by audience or enforce server-side ACL filters, redact before embedding, and propagate deletions.

**Q: What does 'AI-ready data' mean concretely?**

Parsed, deduplicated, versioned content with lineage, classified and redacted before any model sees it. Add descriptive metadata and a semantic layer, freshness SLAs with owners, and versioned eval sets. It's BI's data-product discipline applied to documents and agents.

**Q: How do you control cost of LLM inference over large tables?**

Filter with cheap SQL first, dedup, and send minimal tokens to the smallest model that passes eval. Process only new or changed rows (cache by input hash), price a sample before the full run, set budgets, and materialize outputs.

**Q: Why do text-to-SQL agents need a semantic layer?**

Raw schemas don't encode grain, join paths, or which "revenue" is official, so the LLM guesses plausibly and wrongly. A semantic layer gives it governed metrics and verified queries.

**Q: How do you evaluate a RAG system?**

Score retrieval (recall@k, MRR against labeled sources) separately from generation (groundedness, correctness, citations) using human-calibrated LLM judges. Run it in CI on every chunking, embedding, prompt, or model change, and feed production traces back into the eval set.

**Q: What changes when agents reach the platform through MCP?**

The agent becomes a principal that can query and act. Use on-behalf-of-user auth with least privilege, audit every tool call, require approval for write tools because of prompt injection, and enforce rate and cost limits in a gateway.

## Takeaways
- Models are commodities; governed, fresh, well-defined data is the moat.
- Point-in-time joins prevent label leakage, and one definition prevents training-serving skew.
- Hybrid search plus rerank beats pure vectors, and index freshness is a pipeline SLA (P7).
- Indexes are copies: enforce ACLs at retrieval and redact before embedding (P9).
- Text-to-SQL is only as good as the semantic layer behind it.
- LLM-in-SQL is a pipeline step: filter, dedup, run incrementally, pin versions, materialize.
- An agent is a principal: least privilege, on-behalf-of auth, audited tool calls.

## Sources
- https://docs.snowflake.com/release-notes/2025/other/2025-11-04-cortex-aisql-operators-ga
- https://docs.snowflake.com/user-guide/snowflake-cortex/aisql
- https://docs.snowflake.com/fr/release-notes/2025/other/2025-10-16-ai-extract
- https://docs.snowflake.com/release-notes/2025/other/2025-08-21-aisql-ai-parse-document-layout-ga
- https://www.snowflake.com/en/news/press-releases/snowflake-intelligence-brings-agentic-AI-to-the-enterprise/
- https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents
- https://docs.snowflake.com/release-notes/2025/other/2025-11-04-cortex-agents-mcp
- https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents-mcp
- https://docs.snowflake.com/release-notes/2025/other/2025-04-17-semantic-views
- https://www.snowflake.com/blog/cortex-search-ai-hybrid-search
- https://docs.snowflake.com/user-guide/snowflake-cortex/cortex-search/query-cortex-search-service
- https://docs.snowflake.com/en/developer-guide/snowflake-ml/feature-store/overview
- https://docs.snowflake.com/en/release-notes/2026/other/2026-07-10-online-feature-store-preview
- https://docs.databricks.com/aws/machine-learning/feature-store/online-feature-store
- https://docs.databricks.com/aws/en/machine-learning/feature-store/migrate-from-online-tables
- https://docs.databricks.com/aws/en/ai-search/ai-search
- https://docs.databricks.com/aws/en/ai-search/create-ai-search
- https://pypi.org/project/databricks-ai-search/0.78/
- https://docs.databricks.com/aws/en/agents/concepts/faq
- https://docs.databricks.com/aws/en/agents/legacy/
- https://docs.databricks.com/aws/en/generative-ai/agent-bricks
- https://docs.databricks.com/aws/generative-ai/mcp/managed-mcp
- https://docs.databricks.com/sql/language-manual/functions/ai_parse_document
- https://docs.databricks.com/aws/large-language-models/ai-functions
- https://docs.databricks.com/mlflow3/genai
- https://docs.databricks.com/aws/en/mlflow3/genai/eval-monitor/concepts/scorers
- https://learn.microsoft.com/azure/databricks/ai-gateway
- https://www.databricks.com/blog/azure-databricks-lakebase-generally-available
- https://docs.databricks.com/aws/en/oltp/projects/sync-tables
- https://docs.databricks.com/gcp/en/machine-learning/feature-store/online-feature-store
- https://docs.databricks.com/aws/en/release-notes/product/2026/june
- https://docs.databricks.com/aws/en/release-notes/product/2026/july
- https://docs.databricks.com/aws/en/release-notes/product/2026/august
- https://docs.databricks.com/aws/en/release-notes/product/2026/september
- https://docs.databricks.com/aws/en/release-notes/product/2026/october
- https://docs.databricks.com/aws/en/release-notes/product/2026/april
- https://docs.databricks.com/aws/en/genie-agents/
- https://docs.databricks.com/aws/en/agents/concepts/
- https://docs.databricks.com/aws/en/agents/mcp-tools/built-in-mcp-services
- https://docs.databricks.com/aws/en/ai-gateway/
- https://docs.databricks.com/aws/en/ai-gateway/overview-serving-endpoints
- https://docs.databricks.com/aws/en/uc-semantics/metric-views
- https://docs.snowflake.com/en/release-notes/2026/other/2026-01-22-ai-filter-ga
- https://docs.snowflake.com/en/release-notes/2026/other/2026-01-22-ai-agg-ai-summarize-agg-ga
- https://docs.snowflake.com/en/release-notes/2025/other/2025-12-08-ai-redact-ga
- https://docs.snowflake.com/en/release-notes/2025/other/2025-11-21-ai-complete-ga
- https://docs.snowflake.com/en/release-notes/2026/other/2026-08-28-cortex-analyst-transition-cortex-agents
- https://docs.snowflake.com/en/user-guide/snowflake-cortex/snowflake-cowork
- https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/cortex-search-overview
- https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/cortex-search-costs
- https://docs.snowflake.com/en/developer-guide/snowflake-ml/inference/inference-overview
- https://docs.snowflake.com/en/release-notes/2025/other/2025-07-31-ai-observability-ga
