# 12. Governance and Security

> As of October 2026. Tags: **(GA)** generally available · **(Preview)** public/private preview · **(⚠ verify)** not confirmed against official docs.

## Why this exists

Every dataset needs access that is least-privilege, auditable and the same no matter which engine or tool reads it (P9). Governance can fail in two ways. If it is too loose, you get breaches, fines and lost trust. If it is too tight, analysts export extracts to spreadsheets and personal buckets. Those shadow copies have no policy, no lineage and no audit trail, so they are worse than the risk you were trying to prevent. The architect's job is to build a control plane where the secure path is also the easiest path.

## Core concepts

### RBAC vs ABAC: why roles explode

In **RBAC**, privileges are granted to roles and roles are granted to users. That works while the number of distinctions is small. Each new dimension multiplies the role count: `ANALYST_EU_PII`, `ANALYST_US_NOPII`, and so on. Every new table then needs a grant for each role.

**ABAC** classifies data once with tags (`pii=email`, `region=eu`) and writes a few policies keyed on those tags and on user attributes such as group membership. Any object that carries a tag picks up the matching policy automatically.

```
RBAC:  #roles    ≈ regions × sensitivity levels × functions    (multiplicative)
ABAC:  #policies ≈ distinct rules; objects inherit via tags     (additive)
```

In practice you use both. RBAC answers the coarse question ("can you use this schema?"). ABAC answers the fine one ("which rows, and is this column masked?").

### Snowflake role hierarchy

- **System roles.** ACCOUNTADMIN sits at the top: give it to very few people and require MFA. SECURITYADMIN manages grants. USERADMIN creates users and roles. SYSADMIN owns warehouses and databases. ORGADMIN and PUBLIC also exist.
- **Ownership** belongs to a role, never to a user. The owning role has full control of the object.
- **Access roles vs functional roles.** Access roles hold privileges on objects (`SALES_RO`, `SALES_RW`). Functional roles map to jobs (`ANALYST_SALES`) and are made up of access roles. Users get functional roles, provisioned from the IdP through SCIM. Roll custom roles up to SYSADMIN so admins can still manage the objects.
- **Future grants** cover tables that don't exist yet, so new tables don't silently break access.
- **Managed access schemas** take grant authority away from object owners and give it to the schema owner (or MANAGE GRANTS). This stops ad hoc sharing.

```sql
CREATE SCHEMA sales.curated WITH MANAGED ACCESS;
GRANT SELECT ON FUTURE TABLES IN SCHEMA sales.curated TO ROLE sales_ro;
GRANT ROLE sales_ro TO ROLE analyst_sales;   -- access role -> functional role
GRANT ROLE analyst_sales TO ROLE sysadmin;   -- keep hierarchy rooted
```

### Unity Catalog privilege model

- **Hierarchy.** Metastore → catalog → schema → table, view, volume, function or model.
- **Inheritance.** Privileges flow downward. SELECT on a catalog covers every current and future table in it, which is UC's version of future grants. Reading a table needs `USE CATALOG` + `USE SCHEMA` + `SELECT`.
- **Ownership.** The owner (a user, group or service principal) has full control. `MANAGE` lets someone who isn't the owner administer grants.
- **Identity.** Account-level users, groups and service principals are synced from the IdP through SCIM. Grant to groups, not to individuals. Workspace-catalog binding limits which workspaces can see a catalog, which is how environments are isolated.
- **Key difference from Snowflake.** A Snowflake session runs under an active role, with optional secondary roles. In UC, a user's effective rights are the union of all their groups, and there is no role switching. Policies written against `CURRENT_ROLE()` and policies written against group membership therefore behave differently.

### Row-level security and column masking

A policy is a function that the engine injects into the query plan at runtime. A row filter becomes an extra predicate. A mask wraps the column in an expression. The same table returns different results depending on who is asking.

- **The old approach: dynamic views.** These use `CASE WHEN is_member(...)` inside a view. They work, but the logic is copied into every view, and anyone with base-table access bypasses them.
- **Snowflake (Enterprise Edition).**
  - *Masking policies* apply to columns.
  - *Row access policies* apply to tables and often look up a mapping table of user to allowed values.
  - *Tag-based masking* assigns a masking policy to a tag, with one policy per data type. Every column carrying the tag (directly or inherited from a table, schema or database) is masked.
  - *Projection policies* allow a column in WHERE or JOIN but not in SELECT output **(GA)**.
  - *Aggregation policies* return only aggregates above a minimum group size **(GA)**.
- **Databricks.**
  - Per-table *row filters* and *column masks* are SQL UDFs attached with `ALTER TABLE … SET ROW FILTER` or `ALTER COLUMN … SET MASK`.
  - *ABAC policies* **(GA, April 2026)** are defined at catalog or schema scope and keyed on governed tags. They apply automatically to every matching table, materialized view and streaming table.
  - ABAC needs serverless compute or DBR 16.4+. Older runtimes can't read ABAC-secured tables at all.
  - ABAC policies can't be attached directly to views. If two distinct filters or masks resolve for the same user and object, the query errors, so design policies that don't overlap.
- **Performance.** The policy runs on every query. A mapping-table lookup turns into a join on every read. A complex UDF can block pruning and caching. A mask on a join or filter key can defeat partition pruning or change join results. Keep policies simple and deterministic, and benchmark them under realistic roles.

### Classification and tags

You can't protect columns you haven't found, and new columns arrive every day. Automatic classification samples column values, infers semantic categories (email, phone, national ID) and applies tags.

- **Snowflake.** *Automatic sensitive data classification* **(GA, 2025)** works through a classification profile set on a database or schema. The profile sets the scan schedule and can auto-apply system or user tags, and those tags trigger tag-based masking. Tag inheritance down the hierarchy is native. Automatic tag propagation to derived objects is newer **(⚠ verify status)**.
- **Databricks.** *Governed tags* **(GA)** are account-level tag definitions that control allowed values and who can assign them. *Data Classification* **(GA, April 2026)** scans UC tables and tags what it finds, and ABAC policies key off those tags. Scanning views is in **(Beta, Sept 2026)**.
- **Why this scales.** You write one policy per sensitivity class instead of per-column grants. A newly tagged column is protected immediately. "Who can see PII?" becomes a query over tags and policies.
- **Failure mode.** Classifiers miss things, especially in free-text and JSON columns. Treat classification as a safety net, and require producers to tag at creation as part of the contract.

### Lineage, audit and access history

Lineage is a graph of which upstream columns produced which downstream columns. The engine captures it by parsing the queries it actually executes, not from documentation. It has four main uses:

- **Impact analysis** before a change.
- **Compliance:** where PII flows, and right-to-erasure scope.
- **Debugging:** which upstream source broke a dashboard.
- **Access reviews.**

| Platform | What it records |
|---|---|
| Snowflake | `ACCOUNT_USAGE.ACCESS_HISTORY` (Enterprise) records which columns each query read (both the objects named and the base objects underneath) and which it wrote, so it supports column lineage and "who read PII" audits. The Snowsight lineage graph (Horizon) visualizes the same data. `QUERY_HISTORY` and `LOGIN_HISTORY` round out the audit trail. `ACCOUNT_USAGE` views lag by minutes to hours. |
| Databricks | UC captures table and column lineage automatically for work on UC compute. It is exposed in Catalog Explorer and in the system tables `system.access.table_lineage`, `system.access.column_lineage`, `system.access.audit` and `system.query.history`. |

The blind spots are the same in both: Python outside the platform, BI extracts, and the other platform. Lineage only covers what the engine executed.

### Data contracts and ownership

A contract is the producer's promise to consumers. It covers:

- **Schema:** names, types, nullability.
- **Semantics:** what one row means, units, keys.
- **SLAs:** freshness and availability.
- **Quality rules.**
- **Owner** and escalation path.
- **Versioning** and deprecation policy.

It is enforced in three places: producer CI rejects breaking changes against the contract, ingestion validates and quarantines, and the catalog carries owner and domain tags. Contracts are organizational more than technical (P8). The tooling is a YAML file and a check. The hard part is getting producers to accept accountability and giving consumers a channel to negotiate change. Data mesh formalizes this as domain ownership; see [§15](15-why-x-over-y.md).

### Network and encryption

Both platforms encrypt data at rest and in transit by default. Architects choose the layers above that.

- **Private connectivity.**
  - Snowflake supports AWS PrivateLink, Azure Private Link and GCP Private Service Connect on **Business Critical** and higher. Network policies and network rules (IP and endpoint allow-lists) are available on every edition.
  - Databricks supports front-end and back-end PrivateLink, a customer-managed VPC, and secure cluster connectivity (no public IPs on nodes). On AWS and GCP these need the **Enterprise** tier. On Azure the equivalent is Premium **(⚠ verify)**.
- **Customer-managed keys.**
  - Snowflake **Tri-Secret Secure** builds a composite master key from a Snowflake key and a customer key held in the cloud KMS. Revoking the customer key makes the data unreadable, which acts as a kill switch. It requires Business Critical **(⚠ verify)**.
  - Databricks CMK covers managed services and workspace storage and needs the Enterprise tier on AWS and GCP.
  - The tradeoff: you now own key rotation, and a lost key is an outage you caused.
- **Posture.** Snowflake's **Trust Center** runs scanner packages; Security Essentials is on by default and a CIS Benchmarks package is available. Databricks offers the compliance security profile and enhanced security monitoring.

### Governing across two platforms

- **Identity.** Use one IdP and SCIM it into both platforms. Groups become the shared vocabulary.
- **Taxonomy.** Define one tag set (sensitivity, PII type, domain, owner) and apply it in both.
- **One enforcement point per dataset.** The platform that owns the data enforces the policy. The other platform reads through a governed path: sharing, or catalog federation over open tables. Policies live in the engine and catalog, not in the files (P11). Anyone who gets raw object-storage credentials bypasses every mask, so only the owning engine should get direct storage access. What each external path enforces varies **(⚠ verify per access path)**.
- **No 1:1 translation.** Snowflake role-context functions and UC group-membership functions have different semantics. Policy engines that author once and push down to both exist, but they add another system to operate.
- **Unified audit.** Land both platforms' audit logs in one place.

See [§08](08-snowflake-vs-databricks.md).

## Comparison

| Concern | Snowflake | Databricks (Unity Catalog) |
|---|---|---|
| Grant target | Roles in a hierarchy; session activates a role (+ secondary roles) | Users, groups, service principals; union of group rights |
| Access to new objects | Future grants; managed access schemas | Inheritance from catalog/schema grants |
| Row-level security | Row access policy | Row filter (per table) or ABAC policy (tag-scoped) **(GA)** |
| Column protection | Masking policy, tag-based masking, projection policy | Column mask or ABAC policy |
| Aggregate-only access | Aggregation policy | No direct equivalent; use aggregated views **(⚠ verify)** |
| Tags | Object tags with hierarchy inheritance | Governed tags **(GA)** |
| Auto-classification | Automatic sensitive data classification **(GA)** | Data Classification **(GA)** |
| Lineage / audit | ACCESS_HISTORY, QUERY_HISTORY, Snowsight lineage | UC lineage, `system.access.*`, `system.query.history` |
| Private networking | PrivateLink / PSC (Business Critical) | PrivateLink (Enterprise tier, AWS/GCP) |
| Customer keys | Tri-Secret Secure (Business Critical **⚠ verify**) | CMK (Enterprise tier) |
| Posture scanning | Trust Center | Compliance security profile, enhanced security monitoring |

## Interview questions

**Q: RBAC or ABAC: when do you use each?**

RBAC is right for coarse, stable boundaries such as schema or catalog access per team, because it's simple to audit. It breaks down when access depends on several attributes at once (region × sensitivity × function), since the role count grows multiplicatively and every new table needs grants. ABAC moves the decision onto tags and user attributes, so policy count tracks the number of rules rather than tables. The cost is that correctness now depends on tagging quality: a missed tag is an unprotected column. Use RBAC for "can you reach it" and ABAC for "what do you see".

**Q: How would you design a Snowflake role hierarchy?**

Use two layers. Access roles hold object privileges (read and write per schema or database). Functional roles model jobs and are composed of access roles, and only functional roles are granted to users, via SCIM from the IdP. Roll everything up to SYSADMIN so admins keep control, keep ACCOUNTADMIN to a few MFA-protected people, and run grants through SECURITYADMIN. Add managed access schemas and future grants so object owners can't grant ad hoc and new tables inherit access automatically. Define it all in IaC so drift shows up in a diff.

**Q: How does the Unity Catalog permission model differ from Snowflake's?**

UC grants go directly to users, groups or service principals, and effective rights are the union of a user's groups. Snowflake sessions instead run under an active role. UC privileges inherit down catalog → schema → table, so a catalog-level grant covers future tables without a separate future-grant mechanism. Reads need `USE CATALOG` and `USE SCHEMA` on the path as well as `SELECT`. The practical consequence is that policies and access reviews port awkwardly between the two, because "current role" and "member of group" are different predicates.

**Q: How do you implement row-level security, and what does it cost?**

Attach a row policy (Snowflake row access policy, Databricks row filter or ABAC policy) that the engine injects as a predicate at query time. Prefer this over dynamic views, which duplicate logic and are bypassed by anyone with base-table access. The cost is per-query evaluation. A mapping-table lookup becomes a join on every read, and complex UDFs can block pruning and caching. Keep policies deterministic and simple, index the logic on attributes that already exist (group membership, region), and load-test with realistic roles before rollout.

**Q: Why tags plus policies instead of per-column grants?**

Per-column grants scale with columns × roles and must be repeated for every new table, so coverage decays. With tags, you classify once (automatically, then human-reviewed), and a policy bound to the tag protects every current and future column carrying it. "Who can see PII?" becomes a query rather than an audit project. The failure mode is classifier false negatives, so pair auto-classification with producer-side tagging in the contract.

**Q: An auditor asks who accessed customer PII in the last 90 days. How do you answer?**

Join PII-tagged columns to access logs. In Snowflake, `ACCESS_HISTORY` lists the base columns each query touched, joined with tag references and `QUERY_HISTORY` for the user and role. In Databricks, use `system.access.audit` plus lineage and query-history system tables, filtered on tagged tables. The weak point is anything outside the platform: extracts, BI caches and copies on the other platform. That is why limiting direct storage access matters as much as the logs. Check log retention ahead of time, because the answer is only as long as your retention window.

**Q: What is a data contract and where is it enforced?**

A data contract is a versioned agreement covering schema, semantics, freshness SLA, quality rules, owner and deprecation policy. It is enforced at the producer's CI (block breaking changes), at ingestion (validate, then quarantine or reject), and in the catalog (owner and domain metadata). The technology is trivial. What makes it work is organizational: a named producer accepts accountability, and changes go through versioning with notice instead of being discovered when a dashboard breaks (P8).

**Q: When do you need customer-managed keys or Tri-Secret Secure?**

When regulation or contracts require you to be able to revoke the provider's ability to read your data. A composite or customer key gives you a kill switch. It costs higher-tier editions, key-management operations, and a new failure mode: revoke or lose the key and you have caused your own outage. If the requirement is just "encrypted at rest," both platforms already satisfy it by default, and CMK adds risk without value.

**Q: How do you govern consistently across Snowflake and Databricks?**

Unify the parts that do translate: one IdP with SCIM groups into both, one tag taxonomy, and one owner per dataset. Enforce each dataset's policy in the platform that owns it, and give the other platform governed access paths rather than raw storage credentials, because masks live in the engine, not the files. Accept that policy semantics (role context vs group membership) don't translate 1:1. Either maintain both from one IaC definition, or adopt a policy engine that pushes down to both and accept the extra moving part. Centralize the audit logs for one view.

**Q: Governance is so tight that analysts are exporting data to spreadsheets. What do you do?**

Treat it as a governance failure. Shadow copies have no masking, lineage or audit, so they are worse than the original risk. Measure time-to-access, offer self-service request flows with auto-approval for low-sensitivity data, and replace blanket denial with masked or aggregated access so analysts can work without seeing raw PII. Then close the export paths that remain.

## Takeaways

- Govern the data, not the tool: attach policies to data in the catalog (P9).
- RBAC for reach, ABAC for visibility; roles multiply, tags don't.
- Snowflake: access roles → functional roles → users, with managed access schemas and future grants.
- UC: grants inherit down the hierarchy, and rights are the union of groups; there is no active role.
- Row and column policies are query-time plan rewrites. Their cost is per query, so keep them simple.
- Classification is a safety net; producer tagging in the contract is the guarantee.
- Lineage covers only what the engine executed; extracts are blind spots.
- Raw storage credentials bypass every policy. Only the owning engine should hold them.
- Over-tight governance creates shadow copies, which are a worse risk.

## Sources

- https://docs.databricks.com/aws/en/release-notes/product/2026/april
- https://docs.databricks.com/aws/en/data-governance/unity-catalog/access-control
- https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/policies
- https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/requirements
- https://docs.databricks.com/aws/en/release-notes/data-classification
- https://www.databricks.com/blog/abac-row-filtering-and-column-masking-policies-governed-tags-and-data-classification-are-now
- https://docs.databricks.com/aws/security/keys/customer-managed-keys
- https://docs.databricks.com/aws/security/network/classic/privatelink
- https://docs.snowflake.com/en/release-notes/2025/9_05
- https://docs.snowflake.com/en/user-guide/tutorials/sensitive-data-auto-classification
- https://docs.snowflake.com/en/release-notes/2024/other/2024-05-03-policies
- https://docs.snowflake.com/en/user-guide/admin-security-privatelink
- https://docs.snowflake.com/en/user-guide/trust-center/overview
