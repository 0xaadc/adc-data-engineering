# 12. Governance and Security

> As of October 2026. Tags: **(GA)** generally available · **(Preview)** public/private preview · **(⚠ verify)** not confirmed against official docs.

## Why this exists

Access must be least-privilege, auditable and identical in every engine and tool that reads the data (P9). Governance fails in two directions. Too loose leads to breach and compliance failures. Too tight leads analysts to export extracts to spreadsheets and personal buckets, and those shadow copies have no policy, lineage or audit, which is worse. The goal is a control plane where the secure path is also the easiest one.

## Core concepts

### RBAC vs ABAC

In **RBAC**, privileges go to roles and roles go to users. Every new dimension multiplies the role count (`ANALYST_EU_PII`, `ANALYST_US_NOPII`, …), and every new table needs a grant for each role. In **ABAC**, you tag data once (`pii=email`, `region=eu`) and write a few policies keyed on tags and user attributes. Any object that carries a tag inherits the matching policy.

```
RBAC:  #roles    ≈ regions × sensitivity × functions   (multiplicative)
ABAC:  #policies ≈ distinct rules; objects inherit by tag (additive)
```

Use RBAC for reach ("can you use this schema?") and ABAC for visibility ("which rows, masked or not?").

### Snowflake role hierarchy

- **System roles.** ACCOUNTADMIN for a few people with MFA. SECURITYADMIN manages grants. USERADMIN manages users and roles. SYSADMIN owns warehouses and databases. ORGADMIN and PUBLIC also exist. Objects are owned by roles, not users.
- **Access roles and functional roles.** Access roles hold object privileges (`SALES_RO`). Functional roles (`ANALYST_SALES`) are made of access roles. Only functional roles go to users, synced from the IdP through SCIM. Roll custom roles up to SYSADMIN.
- **Future grants** cover tables that don't exist yet.
- **Managed access schemas** take grant authority away from object owners, which centralizes control.

```sql
CREATE SCHEMA sales.curated WITH MANAGED ACCESS;
GRANT SELECT ON FUTURE TABLES IN SCHEMA sales.curated TO ROLE sales_ro;
GRANT ROLE sales_ro TO ROLE analyst_sales;
GRANT ROLE analyst_sales TO ROLE sysadmin;
```

### Unity Catalog privilege model

- **Hierarchy and inheritance.** Metastore → catalog → schema → table, view, volume, function or model. Privileges inherit downward, so SELECT on a catalog covers its future tables too. A read needs `USE CATALOG` + `USE SCHEMA` + `SELECT`.
- **Ownership.** The owner (a user, group or service principal) has full control. `MANAGE` delegates administration of grants.
- **Identity and isolation.** Users, groups and service principals are account-level and synced by SCIM. Workspace-catalog binding isolates environments.
- **Difference from Snowflake.** Snowflake sessions run under an *active role*. UC rights are the *union of a user's groups*, with no role switching. That is why policies don't port 1:1 between the two.

### Row-level security and column masking

A policy is a function the engine injects into the plan at query time. A row filter becomes a predicate, and a mask wraps the column in an expression. The older approach, dynamic views with `CASE WHEN is_member(...)`, duplicates logic in every view, and anyone with base-table access bypasses it.

- **Snowflake (Enterprise).**
  - *Masking policies* on columns and *row access policies* on tables, the latter often backed by a mapping table.
  - *Tag-based masking* binds a masking policy to a tag, one per data type. Every column carrying the tag, directly or inherited from a table, schema or database, is masked.
  - *Projection policies* allow a column in WHERE or JOIN but not in SELECT output **(GA)**.
  - *Aggregation policies* return only aggregates above a minimum group size **(GA)**.
- **Databricks.**
  - Per-table *row filters* and *column masks* are SQL UDFs.
  - *ABAC policies* **(GA, April 2026)** are defined at catalog or schema scope on governed tags and apply to every matching table, materialized view and streaming table.
  - ABAC needs serverless or DBR 16.4+; older runtimes can't read ABAC-secured tables. Policies can't attach directly to views. Two distinct filters or masks resolving for the same user and object cause an error, so design policies that don't overlap.
- **Performance.** Policies run on every query. A mapping-table lookup becomes a join on every read, complex UDFs can block pruning and caching, and masking a join key can change join results. Keep policies simple and deterministic, and benchmark them under realistic roles.

### Classification and tags

You can't protect columns you haven't found. Classifiers sample values, infer categories (email, national ID) and apply tags.

- **Snowflake.** *Automatic sensitive data classification* **(GA, 2025)** uses a classification profile on a database or schema that sets the schedule and can auto-apply tags. Those tags then trigger tag-based masking. Automatic tag propagation to derived objects is newer **(⚠ verify status)**.
- **Databricks.** *Governed tags* **(GA)** are account-level definitions that control allowed values and who may assign them. *Data Classification* **(GA, April 2026)** tags UC tables, and ABAC policies key off those tags. Scanning views is **(Beta, Sept 2026)**.

Tags plus policies scale because you write one policy per sensitivity class instead of per-column grants. New tagged columns are protected immediately, and "who can see PII?" becomes a query. Classifiers miss free-text and JSON fields, so require producers to tag at creation as part of the contract.

### Lineage, audit and access history

Lineage is captured by the engine parsing the queries it executes. It is used for impact analysis before a change, for compliance (PII flow, erasure scope) and for debugging (which upstream broke this?).

- **Snowflake.** `ACCOUNT_USAGE.ACCESS_HISTORY` (Enterprise) records the columns each query read, both the objects named and the base objects beneath them, and the columns it wrote. The Snowsight lineage graph (Horizon) visualizes this. `QUERY_HISTORY` and `LOGIN_HISTORY` complete the audit trail. These views lag by minutes to hours.
- **Databricks.** UC captures table and column lineage automatically on UC compute. It is exposed through the system tables `system.access.table_lineage`, `system.access.column_lineage`, `system.access.audit` and `system.query.history`.

Both have the same blind spots: external scripts, BI extracts and the other platform.

### Data contracts and ownership

A contract specifies:

- **Schema:** names, types, nullability.
- **Semantics:** grain, units, keys.
- **SLAs:** freshness and availability.
- **Quality rules.**
- **Owner** and escalation path.
- **Versioning** and deprecation policy.

It is enforced in producer CI (breaking-change check), at ingestion (validate, quarantine) and in the catalog (owner tags). It is organizational more than technical (P8). The tooling is a YAML file and a check. The hard part is producer accountability plus a channel for negotiating change. Data mesh formalizes domain ownership ([§15](15-why-x-over-y.md)).

### Network and encryption

Both platforms encrypt at rest and in transit by default. The layers above that:

| Layer | Snowflake | Databricks |
|---|---|---|
| Private connectivity | PrivateLink / Private Link / PSC: **Business Critical**+. Network policies and rules on all editions. | Front-end and back-end PrivateLink, customer-managed VPC, secure cluster connectivity: **Enterprise** tier (AWS/GCP). Azure: Premium **(⚠ verify)**. |
| Customer keys | **Tri-Secret Secure** builds a composite key from a Snowflake key and a customer KMS key. Revoking yours makes the data unreadable. Business Critical **(⚠ verify)**. | CMK for managed services and storage: Enterprise tier (AWS/GCP). |
| Posture | **Trust Center** scanner packages (Security Essentials on by default, CIS Benchmarks) | Compliance security profile, enhanced security monitoring |

CMK is a kill switch you also own. Rotation, and the outage you cause if you lose the key, are your problem.

### Governing across two platforms

- **Identity.** Use one IdP and SCIM groups into both. Groups are the shared vocabulary.
- **Taxonomy.** One tag set: sensitivity, PII type, domain, owner.
- **One enforcement point per dataset.** The owning platform enforces, and the other reads through governed paths such as sharing or catalog federation. Policies live in the engine and catalog, not in the files (P11), so raw storage credentials bypass every mask. What each external path enforces varies **(⚠ verify per access path)**.
- **No 1:1 translation.** Role-context and group-membership predicates differ. Keep both platforms in one IaC definition, or adopt a policy engine that pushes down to both, at the cost of another system to operate.
- **Unified audit.** Centralize both platforms' audit logs. See [§08](08-snowflake-vs-databricks.md).

## Comparison

| Concern | Snowflake | Databricks (Unity Catalog) |
|---|---|---|
| Grant target | Roles; session activates a role (+ secondary roles) | Users, groups, service principals; union of groups |
| New objects | Future grants; managed access schemas | Inheritance from catalog/schema |
| Row-level | Row access policy | Row filter or ABAC policy **(GA)** |
| Column | Masking, tag-based masking, projection policy | Column mask or ABAC policy |
| Aggregate-only access | Aggregation policy | Aggregated views; no direct equivalent **(⚠ verify)** |
| Tags | Object tags, inherited down hierarchy | Governed tags **(GA)** |
| Auto-classification | Sensitive data classification **(GA)** | Data Classification **(GA)** |
| Lineage / audit | ACCESS_HISTORY, QUERY_HISTORY, Snowsight lineage | UC lineage, `system.access.*`, `system.query.history` |

## Interview questions

**Q: RBAC or ABAC: when do you use each?**

RBAC suits coarse, stable boundaries such as catalog or schema access per team, because it's easy to audit. It breaks when access depends on several attributes at once, since roles multiply and every new table needs grants. ABAC moves the decision onto tags and user attributes, so policy count tracks rules rather than tables. The catch is that correctness now depends on tagging quality: a missed tag is an exposed column.

**Q: How would you design a Snowflake role hierarchy?**

Use access roles for object privileges, compose them into functional roles that model jobs, and grant only functional roles to users via SCIM. Roll everything up to SYSADMIN, keep ACCOUNTADMIN to a few MFA-protected people, and administer grants through SECURITYADMIN. Managed access schemas and future grants stop ad hoc grants and keep new tables covered. Define all of it in IaC so drift shows up in a diff.

**Q: How does Unity Catalog's permission model differ from Snowflake's?**

UC grants go to users, groups or service principals, and effective rights are the union of a user's groups. Snowflake evaluates the session's active role. UC privileges inherit down catalog → schema → table, so a catalog grant covers future tables without a separate mechanism. Because "current role" and "member of group" are different predicates, policies and access reviews port awkwardly between the two.

**Q: How do you implement row-level security, and what does it cost?**

Attach a row policy (row access policy, row filter or ABAC policy) that the engine injects as a predicate. This is better than dynamic views, which duplicate logic and are bypassed through base tables. The cost is evaluation on every query: a mapping-table lookup becomes a join on every read, and opaque UDFs can block pruning and caching. Keep the logic deterministic, base it on existing attributes such as group or region, and load-test with real roles.

**Q: Why tags plus policies instead of per-column grants?**

Per-column grants scale with columns × roles and must be repeated for every new table, so coverage decays. You classify once, automatically and then reviewed by a person. A tag-bound policy then protects every current and future column carrying the tag, and access questions become queries. The failure mode is classifier false negatives, which is why producer-side tagging belongs in the contract.

**Q: An auditor asks who accessed customer PII in the last 90 days. How do you answer?**

Join PII-tagged columns to access logs. In Snowflake that is `ACCESS_HISTORY` (base columns per query) with tag references and `QUERY_HISTORY`. In Databricks it is `system.access.audit` with lineage and query-history system tables. Retention bounds the answer, so check it ahead of time. Anything outside the platform, such as extracts or copies on the other platform, is invisible, which is why restricting direct storage access matters.

**Q: What is a data contract and where is it enforced?**

A data contract is a versioned agreement on schema, semantics, freshness SLA, quality rules, owner and deprecation policy. It is enforced in producer CI, at ingestion through validation and quarantine, and through catalog metadata. The technology is trivial. It works only if a named producer is accountable and changes ship as versions with notice (P8).

**Q: When do you need customer-managed keys or Tri-Secret Secure?**

When regulation or contracts require that you can revoke the provider's ability to read your data. It costs higher editions or tiers, key operations, and a new failure mode: a revoked or lost key is a self-inflicted outage. If the requirement is only "encrypted at rest," default encryption already meets it.

**Q: How do you govern consistently across Snowflake and Databricks?**

Unify identity (one IdP, SCIM groups), taxonomy (one tag set) and ownership (one owner per dataset). Enforce each dataset's policy in the platform that owns it, and give the other governed access paths, never raw storage credentials, because masks live in the engine. Accept that policy semantics don't translate 1:1. Generate both from one IaC definition or use a push-down policy engine, and centralize audit logs.

**Q: Analysts are exporting data to spreadsheets because access is too slow. What do you do?**

Treat it as a governance failure, because shadow copies have no masking, lineage or audit. Measure time-to-access, add self-service requests with auto-approval for low-sensitivity data, and offer masked or aggregated access instead of denial. Then close the export paths that remain.

## Takeaways

- Govern the data, not the tool: policies attach to data in the catalog (P9).
- RBAC for reach, ABAC for visibility; roles multiply, tags don't.
- Snowflake: access roles → functional roles → users, plus managed access and future grants.
- UC: grants inherit down the hierarchy, and rights are the union of groups; there is no active role.
- Row and column policies are query-time plan rewrites, so their cost is per query.
- Classification is a net; producer tagging in the contract is the guarantee.
- Raw storage credentials bypass every policy. Only the owning engine should hold them.
- Over-tight governance creates shadow copies, a worse risk.

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
