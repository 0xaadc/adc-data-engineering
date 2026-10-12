# Data Engineering & Architecture Primer

A principles-first primer for **data architect** interviews and for day-to-day architecture reasoning across **Snowflake, Databricks, and Kafka/streaming**.

Most interview prep is a list of facts to memorize. This primer starts from eleven first principles: why data engineering exists, why storage and compute separated, why immutable files make transactions possible, why "exactly-once" is really idempotency. Every platform feature is then explained as a consequence of those principles. Once you understand the mechanism, you can derive the answer.

**Start here → [primer/00-index.md](primer/00-index.md)**

- [First Principles](primer/01-first-principles.md): the eleven principles (P1–P11) everything else refers to
- [Snowflake vs. Databricks](primer/08-snowflake-vs-databricks.md): two bets on the same principles
- [Why X over Y](primer/15-why-x-over-y.md): the tradeoff bank
- [Cheat Sheet](primer/cheat-sheet.md) · [Question Bank](primer/question-bank.md) (self-test)

Content is current as of October 2026. Vendor features change quickly; anything not confirmed against official documentation is tagged **(⚠ verify)**.

Licensed under the terms in [LICENSE](LICENSE).
