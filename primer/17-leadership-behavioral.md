# 17. Leadership and Behavioral (Architect / Co-Lead)

> As of October 2026. Tags: **(GA)** generally available · **(Preview)** public/private preview · **(⚠ verify)** not confirmed against official docs.

## Why this exists

An architect or co-lead is hired for judgment, influence without authority, and raising the team's floor, not only for technical depth. Interviewers probe the shift from "I build" to "I make the team build well": written decisions, standards people adopt willingly, disagreements resolved without damage, and a team that ships without you in the room. Behavioral rounds test this with past behavior, the best available predictor.

## Core concepts

### Setting standards without becoming a bottleneck

- **Golden paths (paved roads).** Make the right way the easy way: project templates, a reference pipeline, CI checks, Terraform modules, default tags and policies. Teams can leave the path, but then they own the extra operational cost. Paved roads scale your judgment, while approval gates queue work behind you.
- **Architecture Decision Records (ADRs).** A short, numbered document per significant decision. It has four parts:
  - **Context:** the forces and constraints.
  - **Decision:** what we will do.
  - **Consequences:** what gets easier, what gets harder, and what we're now committed to.
  - **Status:** proposed, accepted, or superseded.

  Written decisions scale: they're asynchronous, searchable, survive turnover, and stop the same debate recurring every quarter.
- **Design reviews that decide.**
  1. The proposer circulates a 1–3 page pre-read 24–48 hours ahead, stating the *explicit questions* and the decision deadline.
  2. Name the decision owner up front. Reviewers advise; the owner decides.
  3. Timebox it, and park bikeshed topics (naming, formatting) in comments or the linter.
  4. End with decide-and-commit, recorded as an ADR that includes the dissent.

### Technical disagreements

- **Classify the door.** Two-way doors are reversible: pick a library, a pipeline pattern, a pilot. Decide fast at the lowest competent level. One-way doors are costly to reverse: a primary platform, a catalog of record (P11), a data contract with external consumers (P8). Slow down, write it up, and widen the review.
- **Replace opinions with evidence.** Agree on the evaluation criteria *before* comparing options. Then run a timeboxed prototype or benchmark on representative data, and look at cost and failure-mode data.
- **Disagree and commit.** Once the owner decides, everyone executes fully, and the dissent is recorded in the ADR along with a revisit trigger: "if p95 latency exceeds X or cost exceeds Y, we reopen."
- **Escalate cleanly.** Escalate together, with a joint one-pager: options, criteria, agreements, and the single point of disagreement. A one-sided escalation burns trust.

### Pushing back on stakeholders

Say no to the ask and yes to the need. "We need real-time" is usually a solution, not a requirement. Ask which decision the data drives and what it costs when the data is an hour old (P7). Then offer options with prices:

| Option | Freshness | Relative cost | Risk |
|---|---|---|---|
| A. Daily batch (today) | 24h | 1x | Low |
| B. Hourly incremental | ~1h | ~1.5x | Low |
| C. Streaming | seconds | 3–5x plus on-call | Higher |

The same move works for "we need everything": ask for the top three questions, deliver those, and measure usage before expanding. Put the agreed requirement in writing.

### Team topology and the co-lead split

- **Team Topologies vocabulary:**
  - *Stream-aligned* teams own a business flow end to end.
  - A *platform* team provides self-service capabilities as an internal product.
  - *Enabling* teams coach and then step away.
  - *Complicated-subsystem* teams own deep specialties.

  A data platform team should run as a product team with users, SLAs, docs, and a roadmap. It enables rather than gatekeeps: the measure of success is how much other teams ship without filing a ticket.
- **Co-lead split.** One common shape is that one lead owns *architecture and technical direction* (standards, ADRs, design reviews, the platform roadmap). The other owns *delivery and people* (planning, staffing, 1:1s, stakeholder cadence). To avoid split-brain:
  - Write down a RACI per decision type.
  - Hold a weekly sync to align before meeting the team.
  - Present one voice to the team, with disagreements settled in private.
  - Have a tie-break rule (each lead owns their domain, or escalate to a named person).
  - Review the split quarterly.

### Prioritizing tech debt

- **Tie debt to a metric** so it competes on equal terms with features:
  - risk: incident count, audit findings, single points of failure
  - cost: monthly spend from inefficient jobs
  - velocity: lead time, how often a change touches a fragile area
- **Use a standing budget** (for example 15–20% of capacity) rather than heroic "debt sprints," plus **fix-on-touch** when you're already changing the code.
- **Keep a debt register** with owner, impact, and escalation trigger. Debt in a stable area nobody touches can stay.

### Scaling from individual contributor to team scope

- **Delegate outcomes, not tasks.** Say "own ingestion reliability; here's the SLA," not "fix these three jobs." Match the support level to the person's experience, and step back as they grow.
- **Document for the reader who isn't there:** runbooks, ADRs, onboarding. Test: can a new hire ship in week two?
- **Bus factor:** a system only one person can operate is a risk. Fix it with pairing, rotation, and recorded walkthroughs.
- **Hiring signals:** tradeoff reasoning over tool trivia, ownership of past failures, clear writing, business curiosity. Use structured interviews with a rubric.
- **Your metric changes** to team throughput, quality, and growth, not your own commits.

### STAR+L template and story bank

**STAR+L:** **S**ituation (2 sentences of context) → **T**ask (your responsibility and the stakes) → **A**ction (what *you* did, with the reasoning, in about 60% of the time) → **R**esult (quantified: cost, latency, incidents, adoption) → **L**earning (what you'd do differently, and how you've applied it since).

Checklist: a clear "I" (not just "we"); the alternatives you rejected and why; a tradeoff you consciously accepted; at least one number in the Result; a stakeholder or people dimension; an honest Learning, not a humble-brag; 2–3 minutes, with detail held back for follow-ups.

**Story prompts to prepare from your own experience:**
1. A platform or architecture decision you made, and its tradeoffs.
2. A time you pushed back on a stakeholder or a senior person.
3. An incident you led: detection, mitigation, the post-incident review.
4. A cost reduction, how you found it, and what it cost in freshness or effort.
5. A time you were wrong, and how you found out.
6. A standard or practice you got a team to adopt without authority.
7. Mentoring someone into owning something you used to own.
8. A delivery under ambiguity or a tight deadline: what you cut, and why.

## Comparison

| Gatekeeping | Enabling |
|---|---|
| Every change needs your approval | Paved road plus automated checks; review only one-way doors |
| Knowledge lives in your head | ADRs, runbooks, templates |
| Measures itself by work reviewed | Measures itself by work others ship without help |
| Scales with your calendar | Scales with documentation and tooling |

## Interview questions

**Q: How do you run a design review?**

A short pre-read goes out 24–48 hours ahead, with explicit questions, a deadline, and a named decision owner. The meeting covers the open questions and one-way-door risks and parks style. It closes with decide-and-commit, captured as an ADR with the dissent and a revisit trigger.

**Q: Can you tell me about a technical disagreement (structure of a strong answer)?**

Use STAR+L, with the Action showing method rather than winning:
- **Situation:** "two viable options for [X]."
- **Action:** "we agreed on the criteria first, I proposed a two-day spike on real data, and the results favored option B on cost but A on operability."
- **Result:** "we chose B with a revisit trigger, and the trigger never fired."
- **Learning:** "agree on criteria before debating options."

Show respect for the other side's position, and show that you'd commit even if you lost.

**Q: How do you say no to a stakeholder?**

I say no to the solution and yes to the need. I ask which decision the data drives and what staleness actually costs. Then I present two or three options with freshness, cost, and risk, and recommend one. For example: "Hourly gets you 95% of the value at a third of the cost of streaming; if the hourly numbers show we're missing fraud, we escalate to streaming." The outcome goes in writing.

**Q: How would you split responsibilities with a co-lead?**

One of us owns architecture and technical direction, the other owns delivery and people, documented in a RACI per decision type. We hold a weekly sync, present one voice, resolve disagreements privately with an agreed tie-break, and review the split quarterly. We name the overlaps (hiring, incident command, roadmap) explicitly.

**Q: How do you prioritize tech debt vs features?**

I translate debt into risk, cost, and velocity metrics, so it's ranked in the same backlog as features. I keep a standing 15–20% capacity budget plus a fix-on-touch norm, and I leave debt alone in stable areas nobody touches. Debt that blocks a roadmap item gets bundled into that item's estimate.

**Q: How do you set standards without becoming a bottleneck?**

Paved roads plus automation (templates, CI checks, policy-as-code) make the standard the default. Human review is reserved for one-way doors, and ADRs let people self-serve the reasoning. A review queue forming behind me means I'm gatekeeping and need to automate or delegate.

**Q: What would you do in your first 90 days?**

- **Days 1–30, listen:** meet stakeholders and the team, map systems, costs, incidents, and pain points. Don't change anything structural.
- **Days 31–60, small wins:** fix one visible pain point, write the first ADRs for decisions already made implicitly, and agree on the co-lead split.
- **Days 61–90, direction:** a written platform strategy with a prioritized roadmap, standards, and metrics, reviewed with the team and stakeholders.

**Q: Can you tell me about a time you were wrong?**

Pick a real, consequential mistake, not a disguised strength. Keep the Situation short, and put the weight on how you detected it and how fast you owned and fixed it. The Learning is a changed habit, for example "I now benchmark on production-shaped data before committing."

**Q: How do you grow engineers on the team?**

Delegate outcomes with clear SLAs and step back as people show ownership. Pair on design docs instead of rewriting them, and rotate on-call and incident command. Success is people owning areas I used to own.

## Takeaways
- The co-lead shift is from personal output to team throughput, quality, and growth.
- Written decisions (ADRs) scale your judgment; paved roads scale your standards.
- Classify each decision as a one-way or two-way door, and spend deliberation on the one-way doors.
- Agree on criteria before debating options, then disagree and commit with a revisit trigger.
- Say no to the ask, yes to the need: options with freshness, cost, and risk (P7).
- Co-leads need a written split, one voice, and a tie-break rule.
- Tech debt competes on metrics with a standing budget; don't pay debt in code nobody touches.
- Prepare 8 STAR+L stories with a clear "I," a tradeoff, a number, and an honest learning.

## Sources
- No external sources were consulted for this edition. The concepts are standard industry practice: architecture decision records (introduced in a widely cited 2011 blog post, "Documenting Architecture Decisions"), the *Team Topologies* book (2019), and the one-way/two-way door and disagree-and-commit framings from published shareholder letters and leadership principles of a large technology company.
