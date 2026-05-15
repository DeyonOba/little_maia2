# Open design questions

Workflow design decisions that have been deliberately deferred during a grilling session. Each entry should name the question, the candidate options considered, and (if any) the leading recommendation — so a future session can resume from where we paused, not from scratch.

## Deferred

### Ticket contract for `ready-for-agent` issues
- **Question**: what is the minimum set of fields a `ready-for-agent` issue must contain to be safely AFK-executable?
- **Why it matters**: with no automated test/lint/CI gate, the ticket + the reviewer-run verification recipe are the *only* acceptance gate. A fuzzy ticket means an expensive AFK round-trip.
- **Options considered**:
  - (A) Free-form
  - (B) 5-field template — `Goal / Acceptance / In-scope paths / Out-of-scope paths / Verification recipe`  ← *leading recommendation*
  - (C) Heavier template with risk, rollback, dependencies, related ADRs
  - (D) GitHub issue forms (YAML-enforced)
- **Status**: deferred during the 2026-05-13 grilling session; user chose to write the domain glossary first.

### Universal guardrails (forbidden actions regardless of ticket)
- **Question**: what is the agent *never* allowed to do, even on a `ready-for-agent` ticket?
- **Candidates to consider**: pushing to `main`, kicking off downloads, `uv add` of new deps not named in the ticket, modifying `train.py`/`model.py`/`config.yaml` semantics, deleting cached data under `data/`.
- **Status**: not yet asked.

### Review/merge loop SLA
- **Question**: who reviews `ready-for-agent` PRs, how quickly, and what happens to stale branches?
- **Status**: not yet asked.
