# AI-readiness backlog

A ranked list of infrastructure work that *enlarges* what `ready-for-agent` can safely cover. Each item should be turnable into its own `ready-for-agent` ticket when prioritised.

This file is the answer to: *"why don't we have X yet? when will we?"* — explicitly **deferred**, not forgotten.

## Why this file exists

When we wrote `docs/agents/ready-for-agent.md`, we agreed the acceptance criterion for an AFK ticket is *"locally verifiable"*. Today, this repo has **no test suite, no linter, no type-checker, no pre-commit hooks, and no CI**. So "locally verifiable" currently rests entirely on the reviewer running the verification recipe spelled out in the ticket. That is a deliberate, soft starting point — see ADR-0001 if one is created when the trade-off is revisited.

Each item below is a future ratchet that hardens that gate.

## Backlog (top of list = do first)

### 1. Pytest harness + seed tests for pure-function code
- **What**: add `pytest` as a dev dep; create `tests/`; write tests for `maia2/utils.py` pure helpers (`generate_promotion_moves`, `get_all_possible_moves`, move-encoding helpers).
- **Why it unlocks more AFK**: gives every future ticket a concrete acceptance command (`uv run pytest -k <name>`) instead of relying on reviewer eyeballs.
- **Done when**: `uv run pytest` is green on a fresh clone and at least one ticket has used it as its acceptance check.

### 2. Linter (ruff)
- **What**: add `ruff` with a minimal config; run on `maia2/`.
- **Why it unlocks more AFK**: catches the cheap class of agent slip-ups (unused imports, undefined names, basic style drift) without reviewer time.
- **Done when**: `uv run ruff check` is green and the agent contract requires it.

### 3. Pre-commit hooks
- **What**: `.pre-commit-config.yaml` running ruff and a basic safety net (trailing whitespace, large-file guard, secret scan).
- **Why it unlocks more AFK**: the agent self-validates before opening the PR. Reviewer time drops further.
- **Done when**: `pre-commit run --all-files` is green and the agent runs it before pushing.

### 4. GitHub Actions CI
- **What**: workflow that runs `uv sync`, `uv run pytest`, `uv run ruff check` on every PR to `main`.
- **Why it unlocks more AFK**: the merge gate is enforced by the platform, not by reviewer discipline. Enables future auto-merge for trivial PRs.
- **Done when**: a failing PR is blocked by CI without human intervention.

### 5. Type checking (mypy or pyright)
- **What**: type-checker configured for `maia2/`, starting permissive and tightening.
- **Why it unlocks more AFK**: agents drift on types more than humans do; static checking catches it cheaply.
- **Done when**: type-checker is green and is part of the agent contract.

### 6. Eval / smoke-train harness
- **What**: a cheap, deterministic check that exercises a tiny training step or inference call and reports the paper's three metrics on a small fixed test set:
  - **Top-1 move-prediction accuracy** (overall and per **Rating band**).
  - **Per-move perplexity** in bits.
  - **Monotonic-coherence** — % of **Positions** where `P(actual move at target rating)` rises monotonically as the **Active Player**'s **Rating band** sweeps low → high (fixed **Position** and **Opponent** band).
  See `docs/architecture-notes.md` for the precise definitions.
- **Why it unlocks more AFK**: this is what would let us *broaden* the `ready-for-agent` scope beyond "locally verifiable without training" to include changes that touch `train.py` and `model.py`. Until this exists, those files stay `ready-for-human`.
- **Done when**: a single command runs in under a minute on a small fixed test set, prints the three metrics, and reliably catches a known-bad model/training change (regression test for at least one of the three metrics).

## How to consume this list

When picking the next infra ticket, take the top item. Don't skip ahead — each one assumes the previous is in place (e.g., CI assumes pytest; pre-commit assumes ruff).

When a new AI-readiness gap is discovered mid-session, append to the bottom with the same three lines (*What / Why it unlocks more AFK / Done when*).
