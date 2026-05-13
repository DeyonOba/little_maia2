# little_maia2

A lightweight chess move-prediction model focused on lower-rated Lichess players. This glossary fixes the project's ubiquitous language so prose, code, and configuration cannot drift apart.

## Language

### People and scope

**Player**:
A human Lichess account that played a **Game** ingested for training, or whose **Rating** is supplied at inference time. *Never refers to the model itself or the inference caller.*

**Time control**:
The Lichess time-control category of a **Game**. *This project is currently Blitz-only*: `main.py::game_filter` keeps only **Games** whose PGN `event` header contains the substring `"Blitz"`. Rapid / Bullet / Classical are filtered out at ingestion; `from_pretrained` accepts `"rapid"` as a parameter but raises `NotImplementedError`. Broadening scope requires coordinated changes in `game_filter`, `from_pretrained`, and the data file-naming convention.

### Units of prediction

**Position**:
A chess board state at a specific moment in a game, serialised as a FEN string (`fen`), materialised as a `chess.Board` object when needed, or stored as a column named `board` in DataFrames. *Position is the unit of model input.*
_Avoid_: just "board" in prose — the DataFrame column `board` is a known misnomer (it holds a FEN, not a `chess.Board`).

**Move**:
A single half-move in UCI notation (e.g. `e2e4`, `e7e8q`). *Move is the model's target and the unit of its output distribution.*
_Avoid_: drift between `move` and `move_uci`; they are synonyms in code.

**Game**:
A complete Lichess session of **Moves** between two **Players**, ingested as a PGN record. *Game is the unit of data ingestion, not the unit of prediction.* Sampling expands one **Game** into many `(Position, Move, …)` training rows — the model is position-level, not sequence-aware.

**Ply**:
A half-move (one **Player**'s **Move**). `move_ply` indexes a **Position** within a **Game** (e.g. the dataset filter `move_ply > 10` skips the first 10 plies). `max_ply: 300` caps **Game** length. *Plies, not full moves; one full move = two plies.* The paper skips the first 10 plies to exclude the opening-theory phase, where moves reflect memorised lines rather than player-specific behaviour.

### Perspective and outcome

**Active Player**:
The **Player** whose turn it is in a given **Position**.
_Avoid_: "side-to-move" in prose. In code: `self`, `active`, or implied by `elo_self`.

**Opponent**:
The other **Player** in the same **Position**.
In code: `oppo`, `opponent`, or `elo_oppo`.

**Active perspective**:
The project's canonical reference frame. Every **Position** the model sees has the **Active Player** to move with their pieces on the bottom (mirrored from a black-to-move source FEN), and the target **Move** is mirrored to match. *Load-bearing invariant*: any code that consumes or emits **Positions** must respect it. Implemented by `chess.Board.mirror()` + `utils.py::mirror_move` in `main.py::process_per_game` (training) and `inference.py::preprocessing` (inference).

**Active win**:
The game outcome relabelled to the **Active Player**'s viewpoint: `+1` if the Active Player won, `-1` if lost. *Draws are not represented in training data* — the outcome model is binary, not ternary.

### Player strength

**Rating**:
A player's numeric strength as reported by Lichess (Glicko-2 under the hood, treated as a generic rating throughout this project for parity with chess conventions).
_Avoid_: Elo (in prose). The code uses the token `elo` as a synonym; treat them as interchangeable in code, but prefer *Rating* in prose.

**Training cohort**:
The closed Lichess **Rating** range from which games are ingested for training (currently `[400, 1200]`, controlled by `config.yaml::elo_lower_bound` / `elo_upper_bound`). Replaces vague statements like *"the project targets lower-rated players"*.
_Avoid_: target audience, Elo range, rating range (ambiguous with **Rating band**).

**Rating band**:
One of the 8 discrete buckets that a **Rating** is mapped to before being fed to the model as an input feature: `<500`, `500-599`, `600-699`, `700-799`, `800-899`, `900-999`, `1000-1099`, `>=1100`. Defined by `utils.py::create_elo_dict` and `map_to_category`. **Independent** of the **Training cohort**: a **Rating** outside the cohort (e.g. 1500 at inference time) still maps to a band (`>=1100`). Bands exist because the paper uses *categorical* rating embeddings (to capture non-linear strength→move relationships); raw rating integers are never fed to the model — they always go through `map_to_category` first.
_Avoid_: Elo category, Elo bucket, rating bucket — all used loosely in code and commit messages, but *Rating band* is canonical.

### Model outputs

**Move probability**:
A probability over **Moves** legal in a given **Position**, returned by inference as a `{uci: prob}` dict. *Keys are in original-board UCI, not Active-perspective UCI* — `mirror_move` is applied on the way out to undo the input-side normalisation. Computed from `logits_maia`, masked by legal moves, then softmaxed.

**Win probability**:
A scalar in `[0, 1]` representing **White's** probability of winning from the given **Position**. *Not the **Active Player**'s win probability* — a `1 - p` flip is applied at inference when the source FEN was black-to-move. Internally trained against **Active win** ∈ `{+1, −1}` via MSE, then rescaled to `[0, 1]` at inference via `(x/2 + 0.5).clamp(0, 1)`.

**Side info**:
Auxiliary multi-label supervision used during training only: features of the target **Move** (moving piece type, captured piece type, whether the move gives check) plus from-square, to-square, and legal-moves masks. *Discarded at inference.* Tuned by `config.yaml::side_info_coefficient`. Purpose: shape the model's representation, not predict at serving time. Castling is special-cased (`e1g1`/`e1c1` also flip the rook's from/to-square bits).

### Model conditioning

**Skill-aware attention**:
The mechanism by which **Rating bands** condition the model. The **Active Player**'s and **Opponent**'s rating-band embeddings (each of size `elo_dim: 128`) are concatenated, then *added to the attention queries* inside each ViT block — not concatenated at the CNN input and not appended at a final MLP. This is why `elo_dim` and `dim_vit` are sized to interoperate. *Implication*: if you change `elo_dim`, you must respect the dimensional contract with the attention queries.

### Game eligibility

A **Game** is silently dropped from the training set unless *all* of the following hold (see `main.py::game_filter` and `process_per_game`):

- `event` header contains `"Rated"` — casual games excluded.
- `event` header contains `"Blitz"` — Rapid, Bullet, Classical excluded. *The current model is blitz-only.*
- Result is decisive (`1-0` or `0-1`) — *draws excluded entirely*.
- Both **Players'** **Ratings** lie within the **Training cohort**.

These filters are silent: a Game that fails any check is dropped without logging. If broadening scope (e.g. adding Rapid), expect to revisit `game_filter`, `from_pretrained(time_control_format=…)`, and the data-ingestion file-naming convention together.

## Relationships

- A **Player** has one **Rating** at a given point in time.
- A **Rating** maps to exactly one **Rating band** via `map_to_category`.
- The **Training cohort** is a filter over **Ratings**, not over **Rating bands**: a **Game** enters the cohort iff both players' **Ratings** lie in `[elo_lower_bound, elo_upper_bound]`.
- The **Training cohort** and the **Rating band** scheme can be tuned independently. Narrowing the cohort does not narrow the band scheme.
- A **Game** decomposes into a sequence of **Plies**; each **Ply** yields one **(Position, Move)** training pair (subject to filters like `move_ply > 10`).
- Every **Position** and every **Move** stored in the training set is already in **Active perspective**; raw FENs from PGN files are *not*.
- The model maps `(Position, Active Player's Rating band, Opponent's Rating band)` → (**Move** logits, **Side info** logits, **Active win** logit).
- Inference exposes only the first and third as **Move probability** and **Win probability**; **Side info** is training-only.

## Example dialogue

> **Dev:** "The README says we target players ≤1200. The bands go up to `>=1100`. Which is right?"
> **Maintainer:** "Both, they're different things. The **Training cohort** is `[400, 1200]` — that's *which games we train on*. The **Rating band** scheme has an overflow bucket at `>=1100` so the model can still produce a band for *any* **Rating** at inference time, even outside the cohort. The README is just imprecise prose."

> **Dev:** "If I pass a black-to-move FEN to the model, does it just figure out it's black to move from the FEN string?"
> **Maintainer:** "No — the model never sees a black-to-move position. The FEN is mirrored to **Active perspective** before tensorisation. If you bypass `inference.preprocessing` you'll silently feed garbage."

## Flagged ambiguities

- *"Elo"* in prose meant three different things (cohort upper bound, band scheme, target audience). Resolved: prose uses **Rating** / **Training cohort** / **Rating band**; code's `elo` token left unchanged to avoid a rename storm — a future ticket may align code with prose.
- `data_ingestion.py::fast_filter_pgn_games` has stale default args `from_rating=1500, to_rating=1550` (an upstream-Maia leftover). These are dead defaults — `download_games` always passes `cfg.elo_lower_bound` / `cfg.elo_upper_bound` — but a future caller could trigger the wrong **Training cohort** by omitting args. Tracked for a future `ready-for-agent` ticket.
- The DataFrame column `board` holds a FEN string, not a `chess.Board` object. Code keeps the misleading name for now; prose must say **Position** (or *FEN* if specifically referring to the string form). A future rename is a candidate AFK ticket.
- `config.yaml::first_n_moves` and `last_n_moves` (both `10`) are ambiguous between "plies" and "full moves". The paper's *plies* interpretation (matching the `move_ply > 10` dataset filter and the opening-theory rationale) is by far the more likely reading; still un-traced in this fork. Flagged for confirmation.
- `config.yaml::clock_threshold: 30` is configured but its filter in `process_per_game` is commented out. The paper's intent is unambiguous: exclude **Plies** played with fewer than 30 seconds remaining on the clock, to keep time-pressure decisions out of training. So this is *unfinished work*, not dead config — the filter should be re-enabled (along with `extract_clock_time` plumbing). Tracked for a future `ready-for-agent` ticket.
- `active_elo` / `elo_self` and `opponent_elo` / `elo_oppo` are synonyms across `dataset.py` and `inference.py`. Prose prefers **Active Player's Rating** / **Opponent's Rating**; code keeps both for now.
- "Self" in code (`elo_self`, `active`) means **Active Player**, not the model itself. Renamed in prose to avoid confusion with model-as-self framings.
- **Inputs are Active-perspective-normalised; outputs are de-normalised back to original-board / White perspective.** `inference.preprocessing` mirrors the input **Position**; `get_preds` / `inference_each` mirror the **Move probability** keys back and apply `1 - p` to **Win probability** when the source FEN was black-to-move. Any code that bypasses these wrappers must replicate *both* halves of the transform, or it will silently corrupt outputs.

## Deliberate divergences from the Maia-2 paper

These are *intentional* choices in `little_maia2` that differ from the published Maia-2 design. Future agents tempted to "fix" them should consult the maintainer first — they are not bugs.

- **Draws excluded from training.** Paper uses a ternary value-head target `{+1, 0, -1}`; this fork drops `1/2-1/2` games entirely in `process_per_game` and trains the value head with MSE on `{+1, -1}` (see **Active win**). The rationale is not yet documented anywhere in the repo; flagged for a future `ready-for-agent` ticket to either record the rationale or revert to the paper's design.
- **Cohort narrowed to lower-rated players.** The paper trains across the full Lichess rating spectrum; this fork's **Training cohort** is `[400, 1200]` and **Rating bands** are re-scoped to match. This is the *project's reason for being* (see README) and is *not* a divergence to revert.
- **Blitz only.** Paper supports multiple **Time control** categories via separate models; this fork is currently Blitz-only by `game_filter` and by `from_pretrained`. Tracked as a scope decision, not a defect.
