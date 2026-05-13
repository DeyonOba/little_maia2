# Architecture notes

Implementation-level explanations of the model and data pipeline, drawn from the [Maia-2 paper](https://arxiv.org/html/2409.20553v2) and cross-referenced with `little_maia2` code. Companion to `CONTEXT.md`, which is the glossary; this file is the *how it works*.

If a fact in this file conflicts with code, the code wins — but flag the drift, because it usually means a divergence from the paper that nobody wrote down.

## Position encoding — the 18 input channels

`utils.py::board_to_tensor` produces a `(18, 8, 8)` tensor (matching `config.yaml::input_channels: 18`). The channels decompose as:

| Channels | Content |
|---|---|
| 1–12 | Piece planes: one per `(piece_type, colour)` pair — Pawn, Knight, Bishop, Rook, Queen, King × {White, Black}. A `1.0` at `(rank, file)` marks that piece's presence. |
| 13 | Side-to-move plane: all-1s if White to move, all-0s if Black. *After **Active-perspective** mirroring this is effectively always all-1s in training; it survives as a plane because inference can feed un-mirrored FENs if someone bypasses the wrapper.* |
| 14–17 | Castling-rights planes: one per `(side, colour)` — White K-side, White Q-side, Black K-side, Black Q-side. Each plane is fully `1.0` if that castling right is available, else all-zero. |
| 18 | En-passant target square: `1.0` on the target square if an EP capture is possible, else all-zero. |

Anyone changing `board_to_tensor` or `input_channels` must keep this contract consistent across training (`main.py::process_per_game`) and inference (`inference.py::preprocessing`).

## Two-tower architecture: CNN + ViT

The model has two stages.

**Position tower (CNN).** A ResNet of depth `num_blocks_cnn: 5` and width `dim_cnn: 256` operating on the `(18, 8, 8)` input. Its output is reshaped to `(vit_length: 8) × 8 × 8` — i.e. an 8-channel `8×8` feature map.

**Channel-wise patching bridge.** Each of the 8 output channels is flattened to a length-64 vector and linearly projected to `dim_vit: 1024`. The result is an 8-token sequence (one token per channel, not per spatial location). This is the *non-obvious* part: tokens correspond to *feature channels*, not to board squares.

**Skill-aware ViT.** `num_blocks_vit: 2` transformer blocks process the 8-token sequence. Each block's attention queries are augmented by the concatenated `(Active rating-band, Opponent rating-band)` embeddings (`elo_dim: 128` each → `256` concatenated → projected to query dim). See `CONTEXT.md::Skill-aware attention`.

**Three heads.** From the ViT output:
- **Move logits** → `Move probability` (CrossEntropy loss with `criterion_maia`).
- **Side info logits** → `Side info` (BCEWithLogits with `criterion_side_info`, weighted by `side_info_coefficient`).
- **Value logit** → `Win probability` (MSE with `criterion_value` against `Active win` ∈ {+1, -1}; this fork drops draws — see *Deliberate divergences* in `CONTEXT.md`).

## Lineage: Maia-1 vs Maia-2

- **Maia-1** was nine separate networks, one per rating bucket. Inference required loading the appropriate network for the target rating.
- **Maia-2** is a single unified network conditioned on rating-band embeddings (see *Skill-aware attention*). The entire purpose of `elo_dim`, `create_elo_dict`, `map_to_category`, and the rating-band machinery is to replace the nine-network ensemble with one model.

This means: do not add a per-rating model loader. The architecture's whole point is that one model serves all bands.

## Paper evaluation metrics

If a future eval harness lands (see `docs/agents/ai-readiness-backlog.md` item 6), it should target the three metrics the paper uses:

1. **Top-1 move-prediction accuracy** — `argmax(Move probability) == target Move`, averaged over a held-out test set, optionally bucketed by `Rating band`.
2. **Per-move perplexity (bits)** — `-log2(Move probability[target Move])`, averaged. Lower is better.
3. **Monotonic-coherence** — the percentage of `Positions` where, sweeping the `Active Player`'s `Rating band` from lowest to highest at fixed `Position` and `Opponent` band, the predicted `Move probability` of the *actual* move played by the target rating rises monotonically. Measures whether the model has learned a coherent skill axis, not just calibration to the training distribution.

These are the comparison axis to the published Maia-2 numbers if you want to claim parity or improvement on the lower-rated cohort.
