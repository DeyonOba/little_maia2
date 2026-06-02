"""Recommend ``max_games_per_elo_range`` from ratings_distribution CSVs.

Simulates the per-chunk, per-(elo-pair) counting that
``maia2.main.process_per_chunk`` performs and reports a recommended cap for
``maia2_models/config.yaml::max_games_per_elo_range``.

Strategy:
    1. For each monthly ``ratings_YYYY_MM.csv`` produced by
       ``maia2.data_ingestion``, draw one chunk of ``--chunk-size`` rows
       (without replacement, seeded) to mimic a training chunk.
    2. Bin both columns into ELO categories via
       ``maia2.utils.map_to_category`` and count games per sorted
       ``(hi, lo)`` pair — the same key process_per_chunk groups by.
    3. Within each month, take the Xth percentile across pairs.
       Aggregate across months with the median, then round → recommended cap.
       The per-month-then-median form is robust to a single partial month.

The script never writes back to ``config.yaml``; copy the value yourself.

Run as ``python main.py analyze-ratings [--percentile P] [--chunk-size N]``.
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from maia2.utils import (
    create_elo_dict,
    map_to_category,
    parse_cfg,
    setup_project_directories,
)


PairKey = tuple[int, int]
_FILENAME_RE = re.compile(r"ratings_(\d{4})_(\d{2})\.csv")


def _bin_series(elos: pd.Series, elo_dict: dict[str, int]) -> np.ndarray:
    """Vectorized ``map_to_category`` over an int Series."""
    return elos.astype(int).map(lambda e: map_to_category(int(e), elo_dict)).to_numpy()


def count_pairs_in_chunk(
    df_chunk: pd.DataFrame,
    elo_dict: dict[str, int],
    elo_lower_bound: int,
    elo_upper_bound: int,
) -> dict[PairKey, int]:
    """Count games per sorted ``(hi, lo)`` elo-category pair in one chunk."""
    if df_chunk.empty:
        return {}

    df = df_chunk.dropna(subset=["WhiteElo", "BlackElo"])
    if df.empty:
        return {}

    white = df["WhiteElo"].astype(int)
    black = df["BlackElo"].astype(int)
    mask = (
        (white >= elo_lower_bound) & (white <= elo_upper_bound)
        & (black >= elo_lower_bound) & (black <= elo_upper_bound)
    )
    if not mask.any():
        return {}

    white_cat = _bin_series(white[mask], elo_dict)
    black_cat = _bin_series(black[mask], elo_dict)
    hi = np.maximum(white_cat, black_cat)
    lo = np.minimum(white_cat, black_cat)

    pairs = pd.Series(list(zip(hi.tolist(), lo.tolist())))
    return pairs.value_counts().to_dict()


def gen_desc_stats(
    cfg,
    paths: dict[str, Path],
    percentile: float,
    chunk_size: int,
) -> tuple[pd.DataFrame, list[float], int | None]:
    """Return (per-month pair counts, per-month percentiles, recommended cap)."""
    elo_dict = create_elo_dict()
    reverse_elo_dict = {v: k for k, v in elo_dict.items()}

    rows: list[dict] = []
    per_month_percentiles: list[float] = []
    skipped: list[str] = []

    csv_paths = sorted(paths["ratings_data"].glob("ratings_*.csv"))
    for filepath in csv_paths:
        match = _FILENAME_RE.match(filepath.name)
        if not match:
            continue
        year, month = int(match.group(1)), int(match.group(2))

        df = pd.read_csv(filepath)
        if df.shape[0] < chunk_size:
            skipped.append(f"{filepath.name} ({df.shape[0]} rows < chunk_size {chunk_size})")
            continue

        df_chunk = df.sample(chunk_size, random_state=cfg.seed)
        counts = count_pairs_in_chunk(
            df_chunk, elo_dict, cfg.elo_lower_bound, cfg.elo_upper_bound
        )
        if not counts:
            skipped.append(f"{filepath.name} (no in-cohort games)")
            continue

        row = {
            f"{reverse_elo_dict[hi]} / {reverse_elo_dict[lo]}": v
            for (hi, lo), v in counts.items()
        }
        row["year"] = year
        row["month"] = month
        rows.append(row)
        per_month_percentiles.append(float(np.percentile(list(counts.values()), percentile)))

    if skipped:
        print(f"Skipped {len(skipped)} file(s):", file=sys.stderr)
        for line in skipped:
            print(f"  - {line}", file=sys.stderr)

    if not rows:
        return pd.DataFrame(), [], None

    df = pd.DataFrame(rows).set_index(["year", "month"]).fillna(0).astype(int)
    recommended = int(round(float(np.median(per_month_percentiles))))
    return df, per_month_percentiles, recommended


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="analyze-ratings",
        description="Recommend max_games_per_elo_range from ratings_distribution CSVs.",
    )
    parser.add_argument(
        "--percentile",
        type=float,
        default=25.0,
        help="Across-pair percentile (0-100) used per month. Lower = more aggressive balancing.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=None,
        help="Rows sampled per monthly CSV to simulate one training chunk (default: cfg.chunk_size).",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("maia2_models/config.yaml"),
        help="Path to config.yaml.",
    )
    parser.add_argument(
        "--run-test",
        action="store_true",
        help="Use test_data/ project layout instead of maia2_data/.",
    )
    args = parser.parse_args(argv)

    if not 0.0 <= args.percentile <= 100.0:
        print(f"error: --percentile must be in [0, 100], got {args.percentile}", file=sys.stderr)
        return 2

    cfg = parse_cfg(str(args.config))
    paths = setup_project_directories(run_test=args.run_test)
    chunk_size = args.chunk_size if args.chunk_size is not None else cfg.chunk_size

    df, per_month_pcts, recommended = gen_desc_stats(cfg, paths, args.percentile, chunk_size)
    if df.empty:
        print(
            f"error: no usable ratings CSVs in {paths['ratings_data']}.\n"
            f"  expected files like 'ratings_YYYY_MM.csv' with >= {chunk_size} rows.",
            file=sys.stderr,
        )
        return 1

    print(f"\nPer-elo-pair game count per chunk ({len(df)} month(s), chunk_size={chunk_size}):")
    print(df.describe().T.to_string())

    pct_df = pd.DataFrame(
        {f"p{args.percentile:g}_across_pairs": per_month_pcts},
        index=df.index,
    )
    print(f"\nPer-month {args.percentile:g}th percentile across elo pairs:")
    print(pct_df.to_string())

    print(
        f"\nRecommended max_games_per_elo_range: {recommended}\n"
        f"  (median of per-month {args.percentile:g}th-percentile across elo pairs)\n"
        f"  Copy into maia2_models/config.yaml::max_games_per_elo_range "
        f"(currently {getattr(cfg, 'max_games_per_elo_range', '<unset>')}).",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
