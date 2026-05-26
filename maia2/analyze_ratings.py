"""Recommend ``max_games_per_elo_range`` from ratings_distribution CSVs.

Simulates the per-chunk, per-(elo-pair) counting that
``maia2.main.process_per_chunk`` performs, then reports a recommended cap at a
configurable across-pair percentile. The cap is the integer value to copy into
``maia2_models/config.yaml`` (``max_games_per_elo_range``); this script never
writes it back automatically.

Run as ``python -m maia2.analyze_ratings``.
"""
from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path
from typing import Iterator

import numpy as np
import pandas as pd

from maia2.utils import (
    create_elo_dict,
    map_to_category,
    parse_cfg,
    setup_project_directories,
)


PairKey = tuple[int, int]


def iter_chunk_ranges(n_entries: int, chunk_size: int) -> Iterator[tuple[int, int]]:
    if n_entries <= 0 or chunk_size <= 0:
        return
    start = 0
    while start < n_entries:
        yield start, min(start + chunk_size, n_entries)
        start += chunk_size


def sorted_pair(white_elo: int, black_elo: int, elo_dict: dict) -> PairKey:
    w = map_to_category(int(white_elo), elo_dict)
    b = map_to_category(int(black_elo), elo_dict)
    return (w, b) if w >= b else (b, w)


def count_pairs_in_chunk(
    df_chunk: pd.DataFrame, elo_dict: dict, lo: int, hi: int
) -> dict[PairKey, int]:
    if df_chunk.empty:
        return {}
    df = df_chunk.dropna(subset=["WhiteElo", "BlackElo"]).copy()
    df["WhiteElo"] = df["WhiteElo"].astype(int)
    df["BlackElo"] = df["BlackElo"].astype(int)
    mask = (
        (df["WhiteElo"] >= lo)
        & (df["WhiteElo"] <= hi)
        & (df["BlackElo"] >= lo)
        & (df["BlackElo"] <= hi)
    )
    df = df[mask]
    if df.empty:
        return {}
    pairs = df.apply(
        lambda row: sorted_pair(row["WhiteElo"], row["BlackElo"], elo_dict), axis=1
    )
    return pairs.value_counts().to_dict()


def collect_pair_counts(
    csv_paths: list[Path], chunk_size: int, elo_dict: dict, lo: int, hi: int
) -> dict[PairKey, list[int]]:
    pair_counts: dict[PairKey, list[int]] = defaultdict(list)
    for csv_path in csv_paths:
        try:
            df = pd.read_csv(csv_path)
        except pd.errors.EmptyDataError:
            continue
        if df.empty:
            continue
        for start, stop in iter_chunk_ranges(len(df), chunk_size):
            chunk_counts = count_pairs_in_chunk(df.iloc[start:stop], elo_dict, lo, hi)
            for pair, count in chunk_counts.items():
                pair_counts[pair].append(int(count))
    return dict(pair_counts)


def per_pair_stats(counts: list[int]) -> dict[str, float]:
    arr = np.asarray(counts, dtype=float)
    return {
        "n_chunks": int(arr.size),
        "min": int(arr.min()) if arr.size else 0,
        "p25": float(np.percentile(arr, 25)) if arr.size else 0.0,
        "median": float(np.percentile(arr, 50)) if arr.size else 0.0,
        "p75": float(np.percentile(arr, 75)) if arr.size else 0.0,
        "max": int(arr.max()) if arr.size else 0,
    }


def recommend_cap(
    pair_counts: dict[PairKey, list[int]], percentile: float
) -> tuple[int, dict]:
    medians = [np.percentile(c, 50) for c in pair_counts.values() if c]
    if not medians:
        return 0, {
            "reason": "no chunks contained any binned pairs",
            "n_pairs_seen": 0,
        }
    cap = int(np.floor(np.percentile(medians, percentile)))
    # min-non-empty-pair-count-per-chunk: smallest count seen for any pair, any chunk
    min_nonempty = min(min(c) for c in pair_counts.values() if c)
    diagnostics = {
        "percentile": percentile,
        "n_pairs_seen": len(medians),
        "across_pair_median_of_medians": float(np.percentile(medians, 50)),
        "across_pair_min_of_medians": float(min(medians)),
        "across_pair_max_of_medians": float(max(medians)),
        "global_min_nonempty_chunk_count": int(min_nonempty),
    }
    return cap, diagnostics


def _label_for_index(idx: int, idx_to_label: dict[int, str]) -> str:
    return idx_to_label.get(idx, f"?{idx}")


def format_table(
    pair_counts: dict[PairKey, list[int]], elo_dict: dict
) -> str:
    idx_to_label = {v: k for k, v in elo_dict.items()}
    n_bins = len(elo_dict)
    rows = []
    for r1 in range(n_bins):
        for r2 in range(r1 + 1):  # r1 >= r2
            counts = pair_counts.get((r1, r2), [])
            stats = per_pair_stats(counts)
            label = f"{_label_for_index(r1, idx_to_label):>10} / {_label_for_index(r2, idx_to_label):<10}"
            rows.append((stats["median"], label, stats))
    rows.sort(key=lambda row: row[0], reverse=True)

    header = (
        f"{'pair (high / low)':<25}  "
        f"{'n_chunks':>9}  {'min':>6}  {'p25':>7}  {'median':>8}  "
        f"{'p75':>7}  {'max':>6}"
    )
    lines = [header, "-" * len(header)]
    for _, label, stats in rows:
        lines.append(
            f"{label:<25}  "
            f"{stats['n_chunks']:>9}  {stats['min']:>6}  "
            f"{stats['p25']:>7.1f}  {stats['median']:>8.1f}  "
            f"{stats['p75']:>7.1f}  {stats['max']:>6}"
        )
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m maia2.analyze_ratings",
        description=(
            "Recommend max_games_per_elo_range by simulating per-chunk per-pair "
            "game counts over ratings_distribution CSVs. Tail chunks shorter than "
            "--chunk-size are treated as full chunks (matching process_per_chunk)."
        ),
    )
    parser.add_argument("--chunk-size", type=int, default=None,
                        help="Games per chunk (default: cfg.chunk_size)")
    parser.add_argument("--percentile", type=float, default=25.0,
                        help="Across-pair percentile for the recommendation (default: 25)")
    parser.add_argument("--config", type=Path,
                        default=Path("maia2_models") / "config.yaml",
                        help="Path to config.yaml")
    parser.add_argument("--ratings-dir", type=Path, default=None,
                        help="Override ratings_distribution directory")
    parser.add_argument("--run-test", action="store_true",
                        help="Use test_data project layout")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    cfg = parse_cfg(args.config)
    paths = setup_project_directories(run_test=args.run_test)

    ratings_dir = args.ratings_dir or paths["ratings_data"]
    chunk_size = args.chunk_size if args.chunk_size is not None else int(cfg.chunk_size)
    lo, hi = int(cfg.elo_lower_bound), int(cfg.elo_upper_bound)

    csv_paths = sorted(Path(ratings_dir).glob("ratings_*.csv"))
    if not csv_paths:
        print(f"No ratings_*.csv files found in {ratings_dir}", file=sys.stderr)
        return 2

    elo_dict = create_elo_dict()
    pair_counts = collect_pair_counts(csv_paths, chunk_size, elo_dict, lo, hi)

    print(f"Scanned {len(csv_paths)} CSV(s) in {ratings_dir}")
    print(f"chunk_size={chunk_size}  elo_bounds=[{lo}, {hi}]  percentile={args.percentile}")
    print()
    print(format_table(pair_counts, elo_dict))
    print()

    cap, diagnostics = recommend_cap(pair_counts, args.percentile)
    print("Diagnostics:")
    for k, v in diagnostics.items():
        print(f"  {k}: {v}")
    print()
    print(f"Recommended max_games_per_elo_range: {cap}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
