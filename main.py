"""maia2 pipeline entrypoint.

Subcommands:
    download         Download monthly Lichess PGNs into maia2_data/raw/.
    train            Train the model. Errors out if any expected raw .zst is missing.
    analyze-ratings  Recommend max_games_per_elo_range from ratings_distribution CSVs.
    ui               Launch the MLflow tracking UI against cfg.tracking_uri.

Example:
    python main.py download
    python main.py train
    python main.py ui --port 5050
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from maia2.data_ingestion import download_games
from maia2.train import run as train_run
from maia2.utils import parse_cfg, setup_project_directories

DEFAULT_CONFIG = Path("maia2_models") / "config.yaml"


def _expected_raw_files(cfg, paths) -> list[Path]:
    files: list[Path] = []
    for year in range(cfg.start_year, cfg.end_year + 1):
        start_month = cfg.start_month if year == cfg.start_year else 1
        end_month = cfg.end_month if year == cfg.end_year else 12
        for month in range(start_month, end_month + 1):
            if year == 2019 and month == 12:
                continue
            files.append(paths["raw_data"] / f"blitz_games_{year}_{month:02d}.pgn.zst")
    return files


def _missing_raw_files(cfg, paths) -> list[Path]:
    return [p for p in _expected_raw_files(cfg, paths) if not p.exists()]


def cmd_download(cfg, args) -> int:
    download_games(cfg, run_test=args.run_test)
    return 0


def cmd_train(cfg, args) -> int:
    paths = setup_project_directories(run_test=args.run_test)
    missing = _missing_raw_files(cfg, paths)
    if missing:
        print(
            f"error: {len(missing)} raw file(s) missing under {paths['raw_data']}.\n"
            f"  first missing: {missing[0]}\n"
            f"  run `python main.py download` first.",
            file=sys.stderr,
        )
        return 2
    train_run(cfg)
    return 0


def cmd_analyze_ratings(cfg, args) -> int:
    from maia2.analyze_ratings import main as analyze_main
    forwarded = [
        "--percentile", str(args.percentile),
        "--config", str(args.config),
    ]
    if args.chunk_size is not None:
        forwarded += ["--chunk-size", str(args.chunk_size)]
    if args.run_test:
        forwarded.append("--run-test")
    return analyze_main(forwarded)


def cmd_ui(cfg, args) -> int:
    cmd = [
        sys.executable, "-m", "mlflow", "ui",
        "--backend-store-uri", getattr(cfg, "tracking_uri", "./mlruns"),
        "--host", args.host,
        "--port", str(args.port),
    ]
    print("Launching:", " ".join(cmd), flush=True)
    return subprocess.call(cmd)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="main.py", description="maia2 pipeline entrypoint")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="Path to config.yaml")
    parser.add_argument("--run-test", action="store_true", help="Use test_data project layout")

    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("download", help="Download monthly Lichess PGNs into maia2_data/raw/")

    sub.add_parser("train", help="Train; errors if any expected raw .zst is missing")

    p_an = sub.add_parser("analyze-ratings", help="Recommend max_games_per_elo_range from ratings CSVs")
    p_an.add_argument("--chunk-size", type=int, default=None)
    p_an.add_argument("--percentile", type=float, default=25.0)

    p_ui = sub.add_parser("ui", help="Launch the MLflow tracking UI")
    p_ui.add_argument("--port", type=int, default=5000)
    p_ui.add_argument("--host", default="127.0.0.1")

    return parser


DISPATCH = {
    "download": cmd_download,
    "train": cmd_train,
    "analyze-ratings": cmd_analyze_ratings,
    "ui": cmd_ui,
}


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    cfg = parse_cfg(args.config)
    rc = DISPATCH[args.command](cfg, args)
    return 0 if rc is None else int(rc)


if __name__ == "__main__":
    sys.exit(main())
