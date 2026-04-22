import asyncio
from maia2.data_ingestion import run_pipeline_async, download_games
from maia2.train import run
from maia2.utils import parse_cfg, compress_zst, decompress_zst
from maia2.main import read_monthly_data_filenames
from maia2.model import from_pretrained
import maia2.inference as inference
import chess


if __name__ == "__main__":
    cfg = parse_cfg("./maia2_models/config.yaml")
    # Download all the game for 2013 Janurary to December
    # download_games(cfg)

    # Debug Training pipeline
    # run(cfg)

    # Test out compression and decompression features
    # src = "data/processed/blitz_games_2013_01.pgn"
    # dest = "data/raw/blitz_games_2013_07.pgn.zst"
    # compress_zst(src, dest)
    # decompress_zst(dest, "./a.pgn")

    # Test read monthly data paths
    # pgn_paths = read_monthly_data_filenames(cfg)
    # print(f"{pgn_paths=}")


    # from pathlib import Path
    # Path().name

    # Test run the small model trained
    model = from_pretrained(time_control_format="blitz", device="cpu")

    fen = chess.STARTING_FEN
    elo_self, elo_oppo = 1000, 1000
    prepare = inference.prepare()

    move_probability, winning_probability = inference.inference_each(model, prepare, fen, elo_self, elo_oppo)
    print(f"{winning_probability=}\n{move_probability=}")



