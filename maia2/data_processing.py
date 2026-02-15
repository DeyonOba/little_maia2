# !/usr/bin/env python3
import io
import chess.pgn
from maia2.data_ingestion import download_lichess_database_buffered, get_lichess_database_metadata
from maia2.utils import setup_data_directory
import pyzstd
import tqdm


def preprocess_pgn_game(game: chess.pgn.Game, output_file: io.FileIO, ratings_file: io.FileIO, analysis_mode: bool = True) -> None:
    if game is None:
        return
    event = game.headers.get("Event", "Unknown Event")
    white_elo = game.headers.get("WhiteElo", "Unknown Player ELO")
    black_elo = game.headers.get("BlackElo", "Unknown Player ELO")
    result = game.headers.get("Result", "Unknown Result")

    try:
        if "blitz" in event.lower() and ("?" not in white_elo and "?" not in black_elo):
            white_elo, black_elo = int(white_elo), int(black_elo)
            if analysis_mode:
                ratings_file.write(f"{white_elo}\n{black_elo}\n")
            if white_elo <= 1200 and black_elo <= 1200:
                if output_file.tell() == 0:
                    output_file.write(game.accept(chess.pgn.StringExporter(headers=True, variations=False, comments=False)))
                else:
                    output_file.write("\n\n" + game.accept(chess.pgn.StringExporter(headers=True, variations=False, comments=False)))

    except ValueError as e:
        print(f"ValueError while processing game with Event: {event}, WhiteElo: {white_elo}, BlackElo: {black_elo}, Result: {result}. Error: {e}")
    except Exception as e:
        print(f"Unexpected error while processing game with Event: {event}, WhiteElo: {white_elo}, BlackElo: {black_elo}, Result: {result}. Error: {e}")


def process_lichess_pgn_stream(year: int, month: int):
    download_games = download_lichess_database_buffered(year, month)
    expected_size = get_lichess_database_metadata(year, month).get("content_length", 0)
    dp = pyzstd.EndlessZstdDecompressor()
    previous_buffer = io.StringIO()
    previous_game: chess.pgn.Game = None
    line_ref, previous_line_ref = 0, 0

    data_dir = setup_data_directory()
    processed_data = data_dir / f"lichess_blitz_games_{year}_{month:02d}.pgn"
    ratings_data = data_dir / f"blitz_ratings_{year}_{month:02d}.txt"

    pbar_desc = f"Processing Lichess PGN for {year}-{month:02d}"

    with open(processed_data, "a") as output_file, open(ratings_data, "a") as ratings_file:
        with tqdm.tqdm(total=expected_size, unit='iB', unit_scale=True, desc=pbar_desc) as pbar:
            for chunk, bytes_downloaded in download_games:
                if dp.needs_input:
                    if not chunk:
                        if not dp.at_frame_edge:
                            raise Exception('data ends in an incomplete frame.')
                        break
                else:
                    chunk = b''
                bpgn: bytes = dp.decompress(chunk)
                current_buffer = io.StringIO(bpgn.decode('utf-8'))

                if previous_buffer.getvalue():
                    combined_data = previous_buffer.getvalue() + current_buffer.getvalue()
                    current_buffer = io.StringIO(combined_data)
                    previous_buffer = io.StringIO()

                while True:
                    line_ref = current_buffer.tell()
                    game = chess.pgn.read_game(current_buffer)

                    if game is None:
                        current_buffer.seek(previous_line_ref)
                        remaining_data = current_buffer.read()
                        previous_buffer = io.StringIO(remaining_data)
                        preprocess_pgn_game(previous_game, output_file, ratings_file)
                        break

                    if previous_game is not None:
                        preprocess_pgn_game(previous_game, output_file, ratings_file)
            
                    previous_line_ref, previous_game = line_ref, game
                pbar.update(bytes_downloaded)
