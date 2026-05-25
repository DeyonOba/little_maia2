
import chess
import pyzstd
from pyzstd import EndlessZstdDecompressor, ZstdCompressor
import shutil
import pickle
import torch
from pathlib import Path
import re
import yaml
import time
import os
import numpy as np
import random
import io


class Config:
    def __init__(self, cfg_dict: dict):
        for key, value in cfg_dict.items():
            setattr(self, key, value)


def parse_cfg(cfg_file_path: str):
    with open(cfg_file_path, "r") as file:
        cfg_dict = yaml.safe_load(file)

    cfg = Config(cfg_dict)
    return cfg


def setup_project_directories(run_test: bool = False, verbose: bool = False) -> dict[str, Path]:
    """
    Initialize project directories before code execution or data processing
    """

    root = Path(".")

    data_dir_name = "test_data" if run_test else "maia2_data" 

    directories = {
        "raw_data": root / data_dir_name / "raw",
        "processed_data": root / data_dir_name / "processed",
        "ratings_data": root / data_dir_name / "ratings_distribution",
        "data_checkpoints": root / data_dir_name / "checkpoints",
        "ml_checkpoints": root / "maia2_models" / "checkpoints",
        "logs": root / "logs"
    }

    # Initializing the directories
    for _, path in directories.items():
        path.mkdir(parents=True, exist_ok=True)
        if verbose:
            print(f"Verified: {path.relative_to(root) if root in path.parents else path}", flush=True)

    return directories
    

def seed_everything(seed: int):

    random.seed(seed)
    os.environ['PYTHONHASHSEED'] = str(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    torch.cuda.manual_seed(seed)
    torch.cuda.manual_seed_all(seed)
    torch.backends.cudnn.deterministic = True
    torch.backends.cudnn.benchmark = False


def delete_file(filename):
    
    if os.path.exists(filename):
        os.remove(filename)
        print(f"Data {filename} has been deleted.")
    else:
        print(f"The file '{filename}' does not exist.")


def readable_num(num):
    
    if num >= 1e9:  # if parameters are in the billions
        return f'{num / 1e9:.2f}B'
    elif num >= 1e6:  # if parameters are in the millions
        return f'{num / 1e6:.2f}M'
    elif num >= 1e3:  # if parameters are in the thousands
        return f'{num / 1e3:.2f}K'
    else:
        return str(num)


def readable_time(elapsed_time):

    hours, rem = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(rem, 60)

    if hours > 0:
        return f"{int(hours)}h {int(minutes)}m {seconds:.2f}s"
    elif minutes > 0:
        return f"{int(minutes)}m {seconds:.2f}s"
    else:
        return f"{seconds:.2f}s"


def count_parameters(model):
    
    total_params = sum(p.numel() for p in model.parameters() if p.requires_grad)
    
    return readable_num(total_params)


# def decompress_zst(compressed_file_path: str, decompressed_file_path: str) -> None:
#     with open(compressed_file_path, "rb") as compressed_file, open(decompressed_file_path, "wb") as decompressed_file:
#         # src -> dst: source file-like object, destination file-like object
#         # TOD0: pyzstd.decompress_stream() is now deprecated, need to update to pyzstd.ZstdDecompressor().stream_reader() 
#         # and handle the streaming decompression manually
#         pyzstd.decompress_stream(compressed_file, decompressed_file)


def decompress_zst(src: str, dest: str) -> None:
    with pyzstd.open(src, "rb") as fsrc:
        with io.open(dest, mode="wb") as fdest:
            shutil.copyfileobj(fsrc, fdest)


def compress_zst(src: str, dest: str) -> None:
    with io.open(src, "rb") as fsrc:
        with pyzstd.open(dest, "w", level_or_option=5) as fdest:
            shutil.copyfileobj(fsrc, fdest)


def extract_clock_time(comment: str) -> int:
    pattern = r"\[%clk (\d+):(\d+):(\d+)\]"
    match = re.search(pattern, comment)
    if match:
        hours, minutes, seconds = map(int, match.groups())
        total_seconds = hours * 3600 + minutes * 60 + seconds
        return total_seconds
    return None


def readable_time(elapsed_time: int) -> str:
    hours, rem = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(rem, 60)

    if hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    elif minutes > 0:
        return f"{minutes}m {seconds}s"
    else:
        return f"{seconds}s"


def create_elo_dict():
    interval = 100
    start = 600
    stop = 1100

    range_dict = {f"<{start}": 0}
    range_index = 1

    for lower_bound in range(start, stop, interval):
        upper_bound = lower_bound + interval - 1

        range_dict[f"{lower_bound}-{upper_bound}"] = range_index
        range_index += 1
    
    range_dict[f">={stop}"] = range_index

    return range_dict


def map_to_category(elo: int, elo_dict: dict) -> int:
    start = 600
    stop = 1100
    interval = 100

    if elo < start:
        return elo_dict[f"<{start}"]
    elif elo >= stop:
        return elo_dict[f">={stop}"]
    else:
        lower_bound = start + ((elo - start) // interval) * interval
        upper_bound = lower_bound + interval - 1
        return elo_dict[f"{lower_bound}-{upper_bound}"]


def get_side_info(board, move_uci, all_moves_dict):
    move = chess.Move.from_uci(move_uci)
    
    moving_piece = board.piece_at(move.from_square)
    captured_piece = board.piece_at(move.to_square)

    from_square_encoded = torch.zeros(64)
    from_square_encoded[move.from_square] = 1

    to_square_encoded = torch.zeros(64)
    to_square_encoded[move.to_square] = 1
    
    if move_uci == 'e1g1':
        rook_move = chess.Move.from_uci('h1f1')
        from_square_encoded[rook_move.from_square] = 1
        to_square_encoded[rook_move.to_square] = 1
    
    if move_uci == 'e1c1':
        rook_move = chess.Move.from_uci('a1d1')
        from_square_encoded[rook_move.from_square] = 1
        to_square_encoded[rook_move.to_square] = 1

    board.push(move)
    is_check = board.is_check()
    board.pop()
    
    # Order: Pawn, Knight, Bishop, Rook, Queen, King
    side_info = torch.zeros(6 + 6 + 1)
    side_info[moving_piece.piece_type - 1] = 1
    if move_uci in ['e1g1', 'e1c1']:
        side_info[3] = 1
    if captured_piece:
        side_info[6 + captured_piece.piece_type - 1] = 1
    if is_check:
        side_info[-1] = 1
    
    legal_moves = torch.zeros(len(all_moves_dict))
    legal_moves_idx = torch.tensor([all_moves_dict[move.uci()] for move in board.legal_moves])
    legal_moves[legal_moves_idx] = 1
    
    side_info = torch.cat([side_info, from_square_encoded, to_square_encoded, legal_moves], dim=0)
    
    return legal_moves, side_info


def extract_clock_time(comment):
    
    match = re.search(r'\[%clk (\d+):(\d+):(\d+)\]', comment)
    if match:
        hours, minutes, seconds = map(int, match.groups())
        return hours * 3600 + minutes * 60 + seconds
    return None
    
def mirror_square(square):
    
    file = square[0]
    rank = str(9 - int(square[1]))
    
    return file + rank


def mirror_move(move_uci):
    # Check if the move is a promotion (length of UCI string will be more than 4)
    is_promotion = len(move_uci) > 4

    # Extract the start and end squares, and the promotion piece if applicable
    start_square = move_uci[:2]
    end_square = move_uci[2:4]
    promotion_piece = move_uci[4:] if is_promotion else ""

    # Mirror the start and end squares
    mirrored_start = mirror_square(start_square)
    mirrored_end = mirror_square(end_square)

    # Return the mirrored move, including the promotion piece if applicable
    return mirrored_start + mirrored_end + promotion_piece


def get_chunks(pgn_path, chunk_size):

    chunks = []
    with open(pgn_path, 'r', encoding='utf-8') as pgn_file:
        while True:
            start_pos = pgn_file.tell()
            game_count = 0
            while game_count < chunk_size:
                line = pgn_file.readline()
                if not line:
                    break
                if line[-4:] == "1-0\n" or line[-4:] == "0-1\n":
                    game_count += 1
                if line[-8:] == "1/2-1/2\n":
                    game_count += 1
                if line[-2:] == "*\n":
                    game_count += 1
            line = pgn_file.readline()
            if line not in ["\n", ""]:
                raise ValueError
            end_pos = pgn_file.tell()
            chunks.append((start_pos, end_pos))
            if not line:
                break

    return chunks


def read_or_create_chunks(pgn_path, cfg):

    cache_file = pgn_path.replace('.pgn', '_chunks.pkl')

    if os.path.exists(cache_file):
        print(f"Loading cached chunks from {cache_file}")
        with open(cache_file, 'rb') as f:
            pgn_chunks = pickle.load(f)
    else:
        print(f"Cache not found. Creating chunks for {pgn_path}")
        start_time = time.time()
        pgn_chunks = get_chunks(pgn_path, cfg.chunk_size)
        print(f'Chunking took {readable_time(time.time() - start_time)}', flush=True)
        
        with open(cache_file, 'wb') as f:
            pickle.dump(pgn_chunks, f)
    
    return pgn_chunks


# def generate_promotion_moves():
#     all_pawn_promotion_moves = []
#     white_promotion_rank, black_promotion_rank = 6, 1

#     for file in range(8):
#         board = chess.Board(None)
#         board.set_piece_at(chess.square(file, white_promotion_rank), chess.Piece(chess.PAWN, chess.WHITE))
#         white_promotion_moves = [move.uci() for move in board.legal_moves]
#         all_pawn_promotion_moves.extend(white_promotion_moves)

#         board.clear_board()
#         board.turn = chess.BLACK
#         board.set_piece_at(chess.square(file, black_promotion_rank), chess.Piece(chess.PAWN, chess.BLACK))
#         black_promotion_moves = [move.uci()  for move in board.legal_moves]
#         all_pawn_promotion_moves.extend(black_promotion_moves)

#     return all_pawn_promotion_moves


# def get_all_possible_moves():
#     all_possible_piece_moves = []

#     for rank in range(8):
#         for file in range(8):
#             board = chess.Board(None)
#             square = chess.square(file, rank)
#             board.set_piece_at(square, chess.Piece(chess.QUEEN, chess.WHITE))
#             queen_moves = [move.uci() for move in board.legal_moves]
#             all_possible_piece_moves.extend(queen_moves)

#             # board.clear_board()
#             board = chess.Board(None)
#             board.set_piece_at(square, chess.Piece(chess.KNIGHT, chess.WHITE))
#             knight_moves = [move.uci() for move in board.legal_moves]
#             all_possible_piece_moves.extend(knight_moves)
#     pawn_promotion_moves = generate_promotion_moves()
#     return all_possible_piece_moves + pawn_promotion_moves


def generate_pawn_promotions():
    # Define the promotion rows for both colors and the promotion pieces
    # promotion_rows = {'white': '7', 'black': '2'}
    promotion_rows = {'white': '7'}
    promotion_pieces = ['q', 'r', 'b', 'n']
    promotions = []

    # Iterate over each color
    for color, row in promotion_rows.items():
        # Target rows for promotion (8 for white, 1 for black)
        target_row = '8' if color == 'white' else '1'

        # Each file from 'a' to 'h'
        for file in 'abcdefgh':
            # Direct move to promotion
            for piece in promotion_pieces:
                promotions.append(f'{file}{row}{file}{target_row}{piece}')

            # Capturing moves to the left and right (if not on the edges of the board)
            if file != 'a':
                left_file = chr(ord(file) - 1)  # File to the left
                for piece in promotion_pieces:
                    promotions.append(f'{file}{row}{left_file}{target_row}{piece}')

            if file != 'h':
                right_file = chr(ord(file) + 1)  # File to the right
                for piece in promotion_pieces:
                    promotions.append(f'{file}{row}{right_file}{target_row}{piece}')

    return promotions


def get_all_possible_moves():
    
    all_moves = []

    for rank in range(8):
        for file in range(8): 
            square = chess.square(file, rank)
            
            board = chess.Board(None)
            board.set_piece_at(square, chess.Piece(chess.QUEEN, chess.WHITE))
            legal_moves = list(board.legal_moves)
            all_moves.extend(legal_moves)
            
            board = chess.Board(None)
            board.set_piece_at(square, chess.Piece(chess.KNIGHT, chess.WHITE))
            legal_moves = list(board.legal_moves)
            all_moves.extend(legal_moves)
    
    all_moves = [all_moves[i].uci() for i in range(len(all_moves))]
    
    pawn_promotions = generate_pawn_promotions()
    
    return all_moves + pawn_promotions

def board_to_tensor(board: chess.Board) -> torch.Tensor:
    """
    List of board channels (
        white pawn, white knight, white bishop, white rook, white queen, white king,
        black pawn, black knight, black bishop, black rook, black queen, black king,
        board colour,
        white king side castling, white queen side castling,
        black king side castling,  black queen side castling,
        en passant
    )
    """
    # Initialise tensor with zeros for the chessboard encoding
    piece_channels = 6 # p, k, b, r, q, k (white, and black) * 2 
    color_channel = 1
    castling_rights_channels = 4
    en_passant_channel = 1  
    n_channels = (piece_channels * 2) + color_channel + castling_rights_channels + en_passant_channel
    tensor = torch.zeros((n_channels, 8, 8), dtype=torch.float32)
    

    piece_types = [chess.PAWN, chess.KNIGHT, chess.BISHOP, chess.ROOK, chess.QUEEN, chess.KING]
    map_piece_idx = {piece:idx for idx, piece in enumerate(piece_types)}

    # Chess piece encoding
    for piece_type in piece_types:
        for color in [chess.WHITE, chess.BLACK]:
            pieces = board.pieces(piece_type, color)
            if pieces is None:
                continue
            
            channel_index = map_piece_idx[piece_type] + (0 if color else 6)
            for square in pieces:
                rank, file = divmod(square, 8)
                tensor[channel_index, file, rank] = 1.0

    # Chess color move encoding
    if board.turn:
        tensor[(piece_channels * 2), :, :] = 1.0

    # Castling rights move encoding
    castling_rights = [
        board.has_kingside_castling_rights(chess.WHITE),
        board.has_queenside_castling_rights(chess.WHITE),
        board.has_kingside_castling_rights(chess.BLACK),
        board.has_queenside_castling_rights(chess.BLACK)
    ]

    for idx, castling_right in enumerate(castling_rights):
        if castling_right:
            tensor[(piece_channels * 2) + color_channel + idx, :, :] = 1.0

    if board.ep_square:
        rank, file = divmod(board.ep_square, 8)
        tensor[piece_channels + color_channel + castling_rights_channels, rank, file] = 1.0

    return tensor


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]