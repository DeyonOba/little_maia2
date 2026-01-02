import pyzstd
import chess
import torch
import re


def decompress_zstd(compressed_file_path: str, decompressed_file_path: str) -> None:
    with open(compressed_file_path, "rb") as compressed_file, open(decompressed_file_path, "wb") as decompressed_file:
        # src -> dst: source file-like object, destination file-like object
        pyzstd.decompress_stream(compressed_file, decompressed_file)


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
    start = 1100
    stop = 2000

    range_dict = {f"<{start}": 0}
    range_index = 1

    for lower_bound in range(start, stop, interval):
        upper_bound = lower_bound + interval - 1

        range_dict[f"{lower_bound}-{upper_bound}"] = range_index
        range_index += 1
    
    range_dict[f">={stop}"] = range_index

    return range_dict


def map_to_category(elo: int, elo_dict: dict) -> int:
    start = 1100
    stop = 2000
    interval = 100

    if elo < start:
        return elo_dict[f"<{start}"]
    elif elo >= stop:
        return elo_dict[f">={stop}"]
    else:
        lower_bound = start + ((elo - start) // interval) * interval
        upper_bound = lower_bound + interval - 1
        return elo_dict[f"{lower_bound}-{upper_bound}"]


def generate_promotion_moves():
    all_pawn_promotion_moves = []
    white_promotion_rank, black_promotion_rank = 6, 1

    for file in range(8):
        board = chess.Board(None)
        board.set_piece_at(chess.square(file, white_promotion_rank), chess.Piece(chess.PAWN, chess.WHITE))
        white_promotion_moves = [move.uci() for move in board.legal_moves]
        all_pawn_promotion_moves.extend(white_promotion_moves)

        board.clear_board()
        board.turn = chess.BLACK
        board.set_piece_at(chess.square(file, black_promotion_rank), chess.Piece(chess.PAWN, chess.BLACK))
        black_promotion_moves = [move.uci()  for move in board.legal_moves]
        all_pawn_promotion_moves.extend(black_promotion_moves)

    return all_pawn_promotion_moves


def get_all_possible_moves():
    all_possible_piece_moves = []

    for rank in range(8):
        for file in range(8):
            board = chess.Board(None)
            square = chess.square(file, rank)
            board.set_piece_at(square, chess.Piece(chess.QUEEN, chess.WHITE))
            queen_moves = [move.uci() for move in board.legal_moves]
            all_possible_piece_moves.extend(queen_moves)

            # board.clear_board()
            board = chess.Board(None)
            board.set_piece_at(square, chess.Piece(chess.KNIGHT, chess.WHITE))
            knight_moves = [move.uci() for move in board.legal_moves]
            all_possible_piece_moves.extend(knight_moves)
    pawn_promotion_moves = generate_promotion_moves()
    return all_possible_piece_moves + pawn_promotion_moves


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
