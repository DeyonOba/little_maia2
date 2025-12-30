import pyzstd
import chess


def decompress_zstd(compressed_file_path: str, decompressed_file_path: str) -> None:
    with open(compressed_file_path, "rb") as compressed_file, open(decompressed_file_path, "wb") as decompressed_file:
        # src -> dst: source file-like object, destination file-like object
        pyzstd.decompress_stream(compressed_file, decompressed_file)


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


     