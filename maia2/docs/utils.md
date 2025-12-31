# File: `utils.py` Code Explaination

## function: `generate_promotion_moves`
Aim: Generate all posible pawn promotion moves.

**Chess Pawn Promotion**:
*Gemini AI Overview*
> Pawn promotion in chess is a rule where a pawn reaching the opponent's back rank (8th for White, 1st for Black) must be exchanged for a queen, rook, bishop, or knight of the same color, chosen by the player. This is usually done for a queen (queening), but underpromotion to a knight, rook, or bishop can be crucial in rare tactical situations, like forcing a checkmate or avoiding stalemate. 

**Code**:
```python
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
```
**Code Description**:
1. Define a variable `all_pawn_promotion_moves` that stores a list of pawn promotion moves.
2. Define two variables `white_promotion_rank`, and `black_promotion_rank` that stores the rank each opponent (black, or white) must reach before promotion. The board rank 6 and 1 were assigned using zero indexing.
3. Iterate through each file (i.e a file in a chess board is made up of 8 columns from a to h)
4. Create an empty chess board with no chess pieces on the board, then set a pawn on the assigned iterable variable `file` number and the default promotion rank (white).
5. Generate all legal promotion moves using a list comprehension to store the UCI formated chess move, then update the list `all_pawn_promotion_moves` with the generated moves.
6. Before generating the promotion move for black the board needs to be cleared again so that the moves generated would not include promotion moves for white also.
7. Assign the colour (i.e black) to play next, without assigning the colour or `chess.Board().turn`, the chess legal move generator would not generate any move since the default `turn` set upon the `chess.Board` instantiation is `chess.WHITE`.
8. Repeat the same steps but using the assigned `black_promotion_rank`, and `turn=chess.BLACK`.
9. Return `all_pawn_promotion_moves`.

## function: `get_all_possible_moves`

**Aim**: Generate all possible move over the board for each piece.

Each piece on a chess board have distinct move and move constraints.
> **Piece Movements**: (*Gemini AI Overview*)
> - King: Moves one square in any direction (horizontal, vertical, or diagonal).
> - Queen: Moves any number of squares in any straight line (horizontally, vertically, or diagonally).
> - Rook: Moves any number of squares horizontally or vertically.
> - Bishop: Moves any number of squares diagonally, staying on the same color square.
> - Knight: Moves in an "L" shape (two squares in one direction, then one square perpendicularly) and can jump over other pieces.
> - Pawn: Moves one square forward, but two squares on its first move. Captures one square diagonally forward.

If you'll look closely at the piece movements you would notice that the queen moves is a superset for all king, rook, bishop, and pawn moves, but knight moves are distinct. In order to generate all the possible moves we need three distinct set of moves *(queen moves + knight moves + pawn promotion moves)*.

**Code**:
```python
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
```

