import chess.pgn
import codecs
import io
import pyzstd
import requests
import tqdm
from maia2.utils import setup_data_directory
from maia2.logger import get_logger

MB: int = 1024 * 1024
log = get_logger("data")


def get_lichess_database_metadata(year: int, month: int) -> dict:
    url = f"https://database.lichess.org/standard/lichess_db_standard_rated_{year}-{month:02d}.pgn.zst"
    try:
        response: requests.Request = requests.get(url, timeout=5)
        # response = requests.head(url, allow_redirects=True, timeout=5)
        response.raise_for_status()
        headers = response.headers
        content_type, content_length = headers.get("content-type"), headers.get("content-length")
        content_length = int(content_length) if content_length is not None else 0
        request_date, last_modified_date = headers.get("Date"), headers.get("Last-Modified")
        status_code = response.status_code
        domain = url.split("//")[1].split("/")[0]
        port, ip_address = response.raw.connection.sock.getpeername() if response.raw.connection and response.raw.connection.sock else (None, None)

        return {
            "url": url,
            "server": response.headers.get('Server'),
            "domain": domain,
            "status_code": status_code,
            "content_type": content_type,
            "content_length": content_length,
            "request_date": request_date,
            "last_modified_date": last_modified_date,
            "ip_address": ip_address,
            "port": port
        }
    except requests.exceptions.Timeout:
        log.error(f"TimeOutException: Request timed out for url -> {url}")
    except requests.exceptions.RequestException as e:
        log.error(f"An error occured while make the request to {url}: {e}")
    except ValueError as e:
        log.error(f"An error occured during request header manipulation: {e}")
    return {}


def download_lichess_database(year: int, month: int) -> None:
    data_dir = setup_data_directory()

    if any(data_dir.glob(f"*{year}-{month:02d}.pgn*")):
        print(f"Lichess database for {year}-{month:02d} already exists in the data directory.")
        return
    
    url = f"https://database.lichess.org/standard/lichess_db_standard_rated_{year}-{month:02d}.pgn.zst"
    filename = f"lichess_db_standard_rated_{year}-{month:02d}.pgn.zst"
    file_path = data_dir / filename

    response = requests.get(url, stream=True)
    response.raise_for_status()

    metadata = get_lichess_database_metadata(year, month)
    content_type = metadata.get("content_type", "unknown")
    total_size_in_bytes = metadata.get("content_length", 0)
    request_date = metadata.get("request_date", "unknown")
    last_modified_date = metadata.get("last_modified_date", "unknown")
    server = metadata.get("server", "unknown")
    domain = metadata.get("domain", "unknown")
    ip_address = metadata.get("ip_address", "unknown")
    port = metadata.get("port", "unknown")
    status_code = metadata.get("status_code", "unknown")
    block_size = 1024  # 1 Kilobyte

    info = (
        f"Data for {year}-{month:02d} is available. Downloading ...\n"
        f"[Request Date]::[Last Modified Date]  -- [{request_date}]::[{last_modified_date}] --\n"
        f"URL: {url}\n"
        f"Server: {server}\n"
        f"Resolved <domain>::<ip_address>::<port> {domain}::{ip_address}::{port}\n"
        f"HTTP request sent, response ... {status_code} OK\n"
        f"Length: {total_size_in_bytes} ({round(total_size_in_bytes)}) [{content_type}]\n"
        f"Saving to: `{str(file_path)}`\n"
        )
    print(info)

    with tqdm(total=total_size_in_bytes, unit='iB', unit_scale=True, colour='green') as progress_bar:
        with open(file_path, 'wb') as file:
            for data in response.iter_content(block_size):
                file.write(data)
                progress_bar.update(len(data))

    if total_size_in_bytes != 0 and progress_bar.n != total_size_in_bytes:
        log.error("ERROR, something went wrong")
    else:
        log.info(f"\nDownloaded Lichess database for {year}-{month:02d} successfully.")


def preprocess_pgn_game(game: chess.pgn.Game, output_file: io.FileIO, ratings_file: io.FileIO, analysis_mode: bool = True) -> None:
    if game is None:
        return
    
    event = game.headers.get("Event", "?")
    white_elo = game.headers.get("WhiteElo", "?")
    black_elo = game.headers.get("BlackElo", "?")
    result = game.headers.get("Result", "?")
    time_control = game.headers.get("TimeControl", "?")
    
    if event == "?" or white_elo == "?" or black_elo == "?" or result == "?" or time_control == "?":
        return
    
    if "Blitz" not in event:
        return
    
    if result not in ("1-0", "0-1", "1/2-1/2"):
        return

    try:
        white_elo, black_elo = int(white_elo), int(black_elo)
        if analysis_mode:
            ratings_file.write(f"{white_elo}\n{black_elo}\n")
        if white_elo <= 1200 and black_elo <= 1200:
            if output_file.tell() == 0:
                output_file.write(game.accept(chess.pgn.StringExporter(headers=True, variations=False, comments=True)))
            else:
                output_file.write("\n\n" + game.accept(chess.pgn.StringExporter(headers=True, variations=False, comments=True)))

    except ValueError as e:
        print(f"ValueError while processing game with Event: {event}, WhiteElo: {white_elo}, BlackElo: {black_elo}, Result: {result}. Error: {e}")
    except Exception as e:
        print(f"Unexpected error while processing game with Event: {event}, WhiteElo: {white_elo}, BlackElo: {black_elo}, Result: {result}. Error: {e}")


def stream_lichess_zst(
    url: str,
    expected_size: int,
    chunk_bytes: int = 2 * MB,
    session: requests.Session | None = None  
):
    if expected_size <= 0:
        log.error("`expected_size` must be greater than zero")
        raise RuntimeError("`expected_size` must be greater than zero")
    
    owns_session = session is None
    session = session or requests.Session()
    
    try:
        start_byte = 0
        
        while start_byte < expected_size:
            remaining = expected_size - start_byte
            request_size = min(chunk_bytes, remaining)
            end_byte = start_byte + request_size - 1
            
            headers = {"Range": f"bytes={start_byte}-{end_byte}"}
            log.info(f"Download next Range bytes={start_byte}-{end_byte}")
            
            with session.get(url, headers=headers, stream=True, timeout=60) as response:
                if response.status_code not in (200, 206):
                    raise RuntimeError(f"Server did not honor range request: {response.status_code}")
                
                bytes_seen: int = 0
                
                for chunk in response.iter_content(chunk_size=64*1024):
                    if not chunk:
                        continue
                    
                    bytes_seen += len(chunk)
                    yield chunk
                
                if bytes_seen != request_size and response.status_code == 206:
                    log.error(f"Partial download mismatch: expected->{request_size} got->{bytes_seen}")
                    raise RuntimeError(
                        f"Partial download mismatch: "
                        f"expected {request_size} "
                        f"got {bytes_seen}"
                    )
            log.info(f"Downloaded data from bytes={start_byte}-{end_byte}")
            start_byte = end_byte + 1
    finally:
        if owns_session:
            session.close()


class ZstdUtf8Stream:
    def __init__(self):
        self._zstd = pyzstd.EndlessZstdDecompressor()
        self._decoder = codecs.getincrementaldecoder("utf-8")()
        
    def feed(self, chunk: bytes) -> str:
        if not chunk:
            return ""
        
        decompressed = self._zstd.decompress(chunk)
        
        if not decompressed:
            return ""
        
        return self._decoder.decode(decompressed)
    
    def flush(self):
        tail = self._zstd.decompress(b"")
        return self._decoder.decode(tail, final=True)

        
class PgnStreamParser:
    def __init__(self):
        self._buffer = ""
        
    def feed(self, pgn_txt: str):
        # Reference documentation for python-chess
        # Reference: https://python-chess.readthedocs.io/en/latest/_modules/chess/pgn.html#read_game
        """
        By using text mode, the parser does not need to handle encodings. It is the
        caller's responsibility to open the file with the correct encoding.
        PGN files are usually ASCII or UTF-8 encoded,...
        
        ...
        
        The end of a game is determined by a completely blank line or the end of
        the file. (Of course, blank lines in comments are possible).
        
        ...
        
        The parser is relatively forgiving when it comes to errors. It skips over
        tokens it can not parse. By default, any exceptions are logged and
        collected in :data:`Game.errors <chess.pgn.Game.errors>`. This behavior can
        be :func:`overridden <chess.pgn.GameBuilder.handle_error>`.

        Returns the parsed game or ``None`` if the end of file is reached.
        """
        if not pgn_txt:
            return []
        
        self._buffer += pgn_txt
        games = []
        
        stream = io.StringIO(self._buffer)
        last_good_pos = 0
        
        while True:
            pos = stream.tell()
            game = chess.pgn.read_game(stream)
            
            if game is None:
                break
            
            last_good_pos = stream.tell()
            games.append(game)
        
        self._buffer = self._buffer[last_good_pos:]
        return games
        

def process_lichess_pgn_stream(year: int, month: int):
    url = (
        f"https://database.lichess.org/standard/"
        f"lichess_db_standard_rated_{year}-{month:02d}.pgn.zst"
    )
    
    meta = get_lichess_database_metadata(year, month)
    
    expected_size = meta["content_length"]
    
    data_dir = setup_data_directory()
    
    processed_data = data_dir / f"lichess_blitz_games_{year}_{month:02d}.pgn"
    ratings_data = data_dir / f"blitz_ratings_{year}_{month:02d}.txt"
    
    zstream = ZstdUtf8Stream()
    parser = PgnStreamParser()
    
    with (
        requests.Session() as session,
        open(processed_data, "w") as output_file,
        open(ratings_data, "w") as ratings_file,
        tqdm.tqdm(total=expected_size, unit="iB", unit_scale=True) as pbar,
    ):
        for raw_chunk in stream_lichess_zst(url, expected_size, session=session):
            pbar.update(len(raw_chunk))
            
            text = zstream.feed(raw_chunk)
            games = parser.feed(text)
            
            for game in games:
                preprocess_pgn_game(game, output_file, ratings_file)
                
        tail_text = zstream.flush()
        for game in parser.feed(tail_text):
            preprocess_pgn_game(game, output_file, ratings_file)
