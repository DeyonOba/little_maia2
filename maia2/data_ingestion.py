import pathlib
import requests
import subprocess
from tqdm import tqdm
import io
import chess.pgn
import pyzstd


def setup_data_directory() -> pathlib.Path:
    """
    Sets up the data directory for storing Lichess game databases.
    """
    data_dir = pathlib.Path(__file__).parent.parent / "data"
    if not data_dir.exists():
        print(f"Creating directory <data> within the root directory ...")
        data_dir.mkdir(parents=True, exist_ok=True)
        print("Created data directory ...")
    return data_dir


def get_data_info(url: str) -> tuple[float, str]:
    """
    Retrieves the content size and type for a given Lichess database URL using a HEAD request,
    without downloading the entire body.
    """
    try:
        response = requests.head(url, allow_redirects=True, timeout=5)
        response.raise_for_status()

        headers = response.headers

        content_type, content_length = headers.get("content-type"), headers.get("content-length")
    
        content_length = int(content_length) if content_length is not None else 0
        if content_length == 0:
            print(f"Validate Lichess database URL -> '{url}'")

        return content_length, content_type
    except requests.exceptions.Timeout:
        print(f"TimeOutException: Request timed out for url -> {url}")
    except requests.exceptions.RequestException as e:
        print(f"An error occured while make the request to {url}: {e}")
    except ValueError as e:
        print(f"An error occured during request header manipulation: {e}")
    return None, None




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

    total_size_in_bytes = int(response.headers.get('content-length', 0))
    server = response.headers.get('Server')
    content_type = response.headers.get('Content-Type')
    request_date, last_modified_date = response.headers.get("Date"), response.headers.get("Last-Modified")
    status_code = response.status_code
    ip_address, port = response.raw.connection.sock.getpeername()
    domain = url.split("//")[1].split("/")[0]
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
        print("ERROR, something went wrong")
    else:
        print(f"\nDownloaded Lichess database for {year}-{month:02d} successfully.")


def download_lichess_database_buffered(year: int, month: int):
    url = f"https://database.lichess.org/standard/lichess_db_standard_rated_{year}-{month:02d}.pgn.zst"
    expected_size, _ = get_data_info(url)
    print(f"Expected size: {expected_size} bytes")

    # increment = 2 * 1024 * 1024 # 10 MB
    increment = 1 * 1024 * 1024# 10 MB

    start_byte = 0
    end_byte = start_byte + increment - 1
    

    while start_byte < expected_size:
        header = {"Range": f"bytes={start_byte}-{end_byte}"}
        response = requests.get(url, headers=header, stream=True)
        response.raise_for_status()

        if response.status_code != 206:
            raise ValueError(f"Expected status code 206 for partial content, got {response.status_code}")
        
        buffer = bytearray()
        for chunk in response.iter_content(chunk_size=1024):
            if chunk:
                buffer.extend(chunk)
        print(f"Downloaded {len(buffer)} bytes from {start_byte} to {end_byte}")
        yield bytes(buffer), end_byte

        start_byte = end_byte + 1
        end_byte = start_byte + increment
        print(f"Next range: bytes={start_byte}-{end_byte}")



def process_lichess_pgn_stream(year: int, month: int):
    download_games = download_lichess_database_buffered(year, month)
    dp = pyzstd.EndlessZstdDecompressor()
    previous_buffer = io.StringIO()
    line_ref, prev_game_ref = 0, 0
    for chunk, expect_size in download_games:
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
            print(f"Combining previous buffer of size {previous_buffer.tell()} with current buffer of size {current_buffer.tell()}")
            print("previous buffer content:", previous_buffer.getvalue(), "...", sep="\n")
            print("current buffer content:", current_buffer.getvalue()[:50], "...", sep="\n")
            current_buffer = io.StringIO(combined_data)
            print("Combined data", current_buffer.getvalue()[:3000], "...", sep="\n")
            previous_buffer = io.StringIO()
            break
        count = 0
        while True:
            line_ref = current_buffer.tell()
            game = chess.pgn.read_game(current_buffer)
            # print("previous ref:", prev_game_ref, " current ref:", line_ref)
            if game is None:
                print(f"Previous char ref {prev_game_ref}, Current char pointer ref {current_buffer.tell()}")
                current_buffer.seek(prev_game_ref)
                remaining_data = current_buffer.read()
                print(f"Storing {len(remaining_data)} bytes of remaining data for next chunk.")
                print("---- Remaining Data Start ----"
                      f"\n{remaining_data}\n"
                      "---- Remaining Data End ----")
                previous_buffer = io.StringIO(remaining_data)
                break
            count += 1
            if count % 100 == 0:
                print(game.headers["White"], "vs", game.headers["Black"])
            # if count == 10:
            #     break
            prev_game_ref = line_ref
        # break
    





if __name__ == "__main__":
    year, month = 2020, 1
    # download_lichess_database(year, month)
    process_lichess_pgn_stream(year, month)


