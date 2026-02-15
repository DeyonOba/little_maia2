import pathlib
import requests
import subprocess
from tqdm import tqdm
import io
import chess.pgn
import pyzstd
from maia2.utils import setup_data_directory


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
        print(f"TimeOutException: Request timed out for url -> {url}")
    except requests.exceptions.RequestException as e:
        print(f"An error occured while make the request to {url}: {e}")
    except ValueError as e:
        print(f"An error occured during request header manipulation: {e}")
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
        print("ERROR, something went wrong")
    else:
        print(f"\nDownloaded Lichess database for {year}-{month:02d} successfully.")


def download_lichess_database_buffered(year: int, month: int):
    url = f"https://database.lichess.org/standard/lichess_db_standard_rated_{year}-{month:02d}.pgn.zst"
    expected_size = get_lichess_database_metadata(year, month).get("content_length", 0)

    if expected_size == 0:
        raise ValueError(f"Expected content length is 0 for url: {url}. Cannot proceed with buffered download.")
    
    # print(f"Expected size: {expected_size} bytes")

    increment = 2 * 1024 * 1024 # 2 MB

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
        # print(f"Downloaded {len(buffer)} bytes from {start_byte} to {end_byte}")
        bytes_downloaded = end_byte if end_byte < expected_size else expected_size
        yield bytes(buffer), bytes_downloaded

        start_byte = end_byte + 1
        end_byte = start_byte + increment
        # print(f"Next range: bytes={start_byte}-{end_byte}")
