import pathlib
import requests
import subprocess
from tqdm import tqdm


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
        import pprint
        response = requests.head(url, allow_redirects=True, timeout=5)
        response.raise_for_status()

        headers = response.headers

        content_type, content_length = headers.get("content-type"), headers.get("content-length")
    
        if content_length is not None:
            content_size = round(int(content_length) / (1024 * 1024), 2)
        else:
            print(f"Validate Lichess database URL -> '{url}'")
            content_size = 0
        return content_size, content_type
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



if __name__ == "__main__":
    url = "https://database.lichess.org/standard/lichess_db_standard_rated_2013-01.pgn.zst"
    expected_size = 17.8 #(MB)
    content_size, content_type = get_data_info(url)
    print(f"Excepted size: {expected_size}")
    print(f"Requested content size: {content_size}")
    print(f"Requested content type: {content_type}")

    download_lichess_database(year=2013, month=1)
