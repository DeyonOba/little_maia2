import requests


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


if __name__ == "__main__":
    url = "https://database.lichess.org/standard/lichess_db_standard_rated_2013-01.pgn.zst"
    expected_size = 17.8 #(MB)
    content_size, content_type = get_data_info(url)
    print(f"Excepted size: {expected_size}")
    print(f"Requested content size: {content_size}")
    print(f"Requested content type: {content_type}")
