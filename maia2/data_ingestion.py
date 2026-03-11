import aiohttp
import asyncio
# import chess.pgn
import codecs
import heapq
import io
import pyzstd
from pathlib import Path
import requests
import re
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor
import json
import tqdm
import hashlib
import signal
import random
from maia2.utils import setup_data_directory
from maia2.logger import get_logger

MB: int = 1024 * 1024
log = get_logger("data")

# Optimized Regex patterns
# Captures: [WhiteElo "1150"] -> Groups: WhiteElo, 1150
TAG_RE = re.compile(r'\[(WhiteElo|BlackElo|Event)\s+"([^"]+)"\]')
# Boundary: Splits strictly at the start of a new PGN block
GAME_BOUNDARY = re.compile(r'\n(?=\[Event )')


def get_lichess_database_metadata(year: int, month: int) -> dict:
    url = f"https://database.lichess.org/standard/lichess_db_standard_rated_{year}-{month:02d}.pgn.zst"
    try:
        response: requests.Request = requests.get(url, timeout=5)
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


class AsyncOrderedQueue:
    def __init__(self, max_chunks: int = 8):
        self.q = asyncio.PriorityQueue(maxsize=max_chunks)
    
    async def put(self, priority: int, data):
        print("Put chunk with priority", priority)
        await self.q.put((priority, data))
        print("Put chunk with priority", priority, "into queue")
        
    async def get(self):
        priority, data = await self.q.get()
        
        if data is None:
            raise StopAsyncIteration
        
        return priority, data
    
    async def close(self):
        await self.q.put((float('inf'), None))

  
class DownloadCheckpoint:
    def __init__(self, checkpoint_path: Path, target_file: Path):
        self.path = Path(checkpoint_path)
        self.target_file = Path(target_file)
        
        # Internal state defaults
        self.state = {
            "next_byte": 0,          # Remote byte position
            "complete": False,       # Full download & process finish
            "expected_size": None,   # Remote content-length
            "processed_games": 0,    # Counter for your DB
            "last_sync_point": 0,    # Last known Zstd frame boundary
            "checksum": None         # SHA-256 hex of data processed so far
        }
        
        self._hasher = hashlib.sha256()
        self._load_and_validate()

    def _load_and_validate(self):
        """Loads state and performs a sanity check against the actual output file."""
        if not self.path.exists():
            return

        try:
            stored = json.loads(self.path.read_text())
            
            # If the output file was deleted but checkpoint exists, reset next_byte
            if not self.target_file.exists() and stored.get("next_byte", 0) > 0:
                print("Target file missing. Resetting checkpoint to 0.")
                return

            self.state.update(stored)
            
        except (json.JSONDecodeError, KeyError) as e:
            print(f"Checkpoint corrupted ({e}). Initializing new state.")

    def update_hash(self, chunk: bytes):
        """Update rolling hash of the COMPRESSED stream."""
        self._hasher.update(chunk)

    def commit(self, next_byte: int, games_count: int = None, is_sync_point: bool = False, complete: bool = False):
        self.state["next_byte"] = next_byte
        self.state["complete"] = complete
        self.state["checksum"] = self._hasher.hexdigest()
        
        if games_count is not None:
            self.state["processed_games"] = games_count
            
        if is_sync_point:
            self.state["last_sync_point"] = next_byte

        # Atomic Write Pattern
        temp_path = self.path.with_suffix(".tmp")
        with open(temp_path, "w") as f:
            json.dump(self.state, f, indent=4)
        
        # rename/replace is atomic on most OSs
        temp_path.replace(self.path)

    @property
    def next_byte(self) -> int:
        return self.state["next_byte"]

    @property
    def last_sync_point(self) -> int:
        """The last byte where Zstd decompression was guaranteed to be stable."""
        return self.state["last_sync_point"]

    @property
    def next_byte(self) -> int:
        return self.state["next_byte"]

    @property
    def is_complete(self) -> bool:
        return self.state["complete"]       


class ZstdUtf8Stream:
    ZSTD_MAGIC = b'\x28\xb5\x2f\xfd'

    def __init__(self):
        self._zstd = pyzstd.EndlessZstdDecompressor()
        self._decoder = codecs.getincrementaldecoder("utf-8")()
        self._in_sync = True
        self._last_call_synced = False  # The flag for just_synced()

    def just_synced(self) -> bool:
        """Returns True if the last feed() call found a new sync point."""
        return self._last_call_synced

    def feed(self, chunk: bytes) -> str:
        if not chunk:
            self._last_call_synced = False
            return ""

        # Reset sync flag for this specific chunk
        self._last_call_synced = False

        try:
            if not self._in_sync:
                # Attempt to find magic number
                synced_chunk = self._resync(chunk)
                if not synced_chunk:
                    return ""
                chunk = synced_chunk
                self._last_call_synced = True # Signal that we found a boundary

            decompressed = self._zstd.decompress(chunk)
            return self._decoder.decode(decompressed)

        except (pyzstd.ZstdError, UnicodeDecodeError) as e:
            # Fallback for mid-stream corruption or lost context
            self._in_sync = False
            self._zstd = pyzstd.EndlessZstdDecompressor()
            self._decoder = codecs.getincrementaldecoder("utf-8")()
            return self.feed(chunk) # Recursively retry with the current chunk

    def _resync(self, chunk: bytes) -> bytes:
        pos = chunk.find(self.ZSTD_MAGIC)
        if pos != -1:
            self._in_sync = True
            return chunk[pos:]
        return b""

    def flush(self):
        try:
            tail = self._zstd.decompress(b"")
            return self._decoder.decode(tail, final=True)
        except:
            return ""

  
def fast_filter_pgn_games(pgn_text: str, elo_threshold: int = 1200) -> bool:
    if 'Blitz' not in pgn_text:
        return False
    # Extract only relevant header tags using regex
    tags = dict(TAG_RE.findall(pgn_text))
    
    try:
        white_elo = int(tags.get("WhiteElo", 0))
        black_elo = int(tags.get("BlackElo", 0))
        # Simple filter: Only consider games where at lease one player has an Elo equal to or below the threshold
        # TODO: Verify other tags like Event, TimeControl, etc. to further optimize filtering
        if white_elo <= elo_threshold or black_elo <= elo_threshold:
            return True
    except ValueError:
        pass
    return False


class ParallelPgnProcessor:
    def __init__(self, workers=None):
        self.executor = ProcessPoolExecutor(max_workers=workers or mp.cpu_count())
        self._leftover = ""

    async def process_text(self, text: str):
        full_text = self._leftover + text
        
        # Find the last boundary to keep data integrity
        parts = GAME_BOUNDARY.split(full_text)
        
        if len(parts) < 2:
            self._leftover = full_text
            return []

        # The last part is incomplete, save it for the next feed
        self._leftover = parts.pop()
        
        # Offload the list of strings to the process pool
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self.executor, self._worker_batch, parts)

    @staticmethod
    def _worker_batch(game_list):
        # This runs in a separate process
        return [g for g in game_list if fast_filter_pgn_games(g)]
    
class PgnStreamParser:
    # Boundary marker for Lichess PGNs
    # GAME_START_RE = re.compile(r'(?=\[Event )')

    def __init__(self, skip_until_count: int = 0):
        self._buffer = ""
        self.games_emitted = 0
        self.games_skipped = 0
        self.skip_until_count = skip_until_count
        self.is_fast_forwarding = skip_until_count > 0

    def feed(self, text: str):
        if not text:
            return []

        self._buffer += text
        
        # Find all game starts in the current buffer
        parts = GAME_BOUNDARY.split(self._buffer)
        
        # If the buffer doesn't contain a full game yet, wait
        if len(parts) < 2:
            return []

        # The last part is likely incomplete (tail), keep it in buffer
        self._buffer = parts.pop()
        
        ready_to_process = []

        for game_text in parts:
            if not game_text.strip():
                continue
            
            # Fast Forward Logic
            if self.is_fast_forwarding:
                self.games_skipped += 1
                if self.games_skipped <= self.skip_until_count:
                    continue  # Drop this game, we've already processed it
                else:
                    self.is_fast_forwarding = False
                    print(f"Fast-forward complete. Resuming emission at game {self.games_skipped}")

            ready_to_process.append(game_text.strip())
            self.games_emitted += 1

        return ready_to_process

    @property
    def total_seen(self):
        """Total games encountered (skipped + emitted)."""
        return self.games_skipped + self.games_emitted
    

def plan_request_ranges(start_byte: int, total_size: int, chunk_size: int):
    pos = start_byte
    
    while pos < total_size:
        end_byte = min(pos + chunk_size - 1, total_size - 1)
        yield pos, end_byte
        pos = end_byte + 1
        

async def async_parallel_stream(
    url: str,
    expected_size: int,
    start_byte: int = 0,
    expected_sha256: str = None,
    chunk_size: int = 4 * 1024 * 1024, # Ensure MB is defined
    workers: int = 6,
    max_retries: int = 3
):
    connector = aiohttp.TCPConnector(limit=workers * 2)
    stop_event = asyncio.Event()
    sha256_hash = hashlib.sha256()
    
    pbar = tqdm.tqdm(
        total=expected_size - start_byte,
        unit="B",
        unit_scale=True,
        desc="Downloading & Hashing".rjust(25),
        leave=True
    )
    
    async with aiohttp.ClientSession(connector=connector) as session:
        queue = asyncio.PriorityQueue(maxsize=workers * 2)
        range_gen = plan_request_ranges(start_byte, expected_size, chunk_size)
        expected_pos = start_byte
        
        async def fetch_worker(worker_id):
            for start, end in range_gen:
                if stop_event.is_set():
                    break
                
                for attempt in range(max_retries + 1):
                    if stop_event.is_set():
                        return
                    
                    try:
                        headers = {"Range": f"bytes={start}-{end}"}
                        
                        async with session.get(url, headers=headers, timeout=30) as response:
                            if response.status not in (200, 206):
                                raise RuntimeError(f"Error: HTTP {response.status}")

                            data = await response.read()
                            await queue.put((start, data))
                            break 
                    except (aiohttp.TimeoutError, aiohttp.ClientError):
                        if attempt == max_retries:
                            stop_event.set()
                            return
                        
                        wait = (2 ** attempt) + random.uniform(0, 1)
                        pbar.set_postfix_str(f"Retry W-{worker_id} in {wait:.1f}s")
                        await asyncio.sleep(wait)
                        
        worker_tasks = [
            asyncio.create_task(fetch_worker(worker_id=i), name=f"W-{i}")
            for i in range(workers)
        ]
        heap = []
        
        try:
            while expected_pos < expected_size:
                workers_done = all(t.done() for t in worker_tasks)
                if workers_done and queue.empty() and not heap:
                    break
                
                try:
                    start, data = await asyncio.wait_for(queue.get(), timeout=0.2)
                    heapq.heappush(heap, (start, data))
                except asyncio.TimeoutError:
                    continue
            
                while heap and heap[0][0] == expected_pos:
                    s, chunk = heapq.heappop(heap)
                    
                    if expected_pos + len(chunk) > expected_size:
                        chunk = chunk[:expected_size - expected_pos]
                    
                    sha256_hash.update(chunk)
                    yield s, chunk
                        
                    expected_pos += len(chunk)
                    pbar.update(len(chunk))
                    
                    if expected_pos >= expected_size:
                        stop_event.set()
                        break
            
            if expected_sha256:
                actual_hash = sha256_hash.hexdigest()
                if expected_sha256.lower() == actual_hash:
                    pbar.set_postfix_str("Verified ✓")
                else:
                    raise ValueError(f"Hash Mismatch!")
        finally:
            pbar.close()
            stop_event.set()
            for t in worker_tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*worker_tasks, return_exceptions=True)


async def process_lichess_pgn_database(year: int, month: int):
    url = (
        f"https://database.lichess.org/standard/"
        f"lichess_db_standard_rated_{year}-{month:02d}.pgn.zst"
    )

    meta = get_lichess_database_metadata(year, month)
    expected_size = meta["content_length"]

    data_dir = setup_data_directory()
    processed_data = data_dir / f"lichess_blitz_games_{year}_{month:02d}.pgn"
    ratings_data = data_dir / f"blitz_ratings_{year}_{month:02d}.txt"
    checkpoint_path = data_dir / f"lichess_{year}_{month:02d}.checkpoint.json"

    checkpoint = DownloadCheckpoint(checkpoint_path, processed_data)
    resume_byte = checkpoint.last_sync_point if checkpoint.last_sync_point > 0 else 0
    
    # FIX: Open in Append mode ("a") to prevent wiping progress on resume
    # If it's a fresh start (resume_byte == 0), the file will just be created.
    file_mode = "a" if resume_byte > 0 else "w"
    
    processor = ParallelPgnProcessor()
    # If resuming, tell the parser how many games to ignore to avoid duplicates
    parser = PgnStreamParser(skip_until_count=checkpoint.state["processed_games"])
    zstream = ZstdUtf8Stream()
    
    # Variable to track if we should shut down gracefully
    keep_running = True

    def handle_exit(sig, frame):
        nonlocal keep_running
        print("\nShutdown signal received. Finishing current chunk and saving...")
        keep_running = False

    # Attach signal listeners for Ctrl+C (SIGINT) and Kill (SIGTERM)
    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)

    try:
        with (
            open(processed_data, file_mode, encoding="utf-8") as out,
            open(ratings_data, file_mode, encoding="utf-8") as ratings_out,
            tqdm.tqdm(total=expected_size, unit="B", unit_scale=True, 
                      initial=resume_byte, desc=f"Processing {year}-{month:02d}".rjust(25)) as pbar
        ):
            async for pos, raw_chunk in async_parallel_stream(
                url, expected_size, resume_byte, chunk_size=32*1024*1024
            ):
                if not keep_running:
                    break
                
                # Update rolling hash first
                checkpoint.update_hash(raw_chunk)
                
                # Decompress
                text_data = zstream.feed(raw_chunk)
                is_sync = zstream.just_synced()
                
                # Parse with Fast-Forward (removes duplicates from the overlap)
                games_to_process = parser.feed(text_data)
                
                # Filter & Write
                if games_to_process:
                    valid_games = await processor.process_text("\n\n".join(games_to_process))
                    if valid_games:
                        out.write("\n\n".join(valid_games) + "\n\n")
                        out.flush() # Ensure it hits the disk

                # Commit state
                checkpoint.commit(
                    next_byte=pos + len(raw_chunk),
                    games_count=parser.total_seen,
                    is_sync_point=is_sync
                )
                
                pbar.update(len(raw_chunk))

            # Final Flush
            if keep_running:
                final_text = zstream.flush()
                valid_games = await processor.process_text(final_text)
                if valid_games:
                    with open(processed_data, "a", encoding="utf-8") as out:
                        out.write("\n\n".join(valid_games) + "\n\n")
                        out.flush()
                checkpoint.commit(next_byte=expected_size, complete=True)

    except Exception as e:
        print(f"An error occurred: {e}")
        # Final emergency checkpoint save
        checkpoint.commit(next_byte=checkpoint.next_byte, complete=False)
        raise


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
