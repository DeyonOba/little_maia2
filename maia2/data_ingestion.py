import aiohttp
import asyncio
import codecs
import heapq
import io
import os
import pyzstd
import psutil
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
from .utils import setup_project_directories, compress_zst
from .logger import get_logger
import sys
import time
from urllib.parse import urlparse
from typing import Dict, Any

MB: int = 1024 * 1024
log = get_logger("data")

# Optimized Regex patterns
# Captures: [WhiteElo "1200"] -> Groups: WhiteElo, 1200
TAG_RE = re.compile(r'\[(WhiteElo|BlackElo|Event|TimeControl|WhiteTitle|BlackTitle)\s+"([^"]+)"\]')
# Boundary: Splits strictly at the start of a new PGN block
GAME_BOUNDARY = re.compile(r'\n(?=\[Event )')
ELO_PATTERN = re.compile(r'\[WhiteElo "(\d+)"\]\s*\[BlackElo "(\d+)"\]')
MAX_CHUNK_SIZE = 20 * MB  # Absolute upper limit to prevent OOM, can be adjusted based on testing
N_RETRIES = 5

PATHS = setup_project_directories(verbose=True)


def get_available_ram() -> int:
    return psutil.virtual_memory().available


def dynamic_chunk_size(num_workers: int, safety_factor: float = 0.2) -> int:
    available_ram = get_available_ram()
    useable_ram = int(available_ram * (1 - safety_factor))

    # Account for state of the data: download (queue) + decompression/processing (7x)
    total_weigthed_slots = num_workers * 2 + num_workers * 7

    budget = useable_ram // total_weigthed_slots
    CHUNK_FLOOR = 5 * MB
    return max(CHUNK_FLOOR, min(budget, MAX_CHUNK_SIZE))


def get_url_content_info(url: str) -> Dict[str, Any]:
    """
    Retrieves metadata for a URL using a HEAD request with exponential backoff.
    """
    with requests.Session() as session:
        for attempt in range(N_RETRIES):
            try:
                response = session.head(url, timeout=10, allow_redirects=True, stream=True)
                
                # Handles common transient errors with retries
                if response.status_code in (408, 429, 503, 504):
                    wait_for = (2 ** attempt) + random.uniform(0, 1)
                    log.warning(f"Retryable error {response.status_code} for {url}. "
                                f"Attempt {attempt + 1}/{N_RETRIES}. Waiting {wait_for:.1f}s.")
                    time.sleep(wait_for)
                    continue

                # Treat other 4xx/5xx as terminal for this URL
                response.raise_for_status()

                headers = response.headers
                content_length = headers.get("Content-Length")
                
                # Robust parsing of domain using standard library
                parsed_url = urlparse(url)
                domain = parsed_url.netloc

                # Peer info extraction: Handle both raw socket and potential lack thereof
                ip_address, port = None, None
                try:
                    # Accessing the underlying urllib3 connection
                    conn = response.raw._connection if hasattr(response.raw, '_connection') else None
                    if conn and hasattr(conn, 'sock') and conn.sock:
                        ip_address, port = conn.sock.getpeername()
                except Exception:
                    pass

                return {
                    "url": url,
                    "server": headers.get('Server'),
                    "domain": domain,
                    "status_code": response.status_code,
                    "content_type": headers.get("Content-Type"),
                    "content_length": int(content_length) if content_length and content_length.isdigit() else 0,
                    "request_date": headers.get("Date"),
                    "last_modified_date": headers.get("Last-Modified"),
                    "ip_address": ip_address,
                    "port": port
                }

            except requests.exceptions.RequestException as e:
                log.error(f"Network error for {url} (attempt {attempt + 1}/{N_RETRIES}): {e}")
                if attempt == N_RETRIES - 2: # Final attempt before giving up is after 2 ** 3 = 8 seconds
                    return {}
                time.sleep((2 ** attempt) + random.uniform(0, 1))

    return {}

  
class DownloadCheckpoint:
    def __init__(self, checkpoint_path: Path, target_file: Path):
        self.path = Path(checkpoint_path)
        self.target_file = Path(target_file)
        
        # Internal state defaults
        self.state = {
            "next_byte": 0,          
            "complete": False,       
            "expected_size": None,   
            "processed_games": 0,    
            "last_sync_point": 0,    
            "checksum": None
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
                print("Target file missing. Resetting checkpoint to 0.", flush=True)
                return

            self.state.update(stored)
            
        except (json.JSONDecodeError, KeyError) as e:
            print(f"Checkpoint corrupted ({e}). Initializing new state.", flush=True)

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
                self._last_call_synced = True

            decompressed = self._zstd.decompress(chunk)
            return self._decoder.decode(decompressed)

        except (pyzstd.ZstdError, UnicodeDecodeError) as e:
            # Fallback for mid-stream corruption or lost context
            self._in_sync = False
            self._zstd = pyzstd.EndlessZstdDecompressor()
            self._decoder = codecs.getincrementaldecoder("utf-8")()
            return self.feed(chunk)

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


def fast_filter_pgn_games(pgn_text: str, from_rating: int = 1500, to_rating: int = 1550) -> bool:
    
    tags = dict(TAG_RE.findall(pgn_text))
    
    try:
        white_elo = int(tags.get("WhiteElo", 0))
        black_elo = int(tags.get("BlackElo", 0))
        event = tags.get("Event", "")
        time_control = tags.get("TimeControl", "??")
        white_title = tags.get("WhiteTitle", "??")
        black_title = tags.get("BlackTitle", "??")

        if not event or "Rated" not in event or "Blitz" not in event:
            return False
        
        if time_control == "??" or time_control == "?":
            return False
        
        if white_title == 'BOT' or black_title == 'BOT':
            return False
        
        # Ensure that the elo range for black and white fall within the stated range
        if from_rating <= white_elo <= to_rating and from_rating <= black_elo <= to_rating:
            return True
    except ValueError:
        pass
    return False


class ParallelPgnProcessor:
    def __init__(self, workers=None):
        self.executor = ProcessPoolExecutor(max_workers=workers or mp.cpu_count())
        self._leftover = ""
        self._closed = False

    async def process_text(self, text: str, from_rating: int, to_rating: int):
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
        return await loop.run_in_executor(self.executor, self._worker_batch, parts, from_rating, to_rating)

    @staticmethod
    def _worker_batch(game_list, from_rating=1500, to_rating=1550):
        # This runs in a separate process
        return [g for g in game_list if fast_filter_pgn_games(g, from_rating, to_rating)]

    def close(self):
        if self._closed:
            return
        self._closed = True
        try:
            self.executor.shutdown(wait=True, cancel_futures=True)
        except Exception as e:
            log.warning(f"ParallelPgnProcessor shutdown encountered: {e!r}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False
    
class PgnStreamParser:
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
                    print(f"Fast-forward complete. Resuming emission at game {self.games_skipped}", flush=True)

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
    chunk_size: int = 4 * MB,
    workers: int = 4,
    max_retries: int = 3,
    *,
    max_concurrent: int | None = None,
    request_timeout: float = 60.0,
    max_throttle_retries: int = 10,
):
    if max_concurrent is None:
        max_concurrent = workers

    connector = aiohttp.TCPConnector(limit=max_concurrent + 2, enable_cleanup_closed=True)
    timeout = aiohttp.ClientTimeout(
        total=request_timeout,
        sock_connect=15,
        sock_read=request_timeout,
    )
    sha256_hash = hashlib.sha256()
    request_semaphore = asyncio.Semaphore(max_concurrent)

    # Shared adaptive delay (siblings cool off after a 429)
    adaptive = {"delay": 0.0}

    pbar = tqdm.tqdm(
        total=expected_size - start_byte,
        unit="B",
        unit_scale=True,
        desc="Downloading & Hashing".rjust(25),
        leave=True,
    )

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        queue: asyncio.Queue = asyncio.Queue(maxsize=max(workers * 2, 4))
        work_queue: asyncio.Queue = asyncio.Queue()

        # Pre-populate every (start, end) tuple
        ranges = list(plan_request_ranges(start_byte, expected_size, chunk_size))
        for r in ranges:
            work_queue.put_nowait(r)
        for _ in range(workers):
            work_queue.put_nowait(None)  # sentinel

        loop = asyncio.get_running_loop()
        download_failure: asyncio.Future = loop.create_future()
        expected_pos = start_byte

        async def _fetch_chunk(worker_id: int, start: int, end: int) -> bool:
            """Fetch a single byte-range. Returns True on success, False on fatal failure."""
            attempt = 0
            throttle_attempts = 0

            while True:
                # Honour any adaptive delay accumulated by sibling 429s
                if adaptive["delay"] > 0:
                    await asyncio.sleep(adaptive["delay"])

                try:
                    headers = {"Range": f"bytes={start}-{end}"}
                    async with request_semaphore:
                        async with session.get(url, headers=headers) as response:
                            status = response.status

                            if status in (200, 206):
                                data = await response.read()
                                await queue.put((start, data))
                                # Decay the adaptive delay on success
                                adaptive["delay"] = max(0.0, adaptive["delay"] * 0.5)
                                return True

                            if status == 429:
                                throttle_attempts += 1
                                if throttle_attempts > max_throttle_retries:
                                    log.error(
                                        f"W-{worker_id} exhausted throttle retries "
                                        f"({max_throttle_retries}) for range {start}-{end}"
                                    )
                                    return False

                                retry_after_hdr = response.headers.get("Retry-After")
                                try:
                                    retry_after = float(retry_after_hdr) if retry_after_hdr else 30.0
                                except ValueError:
                                    retry_after = 30.0

                                sleep_for = min(retry_after, 120.0) + random.uniform(0, 1.0)
                                # Bump shared adaptive delay so siblings slow down
                                adaptive["delay"] = min(
                                    max(adaptive["delay"], retry_after * 0.25),
                                    10.0,
                                )
                                log.warning(
                                    f"W-{worker_id} got 429 for {start}-{end}; "
                                    f"sleeping {sleep_for:.1f}s "
                                    f"(throttle attempt {throttle_attempts}/{max_throttle_retries})"
                                )
                                pbar.set_postfix_str(f"429 W-{worker_id} {sleep_for:.0f}s")
                                await asyncio.sleep(sleep_for)
                                continue

                            # Other non-2xx -> exponential backoff like a transient client error
                            raise RuntimeError(f"HTTP {status}")

                except (aiohttp.ClientError, asyncio.TimeoutError, RuntimeError) as e:
                    if attempt >= max_retries:
                        log.error(
                            f"W-{worker_id} fatal failure for range {start}-{end} "
                            f"after {max_retries} retries: {e!r}"
                        )
                        return False

                    wait = (2 ** attempt) + random.uniform(0, 1)
                    log.warning(
                        f"W-{worker_id} error for {start}-{end} "
                        f"(attempt {attempt + 1}/{max_retries}): {e!r}; "
                        f"retrying in {wait:.1f}s"
                    )
                    pbar.set_postfix_str(f"Retry W-{worker_id} in {wait:.1f}s")
                    await asyncio.sleep(wait)
                    attempt += 1

        async def fetch_worker(worker_id: int):
            while True:
                item = await work_queue.get()
                try:
                    if item is None:
                        return
                    start, end = item
                    ok = await _fetch_chunk(worker_id, start, end)
                    if not ok:
                        if not download_failure.done():
                            download_failure.set_exception(
                                RuntimeError(
                                    f"W-{worker_id} could not fetch range {start}-{end}"
                                )
                            )
                        return
                finally:
                    work_queue.task_done()

        worker_tasks = [
            asyncio.create_task(fetch_worker(worker_id=i), name=f"W-{i}")
            for i in range(workers)
        ]
        heap = []

        # Track normal completion (all work drained AND all workers exited)
        completion_task = asyncio.create_task(work_queue.join(), name="work-drain")

        try:
            while expected_pos < expected_size:
                queue_get_task = asyncio.create_task(queue.get(), name="queue-get")
                done, _pending = await asyncio.wait(
                    {queue_get_task, completion_task, download_failure},
                    return_when=asyncio.FIRST_COMPLETED,
                )

                if download_failure in done:
                    queue_get_task.cancel()
                    # Surface the worker's exception
                    raise download_failure.exception()

                if queue_get_task in done:
                    start, data = queue_get_task.result()
                    heapq.heappush(heap, (start, data))
                else:
                    queue_get_task.cancel()

                # Drain anything else already buffered in the queue. Important when
                # completion_task fires before queue_get_task even though the queue
                # still has items — we'd otherwise lose those buffered chunks.
                while True:
                    try:
                        s2, d2 = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                    heapq.heappush(heap, (s2, d2))

                # Drain the priority heap in order
                while heap and heap[0][0] == expected_pos:
                    s, chunk = heapq.heappop(heap)

                    if expected_pos + len(chunk) > expected_size:
                        chunk = chunk[: expected_size - expected_pos]

                    sha256_hash.update(chunk)
                    yield s, chunk

                    expected_pos += len(chunk)
                    pbar.update(len(chunk))

                    if expected_pos >= expected_size:
                        break

                # If workers are done, nothing in flight, and we still haven't
                # reached expected_size — there's an unrecoverable hole.
                if (
                    completion_task.done()
                    and queue.empty()
                    and expected_pos < expected_size
                ):
                    raise RuntimeError(
                        f"Workers exited before completing download "
                        f"(at {expected_pos}/{expected_size}, heap_size={len(heap)})"
                    )

            if expected_sha256:
                actual_hash = sha256_hash.hexdigest()
                if expected_sha256.lower() == actual_hash:
                    pbar.set_postfix_str("Verified ✓")
                else:
                    raise ValueError("Hash Mismatch!")
        finally:
            pbar.close()
            if not completion_task.done():
                completion_task.cancel()
            for t in worker_tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(completion_task, *worker_tasks, return_exceptions=True)


async def run_pipeline_async(
    year: int,
    month: int,
    from_rating: int = 1500,
    to_rating: int = 1550,
    run_test: bool = False,
    *,
    cfg=None,
):
    url = f"https://database.lichess.org/standard/lichess_db_standard_rated_{year}-{month:02d}.pgn.zst"

    metadata = get_url_content_info(url)
    expected_size = metadata.get("content_length")

    if not expected_size:
        log.error(f"Unable to retrieve URL content info for {url}. Aborting.")
        return

    paths = setup_project_directories(run_test=True) if run_test else PATHS
    processed_data = paths["processed_data"] / f"blitz_games_{year}_{month:02d}.pgn"
    ratings_data = paths["ratings_data"] / f"ratings_{year}_{month:02d}.csv"
    checkpoint_path = paths["data_checkpoints"] / f"download_{year}_{month:02d}.checkpoint.json"

    checkpoint = DownloadCheckpoint(checkpoint_path, processed_data)
    resume_byte = checkpoint.last_sync_point if checkpoint.last_sync_point > 0 else 0

    # If it's a fresh start (resume_byte == 0), the file will just be created.
    file_mode = "a" if resume_byte > 0 else "w"

    # CPU pool size for the game-filter stage stays generous; download fan-out is independent.
    cpu_workers = mp.cpu_count()
    chunk_size = dynamic_chunk_size(cpu_workers)

    download_workers = getattr(cfg, "download_workers", 4) if cfg is not None else 4
    max_concurrent = getattr(cfg, "max_concurrent_requests", download_workers) if cfg is not None else download_workers
    request_timeout = getattr(cfg, "request_timeout_seconds", 60.0) if cfg is not None else 60.0
    max_retries = getattr(cfg, "download_max_retries", 3) if cfg is not None else 3
    max_throttle_retries = getattr(cfg, "download_max_throttle_retries", 10) if cfg is not None else 10

    log.info(
        f"Pipeline {year}-{month:02d}: chunk={chunk_size // MB} MB, "
        f"cpu_workers={cpu_workers}, download_workers={download_workers}, "
        f"max_concurrent={max_concurrent}, RAM budget={get_available_ram() // MB} MB"
    )

    processor = ParallelPgnProcessor(cpu_workers)
    # If resuming, tell the parser how many games to ignore to avoid duplicates
    parser = PgnStreamParser(skip_until_count=checkpoint.state["processed_games"])
    zstream = ZstdUtf8Stream()

    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _request_shutdown():
        if not shutdown_event.is_set():
            log.warning(f"Shutdown signal received during {year}-{month:02d}. Draining current chunk.")
            shutdown_event.set()

    installed_signals = []
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_shutdown)
            installed_signals.append(sig)
        except (NotImplementedError, RuntimeError):
            # add_signal_handler is unsupported on some platforms (e.g. Windows)
            pass

    try:
        with (
            processor,
            open(processed_data, file_mode, encoding="utf-8") as out,
            open(ratings_data, file_mode, encoding="utf-8") as ratings_out,
            tqdm.tqdm(
                total=expected_size,
                unit="B",
                unit_scale=True,
                colour="green",
                initial=resume_byte,
                desc=f"Processing {year}-{month:02d}".rjust(25),
            ) as pbar,
        ):
            if ratings_out.tell() == 0:
                ratings_out.write("WhiteElo,BlackElo\n")
                ratings_out.flush()

            interrupted = False

            stream = async_parallel_stream(
                url,
                expected_size,
                resume_byte,
                chunk_size=chunk_size,
                workers=download_workers,
                max_retries=max_retries,
                max_concurrent=max_concurrent,
                request_timeout=request_timeout,
                max_throttle_retries=max_throttle_retries,
            )

            async for pos, raw_chunk in stream:
                if shutdown_event.is_set():
                    interrupted = True
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
                    valid_games = await processor.process_text(
                        "\n\n".join(games_to_process), from_rating, to_rating
                    )
                    if valid_games:
                        valid_games_str = "\n\n".join(valid_games) + "\n\n"
                        out.write(valid_games_str)
                        out.flush()
                        ratings_out.write(
                            "\n".join(
                                [f"{white},{black}" for white, black in ELO_PATTERN.findall(valid_games_str)]
                            )
                            + "\n"
                        )
                        ratings_out.flush()

                # Commit state
                checkpoint.commit(
                    next_byte=pos + len(raw_chunk),
                    games_count=parser.total_seen,
                    is_sync_point=is_sync,
                )

                pbar.update(len(raw_chunk))

            # Final flush only on a clean walk-through (no SIGINT mid-stream)
            if not interrupted:
                final_text = zstream.flush()
                valid_games = await processor.process_text(final_text, from_rating, to_rating)
                if valid_games:
                    valid_games_str = "\n\n".join(valid_games) + "\n\n"
                    out.write(valid_games_str)
                    out.flush()
                    ratings_out.write(
                        "\n".join(
                            [f"{white},{black}" for white, black in ELO_PATTERN.findall(valid_games_str)]
                        )
                        + "\n"
                    )
                    ratings_out.flush()

                checkpoint.commit(next_byte=expected_size, complete=True)

            if interrupted:
                # Idempotent emergency commit; preserves last_sync_point
                checkpoint.commit(next_byte=checkpoint.next_byte, complete=False)
                raise KeyboardInterrupt(f"Shutdown during {year}-{month:02d}")

    except KeyboardInterrupt:
        raise
    except Exception as e:
        log.error(f"run_pipeline_async error for {year}-{month:02d}: {e!r}")
        # Final emergency checkpoint save
        checkpoint.commit(next_byte=checkpoint.next_byte, complete=False)
        raise
    finally:
        for sig in installed_signals:
            try:
                loop.remove_signal_handler(sig)
            except (NotImplementedError, RuntimeError):
                pass


def download_games(cfg, run_test: bool = False):
    import time
    import traceback

    failure_backoff = getattr(cfg, "month_failure_backoff_seconds", 30)
    failure_count = 0

    for year in range(cfg.start_year, cfg.end_year + 1):
        start_month = cfg.start_month if year == cfg.start_year else 1
        end_month = cfg.end_month if year == cfg.end_year else 12

        for month in range(start_month, end_month + 1):
            filename = f"blitz_games_{year}_{month:02d}.pgn"
            paths = setup_project_directories(run_test) if run_test else PATHS
            src = paths["processed_data"] / filename
            dest = paths["raw_data"] / (filename + ".zst")

            if os.path.exists(dest):
                log.info(f"Skipping {year}-{month:02d}: {dest} already exists.")
                continue

            log.info(f"Starting download for {year}-{month:02d}")
            t0 = time.monotonic()
            try:
                asyncio.run(
                    run_pipeline_async(
                        year,
                        month,
                        cfg.elo_lower_bound,
                        cfg.elo_upper_bound,
                        run_test=run_test,
                        cfg=cfg,
                    )
                )
                if os.path.exists(src):
                    compress_zst(str(src), str(dest))
                    os.remove(src)
                log.info(
                    f"Completed {year}-{month:02d} in {time.monotonic() - t0:.1f}s"
                )
                failure_count = 0
            except KeyboardInterrupt:
                log.warning(
                    f"Interrupted during {year}-{month:02d}; checkpoint preserved."
                )
                raise
            except Exception as e:
                failure_count += 1
                log.error(
                    f"Failed {year}-{month:02d}: {e!r}\n{traceback.format_exc()}"
                )
                sleep_for = min(failure_backoff * failure_count, 300)
                log.info(
                    f"Backing off {sleep_for}s before next month (failure #{failure_count})."
                )
                time.sleep(sleep_for)
                continue
        

def download_lichess_database(year: int, month: int) -> None:
    data_dir = setup_project_directories()["raw_data"]

    if any(data_dir.glob(f"*{year}-{month:02d}.pgn*")):
        print(f"Lichess database for {year}-{month:02d} already exists in the data directory.")
        return
    
    url = f"https://database.lichess.org/standard/lichess_db_standard_rated_{year}-{month:02d}.pgn.zst"
    filename = f"lichess_db_standard_rated_{year}-{month:02d}.pgn.zst"
    file_path = data_dir / filename

    response = requests.get(url, stream=True)
    response.raise_for_status()

    metadata = get_url_content_info(url)

    if metadata and metadata.get("content_length"):
        print("Unable to retrieve URL content info. Aborting.", flush=True)
        return
    

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

    with tqdm.tqdm(total=total_size_in_bytes, unit='iB', unit_scale=True, colour='green') as progress_bar:
        with open(file_path, 'wb') as file:
            for data in response.iter_content(block_size):
                file.write(data)
                progress_bar.update(len(data))

    if total_size_in_bytes != 0 and progress_bar.n != total_size_in_bytes:
        log.error("ERROR, something went wrong")
    else:
        log.info(f"\nDownloaded Lichess database for {year}-{month:02d} successfully.")
