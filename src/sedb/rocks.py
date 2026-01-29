import os
import re
import resource
import threading

from rocksdict import Rdict, Options, BlockBasedOptions, WriteOptions
from rocksdict import WriteBatch, Cache, DBCompressionType, ReadOptions
from pathlib import Path
from tclogger import FileLogger, TCLogger, TCLogbar, logstr, brp, brk
from tclogger import PathType, norm_path
from tclogger import KeyType, KeysType
from typing import Generator, Literal, TypedDict, Union, Any

from .message import ConnectMessager

logger = TCLogger()


def get_system_max_open_files() -> tuple[int, int]:
    """Get current and maximum open file limits from the system.

    Returns:
        tuple: (soft_limit, hard_limit)
    """
    try:
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        return soft, hard
    except Exception:
        return 1024, 1024  # Conservative fallback


def calc_safe_max_open_files(
    requested: int = -1,
    reserve_ratio: float = 0.7,
    min_value: int = 5000,
    max_value: int = 500000,
) -> int:
    """Calculate a safe max_open_files value based on system limits.

    Args:
        requested: User-requested value. -1 means auto-calculate.
        reserve_ratio: Fraction of system limit to use (leave headroom for other processes)
        min_value: Preferred minimum value (only used if system allows)
        max_value: Maximum value to return (RocksDB performs well with bounded limits)

    Returns:
        Safe max_open_files value for RocksDB
    """
    if requested > 0:
        return requested

    soft_limit, hard_limit = get_system_max_open_files()
    # Use a fraction of soft limit to leave room for other file handles
    calculated = int(soft_limit * reserve_ratio)

    # IMPORTANT: Never exceed the system soft limit!
    # If system limit is too low, we must respect it even if below min_value
    if soft_limit < min_value:
        # System limit is too restrictive, use what we can
        return max(256, calculated)  # At least 256 for basic operation

    return max(min_value, min(calculated, max_value))


def calc_parallelism(max_value: int = 128) -> int:
    return min(os.cpu_count() or 8, max_value)


# Global shared block cache - shared across all RocksOperator instances
# This ensures data cached by one instance can be reused by others
_SHARED_BLOCK_CACHE: Cache = None
_SHARED_BLOCK_CACHE_LOCK = threading.Lock()


def get_shared_block_cache(size_mb: int = 8192) -> Cache:
    """Get or create a shared block cache.

    Block cache is shared across all RocksOperator instances in the same process.
    This improves cache efficiency when multiple DB connections are created.
    """
    global _SHARED_BLOCK_CACHE
    with _SHARED_BLOCK_CACHE_LOCK:
        if _SHARED_BLOCK_CACHE is None:
            _SHARED_BLOCK_CACHE = Cache(size_mb * 1024 * 1024)
        else:
            # Resize if requested size is different
            # Note: Cache.set_capacity() can dynamically change the cache size
            current_size = size_mb * 1024 * 1024
            # Cache doesn't expose get_capacity(), so we track it via property
            _SHARED_BLOCK_CACHE.set_capacity(current_size)
        return _SHARED_BLOCK_CACHE


class RocksConfigsType(TypedDict, total=False):
    db_path: Union[str, Path]
    # options
    max_open_files: int = -1  # -1 = auto-calculate based on system limits
    max_file_opening_threads: int = 16
    target_file_size_base_mb: int = 256
    write_buffer_size_mb: int = 256
    max_write_buffer_number: int = 4
    level_zero_slowdown_writes_trigger: int = 20000
    level_zero_stop_writes_trigger: int = 50000
    parallelism: int = calc_parallelism()
    # table options
    block_cache_size_mb: int = 8192  # 8 GB
    bits_per_key: int = 10
    block_based: bool = False
    # retry options for transient errors
    max_retries: int = 3
    retry_delay_seconds: float = 1.0
    # read optimization options
    allow_mmap_reads: bool = True  # use mmap for reading SST files
    advise_random_on_open: bool = True  # optimize for random reads
    compaction_readahead_size_mb: int = 2  # readahead during compaction
    # read options (applied at read time, not db open time)
    verify_checksums: bool = False  # skip checksum verification for speed
    async_io: bool = True  # enable async IO for parallel reads
    read_readahead_size_mb: int = 2  # readahead for sequential patterns


class RocksOperator:
    """rocksdict API documentation
    * https://rocksdict.github.io/RocksDict/rocksdict.html

    RocksDB include headers:
    * https://github.com/facebook/rocksdb/blob/10.4.fb/include/rocksdb/db.h

    Write Stalls · facebook/rocksdb Wiki
    * https://github.com/facebook/rocksdb/wiki/Write-Stalls

    NOTE: Run `ulimit -n 1048576` to increase the max open files limit system-wide
    """

    def __init__(
        self,
        configs: RocksConfigsType,
        connect_at_init: bool = True,
        connect_msg: str = None,
        connect_cls: type = None,
        lock: threading.Lock = None,
        log_path: PathType = None,
        verbose: bool = True,
        indent: int = 0,
        raw_mode: bool = False,
    ):
        self.configs = configs
        self.connect_at_init = connect_at_init
        self.connect_msg = connect_msg
        self.verbose = verbose
        self.indent = indent
        self.raw_mode = raw_mode
        self.init_configs()
        self.msgr = ConnectMessager(
            msg=connect_msg,
            cls=connect_cls,
            opr=self,
            dbt="rocks",
            verbose=verbose,
            indent=indent,
        )
        self.lock = lock or threading.Lock()
        if log_path:
            self.file_logger = FileLogger(log_path)
        else:
            self.file_logger = None
        if self.connect_at_init:
            self.connect()

    def init_configs(self):
        # init db_path
        self.db_path = Path(self.configs["db_path"])

        # init retry options
        self.max_retries = self.configs.get("max_retries", 3)
        self.retry_delay = self.configs.get("retry_delay_seconds", 1.0)

        # init db options
        options = Options(raw_mode=self.raw_mode)
        options.create_if_missing(True)

        # Calculate safe max_open_files based on system limits
        # Using -1 (unlimited) can cause "Too many open files" when SST count exceeds ulimit
        requested_max_open = self.configs.get("max_open_files", -1)
        self.max_open_files = calc_safe_max_open_files(requested_max_open)
        options.set_max_open_files(self.max_open_files)

        options.set_max_file_opening_threads(
            self.configs.get("max_file_opening_threads", 16)
        )
        options.increase_parallelism(
            self.configs.get("parallelism", calc_parallelism())
        )
        options.set_max_background_jobs(
            self.configs.get("parallelism", calc_parallelism())
        )
        options.set_target_file_size_base(
            self.configs.get("target_file_size_base_mb", 64) * 1024 * 1024
        )
        options.set_write_buffer_size(
            self.configs.get("write_buffer_size_mb", 64) * 1024 * 1024
        )
        # More write buffers allow more concurrent writes before stalling
        options.set_max_write_buffer_number(
            self.configs.get("max_write_buffer_number", 4)
        )
        options.set_level_zero_slowdown_writes_trigger(
            self.configs.get("level_zero_slowdown_writes_trigger", 20000)
        )
        options.set_level_zero_stop_writes_trigger(
            self.configs.get("level_zero_stop_writes_trigger", 50000)
        )
        options.set_compression_type(DBCompressionType.lz4())

        # Read optimization options
        # Enable mmap reads - uses OS page cache, good for read-heavy workloads
        if self.configs.get("allow_mmap_reads", True):
            options.set_allow_mmap_reads(True)
        # Advise random access - tells OS not to do readahead on file open
        # Good for point lookups, bad for sequential scans
        if self.configs.get("advise_random_on_open", True):
            options.set_advise_random_on_open(True)
        # Compaction readahead - improves compaction performance
        compaction_readahead_mb = self.configs.get("compaction_readahead_size_mb", 2)
        if compaction_readahead_mb > 0:
            options.set_compaction_readahead_size(compaction_readahead_mb * 1024 * 1024)

        # init table options with SHARED block cache
        # Using shared cache ensures data cached by one RocksOperator can be
        # reused by other instances (e.g., benchmark analyzer + benchmark runner)
        table_options = BlockBasedOptions()
        cache_size_mb = self.configs.get("block_cache_size_mb", 8192)
        shared_cache = get_shared_block_cache(cache_size_mb)
        table_options.set_block_cache(shared_cache)
        table_options.set_bloom_filter(
            bits_per_key=self.configs.get("bits_per_key", 10),
            block_based=self.configs.get("block_based", False),
        )
        table_options.set_cache_index_and_filter_blocks(True)
        table_options.set_pin_l0_filter_and_index_blocks_in_cache(True)
        options.set_block_based_table_factory(table_options)

        self.db_options = options

        # init write options
        write_options = WriteOptions()
        write_options.no_slowdown = True
        self.write_options = write_options

        # init read options for optimized reads
        read_options = ReadOptions()
        if self.configs.get("verify_checksums", False) is False:
            read_options.set_verify_checksums(False)
        if self.configs.get("async_io", True):
            read_options.set_async_io(True)
        read_options.fill_cache(True)
        readahead_mb = self.configs.get("read_readahead_size_mb", 2)
        if readahead_mb > 0:
            read_options.set_readahead_size(readahead_mb * 1024 * 1024)
        self.read_options = read_options

        # init endpoint with db_path
        self.endpoint = norm_path(self.db_path)

    def _remove_options_files(self):
        """Remove OPTIONS files to prevent rocksdict from auto-loading old configs.

        rocksdict has a bug where it ignores user-provided BlockBasedOptions
        when reopening an existing database. It internally calls Options.load_latest()
        which uses a default 8MB cache instead of the user-specified cache size.

        By removing OPTIONS files before opening, we force rocksdict to use
        our provided options including the correct block cache size.
        """
        if not self.db_path.exists():
            return
        for f in os.listdir(self.db_path):
            if f.startswith("OPTIONS"):
                os.remove(self.db_path / f)

    def _log_connect_info(self, status: str):
        """Log connection info separately from connect logic."""
        count = self.get_total_count()
        count_str = f"{count} keys"
        logger.okay(f"  * RocksDB: {brk(status)} {brk(count_str)}", self.indent)
        # Log system limits for debugging file descriptor issues
        soft_limit, _ = get_system_max_open_files()
        if soft_limit < 100000:
            logger.warn(
                f"  ! System ulimit ({soft_limit}) is low. "
                f"Run 'ulimit -n 1048576' for better performance.",
                self.indent,
            )

    def connect(self):
        self.msgr.log_endpoint()
        self.msgr.log_now()
        self.msgr.log_msg()
        if not Path(self.db_path).exists():
            status = "Created"
        else:
            status = "Opened"
            # NOTE: Remove OPTIONS files to ensure block cache config is used
            self._remove_options_files()
        self.db = Rdict(path=str(self.db_path.resolve()), options=self.db_options)
        self.db.set_write_options(self.write_options)
        self.db.set_read_options(self.read_options)
        if self.verbose:
            self._log_connect_info(status)

    def get_total_count(self) -> int:
        """- https://rocksdict.github.io/RocksDict/rocksdict.html#Rdict.property_int_value
        - https://github.com/facebook/rocksdb/blob/10.4.fb/include/rocksdb/db.h#L1445"""
        return self.db.property_int_value("rocksdb.estimate-num-keys")

    def get_stats(self) -> dict:
        """Get database statistics for performance monitoring.

        Returns:
            dict with various performance metrics including:
            - estimate_num_keys: Approximate number of keys
            - estimate_live_data_size: Estimated size of live data
            - block_cache_usage: Current block cache usage
        """
        stats = {
            "estimate_num_keys": self.db.property_int_value(
                "rocksdb.estimate-num-keys"
            ),
            "estimate_live_data_size": self.db.property_int_value(
                "rocksdb.estimate-live-data-size"
            ),
            "cur_size_all_mem_tables": self.db.property_int_value(
                "rocksdb.cur-size-all-mem-tables"
            ),
            "block_cache_usage": self.db.property_int_value(
                "rocksdb.block-cache-usage"
            ),
            "block_cache_pinned_usage": self.db.property_int_value(
                "rocksdb.block-cache-pinned-usage"
            ),
        }

        return stats

    def log_stats(self):
        """Log database statistics for debugging and monitoring."""
        stats = self.get_stats()
        logger.note(f"  * RocksDB Stats:", self.indent)
        logger.mesg(f"    - Keys: {stats['estimate_num_keys']:,}", self.indent)
        logger.mesg(
            f"    - Live data: {stats['estimate_live_data_size'] / (1024*1024):.1f} MB",
            self.indent,
        )
        logger.mesg(
            f"    - Block cache: {stats['block_cache_usage'] / (1024*1024):.1f} MB",
            self.indent,
        )

    def get(self, key: Union[str, bytes]) -> Any:
        return self.db.get(key)

    def mget(self, keys: list[Union[str, bytes]]) -> list[Any]:
        """Separate this method only for readability, as `Rdict.get()` support list input natively"""
        return self.db.get(keys)

    def set(self, key: Union[str, bytes], value: Any):
        self.db.put(key, value)

    def _is_retryable_error(self, e: Exception) -> bool:
        """Check if an exception is a transient error worth retrying."""
        error_msg = str(e).lower()
        return "too many open files" in error_msg or "io error" in error_msg

    def _write_with_retry(self, wb: WriteBatch):
        """Write a batch with exponential backoff retry for transient errors."""
        import time

        last_error = None
        for attempt in range(self.max_retries):
            try:
                self.db.write(wb)
                return
            except Exception as e:
                if self._is_retryable_error(e) and attempt < self.max_retries - 1:
                    last_error = e
                    delay = self.retry_delay * (2**attempt)
                    if self.verbose:
                        logger.warn(
                            f"  ! RocksDB write failed (attempt {attempt + 1}/{self.max_retries}), "
                            f"retrying in {delay:.1f}s: {e}",
                            self.indent,
                        )
                    time.sleep(delay)
                    continue
                raise e
        if last_error:
            raise last_error

    def mset(self, d: Union[dict, list[tuple]]):
        """Set multiple key-value pairs at once with WriteBatch."""
        wb = WriteBatch(raw_mode=self.raw_mode)
        if isinstance(d, dict):
            for key, value in d.items():
                wb.put(key, value)
        elif isinstance(d, list):
            for item in d:
                key, value = item
                wb.put(key, value)
        else:
            raise ValueError("Input must be dict or list of (key, value) tuples")
        self._write_with_retry(wb)

    def flush(self, verbose: bool = False):
        self.db.flush()
        if verbose:
            status = "Flushed"
            logger.file(f"  * RocksDB: {brk(status)}", self.indent)

    def close(self, verbose: bool = False):
        self.db.close()
        if verbose:
            status = "Closed"
            logger.warn(f"  - RocksDB: {brk(status)}", self.indent)

    def __del__(self):
        try:
            self.flush()
            self.close()
        except Exception as e:
            pass

    def _iter(
        self,
        iter_type: Literal["keys", "vals", "items"] = "keys",
        pattern: str = None,
        max_count: int = None,
        batch_size: int = None,
    ) -> Generator[Union[KeysType, list[Any], list[tuple[KeyType, Any]]], None, None]:
        """Core iteration method for keys, values, or items."""
        if pattern:
            regex = re.compile(pattern)
        else:
            regex = None

        all_count = self.get_total_count()
        total_count = max_count or all_count
        batch_size = batch_size or 1000

        logger.note(
            f"> Iter rocks: {logstr.mesg(brp(iter_type))}: "
            f"{logstr.file(brk(total_count))}"
        )
        bar = TCLogbar(total=total_count, desc="* ")

        if iter_type == "keys":
            iterator = self.db.keys()
        else:
            iterator = self.db.items()

        yield_batch = []
        scanned_count = 0
        for item in iterator:
            if scanned_count >= total_count:
                break

            if iter_type == "keys":
                key, val = item, None
            else:
                key, val = item

            if isinstance(key, bytes):
                key_str = key.decode("utf-8")
            else:
                key_str = str(key)

            if regex and not regex.match(key_str):
                continue

            scanned_count += 1

            if iter_type == "keys":
                yield_batch.append(key)
            elif iter_type == "vals":
                yield_batch.append(val)
            else:
                yield_batch.append((key, val))

            bar.update(1, desc=f"  * {key_str}")

            if len(yield_batch) >= batch_size:
                yield yield_batch
                yield_batch = []

        if yield_batch:
            yield yield_batch
            yield_batch = []
        print()

    def iter_keys(
        self,
        pattern: str = None,
        max_count: int = None,
        batch_size: int = None,
    ) -> Generator[KeysType, None, None]:
        """yield list of keys"""
        return self._iter("keys", pattern, max_count, batch_size)

    def iter_vals(
        self,
        pattern: str = None,
        max_count: int = None,
        batch_size: int = None,
    ) -> Generator[list[Any], None, None]:
        """yield list of values"""
        return self._iter("vals", pattern, max_count, batch_size)

    def iter_items(
        self,
        pattern: str = None,
        max_count: int = None,
        batch_size: int = None,
    ) -> Generator[list[tuple[KeyType, Any]], None, None]:
        """yield list of tuples: (key, value)"""
        return self._iter("items", pattern, max_count, batch_size)
