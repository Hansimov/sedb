import os
import re
import threading

from rocksdict import Rdict, Options, BlockBasedOptions, WriteOptions
from rocksdict import WriteBatch, Cache, DBCompressionType
from pathlib import Path
from tclogger import FileLogger, TCLogger, TCLogbar, logstr, brp, brk
from tclogger import PathType, norm_path
from tclogger import KeyType, KeysType
from typing import Generator, Literal, TypedDict, Union, Any

from .message import ConnectMessager

logger = TCLogger()


def calc_parallelism(max_value: int = 128) -> int:
    return min(os.cpu_count() or 8, max_value)


class RocksConfigsType(TypedDict):
    db_path: Union[str, Path]
    # options
    max_open_files: int = -1
    target_file_size_base_mb: int = 256
    write_buffer_size_mb: int = 256
    level_zero_slowdown_writes_trigger: int = 20000
    level_zero_stop_writes_trigger: int = 50000
    parallelism: int = calc_parallelism()
    # table options
    block_cache_size_mb: int = 8192  # 8 GB
    bits_per_key: int = 10
    block_based: bool = False


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

        # init db options
        options = Options(raw_mode=self.raw_mode)
        options.create_if_missing(True)
        options.set_max_file_opening_threads(128)
        options.increase_parallelism(
            self.configs.get("parallelism", calc_parallelism())
        )
        options.set_max_background_jobs(
            self.configs.get("parallelism", calc_parallelism())
        )
        # set "max_open_files" to -1 means manage file handles automatically,
        # which could resolve "Too many open files" issue,
        # NOTE: run cmd to increase limit: `ulimit -n 1048576`
        options.set_max_open_files(self.configs.get("max_open_files", -1))
        options.set_target_file_size_base(
            self.configs.get("target_file_size_base_mb", 64) * 1024 * 1024
        )
        options.set_write_buffer_size(
            self.configs.get("write_buffer_size_mb", 64) * 1024 * 1024
        )
        options.set_level_zero_slowdown_writes_trigger(
            self.configs.get("level_zero_slowdown_writes_trigger", 20000)
        )
        options.set_level_zero_stop_writes_trigger(
            self.configs.get("level_zero_stop_writes_trigger", 50000)
        )
        options.set_compression_type(DBCompressionType.lz4())

        # init table options
        table_options = BlockBasedOptions()
        table_options.set_block_cache(
            Cache(self.configs.get("block_cache_size_mb", 8192) * 1024 * 1024)
        )
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

    def connect(self):
        self.msgr.log_endpoint()
        self.msgr.log_now()
        self.msgr.log_msg()
        try:
            if not Path(self.db_path).exists():
                status = "Created"
            else:
                status = "Opened"
                # NOTE: Remove OPTIONS files to ensure block cache config is used
                self._remove_options_files()
            self.db = Rdict(path=str(self.db_path.resolve()), options=self.db_options)
            self.db.set_write_options(self.write_options)
            if self.verbose:
                count = self.get_total_count()
                count_str = f"{count} keys"
                logger.okay(f"  * RocksDB: {brk(status)} {brk(count_str)}", self.indent)
        except Exception as e:
            raise e

    def get_total_count(self) -> int:
        """- https://rocksdict.github.io/RocksDict/rocksdict.html#Rdict.property_int_value
        - https://github.com/facebook/rocksdb/blob/10.4.fb/include/rocksdb/db.h#L1445"""
        return self.db.property_int_value("rocksdb.estimate-num-keys")

    def get(self, key: Union[str, bytes]) -> Any:
        return self.db.get(key)

    def mget(self, keys: list[Union[str, bytes]]) -> list[Any]:
        """Separate this method only for readability, as `Rdict.get()` support list input natively"""
        return self.db.get(keys)

    def set(self, key: Union[str, bytes], value: Any):
        self.db.put(key, value)

    def mset(self, d: Union[dict, list[tuple]]):
        """Set multiple key-value pairs at once with WriteBatch"""
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
        self.db.write(wb)

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
