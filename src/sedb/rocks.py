import threading

from rocksdict import Rdict, Options, WriteOptions, WriteBatch
from pathlib import Path
from tclogger import FileLogger, TCLogger, brk
from tclogger import PathType, norm_path
from typing import TypedDict, Union, Any

from .message import ConnectMessager

logger = TCLogger()


class RocksConfigsType(TypedDict):
    db_path: Union[str, Path]
    max_open_files: int = 20000
    target_file_size_base_mb: int = 64
    write_buffer_size_mb: int = 64
    level_zero_slowdown_writes_trigger: int = 20000
    level_zero_stop_writes_trigger: int = 50000


class RocksOperator:
    """rocksdict API documentation
    * https://rocksdict.github.io/RocksDict/rocksdict.html

    RocksDB include headers:
    * https://github.com/facebook/rocksdb/blob/10.4.fb/include/rocksdb/db.h

    Write Stalls · facebook/rocksdb Wiki
    * https://github.com/facebook/rocksdb/wiki/Write-Stalls

    NOTE: Run `ulimit -n 20000` to increase the max open files limit system-wide
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
        options.set_max_background_jobs(128)
        options.set_max_open_files(self.configs.get("max_open_files", 20000))
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
        self.db_options = options

        # init write options
        write_options = WriteOptions()
        write_options.no_slowdown = True
        self.write_options = write_options
        self.endpoint = norm_path(self.db_path)

    def connect(self):
        self.msgr.log_endpoint()
        self.msgr.log_now()
        self.msgr.log_msg()
        try:
            if not Path(self.db_path).exists():
                status = "Created"
            else:
                status = "Opened"
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

    def flush(self):
        self.db.flush()
        # if self.verbose:
        #     status = "Flushed"
        #     logger.file(f"  * RocksDB: {brk(status)}", self.indent)

    def close(self):
        self.db.close()
        # if self.verbose:
        #     status = "Closed"
        #     logger.warn(f"  - RocksDB: {brk(status)}", self.indent)

    def __del__(self):
        try:
            self.flush()
            self.close()
        except Exception as e:
            pass
