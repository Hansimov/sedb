from rocksdict import Rdict, Options, WriteOptions

from pathlib import Path
from tclogger import logger, logstr, get_now_str, brk
from typing import TypedDict, Union


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
        indent: int = 0,
        raw_mode: bool = False,
        verbose: bool = True,
    ):
        self.configs = configs
        self.connect_at_init = connect_at_init
        self.connect_msg = connect_msg
        self.indent = indent
        self.raw_mode = raw_mode
        self.verbose = verbose
        self.init_configs()
        if self.connect_at_init:
            self.connect(connect_msg=connect_msg)

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

    def connect(self, connect_msg: str = None):
        db_str = logstr.mesg(brk(self.db_path))
        if self.verbose:
            logger.note(f"> Connecting to: {db_str}")
            logger.file(f"  * {get_now_str()}")
            connect_msg = connect_msg or self.connect_msg
            if connect_msg:
                logger.file(f"  * {connect_msg}")
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
                logger.okay(f"  + RocksDB: {brk(status)} {brk(count_str)}", self.indent)
        except Exception as e:
            raise e

    def get_total_count(self) -> int:
        """- https://rocksdict.github.io/RocksDict/rocksdict.html#Rdict.property_int_value
        - https://github.com/facebook/rocksdb/blob/10.4.fb/include/rocksdb/db.h#L1445"""
        return self.db.property_int_value("rocksdb.estimate-num-keys")

    def flush(self):
        self.db.flush()
        status = "Flushed"
        if self.verbose:
            logger.file(f"  * RocksDB: {brk(status)}", self.indent)

    def close(self):
        self.db.close()
        status = "Closed"
        if self.verbose:
            logger.warn(f"  - RocksDB: {brk(status)}", self.indent)

    def __del__(self):
        try:
            self.flush()
            self.close()
        except Exception as e:
            pass
