import rocksdbpy

from pathlib import Path
from tclogger import logger, logstr, get_now_str, brk
from typing import TypedDict, Union


class RocksConfigsType(TypedDict):
    db_path: Union[str, Path]


class RocksOperator:
    def __init__(
        self,
        configs: RocksConfigsType,
        connect_at_init: bool = True,
        connect_msg: str = None,
        indent: int = 0,
        verbose: bool = True,
    ):
        self.configs = configs
        self.connect_at_init = connect_at_init
        self.connect_msg = connect_msg
        self.indent = indent
        self.verbose = verbose
        self.init_configs()
        if self.connect_at_init:
            self.connect(connect_msg=connect_msg)

    def init_configs(self):
        self.db_path = Path(self.configs["db_path"])

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
            self.db = rocksdbpy.open_default(str(self.db_path.resolve()))
            if self.verbose:
                logger.okay(f"  * RocksDB: {brk(status)}", self.indent)
        except Exception as e:
            raise e

    def close(self):
        self.db.close()
        status = "Closed"
        if self.verbose:
            logger.warn(f"  * RocksDB: {brk(status)}", self.indent)

    def __del__(self):
        self.close()
