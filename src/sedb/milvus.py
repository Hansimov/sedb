import threading

from pathlib import Path
from pymilvus import MilvusClient
from tclogger import TCLogger, logstr, FileLogger
from tclogger import get_now_str, ts_to_str, str_to_ts, dict_to_str
from typing import Literal, Union, TypedDict


logger = TCLogger()


class MilvusConfigsType(TypedDict):
    host: str
    port: int
    dbname: str


class MilvusOperator:
    def __init__(
        self,
        configs: MilvusConfigsType,
        connect_at_init: bool = True,
        connect_msg: str = None,
        lock: threading.Lock = None,
        log_path: Union[str, Path] = None,
        verbose: bool = True,
        indent: int = 0,
    ):
        self.configs = configs
        self.verbose = verbose
        self.indent = indent
        logger.indent(self.indent)
        self.init_configs()
        self.connect_at_init = connect_at_init
        self.connect_msg = connect_msg
        self.lock = lock or threading.Lock()
        if log_path:
            self.file_logger = FileLogger(log_path)
        else:
            self.file_logger = None
        if self.connect_at_init:
            self.connect(connect_msg=connect_msg)

    def init_configs(self):
        self.host = self.configs["host"]
        self.port = self.configs["port"]
        self.dbname = self.configs["dbname"]
        self.endpoint = f"http://{self.host}:{self.port}"

    def connect(self, connect_msg: str = None):
        connect_msg = connect_msg or self.connect_msg
        if self.verbose:
            logger.note(f"> Connecting to: {logstr.mesg('['+self.endpoint+']')}")
            logger.file(f"  * {get_now_str()}")
            if connect_msg:
                logger.file(f"  * {connect_msg}")
        try:
            self.client = MilvusClient(uri=self.endpoint, db_name=self.dbname)
            logger.file(
                f"  * database: {logstr.success(self.dbname)}", verbose=self.verbose
            )
        except Exception as e:
            raise e

    def get_db_info(self) -> dict:
        server_version = self.client.get_server_version()
        collections = self.client.list_collections()
        if collections:
            collections_indexes = {}
            for collection in collections:
                collections_indexes[collection] = self.client.list_indexes(collection)
        else:
            collections_indexes = {}
        users = self.client.list_users()
        db_info = {
            "dbname": self.dbname,
            "collections": collections,
            "indexes": collections_indexes,
            "users": users,
            "version": server_version,
        }
        return db_info

    def log_error(self, docs: list = None, e: Exception = None):
        error_info = {"datetime": get_now_str(), "doc": docs, "error": repr(e)}
        if self.verbose:
            logger.err(f"× Milvus Error: {logstr.warn(error_info)}")
        if self.file_logger:
            error_str = dict_to_str(error_info, is_colored=False)
            self.file_logger.log(error_str, "error")
