import threading

from pathlib import Path
from qdrant_client import QdrantClient
from tclogger import TCLogger, logstr, FileLogger
from tclogger import get_now_str, ts_to_str, str_to_ts, dict_to_str
from typing import Literal, Union, TypedDict


logger = TCLogger()


class QdrantConfigsType(TypedDict):
    host: str
    port: int
    dbname: str


class QdrantOperator:
    def __init__(
        self,
        configs: QdrantConfigsType,
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
            self.client = QdrantClient(url=self.endpoint)
            logger.file(
                f"  * database: {logstr.success(self.dbname)}", verbose=self.verbose
            )
        except Exception as e:
            raise e

    def get_db_info(self) -> dict:
        collection_descs = self.client.get_collections().collections
        collection_infos = []
        for desc in collection_descs:
            dbname, collection_name = desc.name.split(".", maxsplit=1)
            if dbname == self.dbname:
                collection_info = {
                    "dbname": self.dbname,
                    "name": collection_name,
                }
                collection_infos.append(collection_info)
        db_info = {
            "dbname": self.dbname,
            "collections": collection_infos,
        }
        return db_info

    def get_db_collection_name(self, collection_nmae: str) -> str:
        return f"{self.dbname}.{collection_nmae}"
