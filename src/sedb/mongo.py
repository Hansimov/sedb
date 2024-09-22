import pymongo
import threading

from pathlib import Path
from tclogger import logger, logstr, FileLogger
from tclogger import get_now_str, ts_to_str, dict_to_str
from typing import Literal, Union, TypedDict


class MongoConfigsType(TypedDict):
    host: str
    port: int
    dbname: str


class MongoOperator:
    def __init__(
        self,
        configs: MongoConfigsType,
        connect_at_init: bool = True,
        connect_msg: str = None,
        lock: threading.Lock = None,
        log_path: Union[str, Path] = None,
        verbose: bool = True,
    ):
        self.configs = configs
        self.verbose = verbose
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
        self.endpoint = f"mongodb://{self.host}:{self.port}"

    def connect(self, connect_msg: str = None):
        connect_msg = connect_msg or self.connect_msg
        if self.verbose:
            logger.note(f"> Connecting to: {logstr.mesg('['+self.endpoint+']')}")
            logger.file(f"  * {get_now_str()}")
            if connect_msg:
                logger.file(f"  * {connect_msg}")
        self.client = pymongo.MongoClient(self.endpoint)
        try:
            self.db = self.client[self.dbname]
            if self.verbose:
                logger.file(f"  * database: {logstr.success(self.dbname)}")
        except Exception as e:
            raise e

    def log_error(self, docs: list = None, e: Exception = None):
        error_info = {"datetime": get_now_str(), "doc": docs, "error": repr(e)}
        if self.verbose:
            logger.err(f"× Mongo Error: {logstr.warn(error_info)}")
        if self.file_logger:
            error_str = dict_to_str(error_info, is_colored=False)
            self.file_logger.log(error_str, "error")

    def get_cursor(
        self,
        collection: str,
        filter_index: str = None,
        filter_op: Literal["gt", "lt", "gte", "lte", "range"] = "gte",
        filter_range: Union[int, str, tuple, list] = None,
        sort_index: str = None,
        sort_order: Literal["asc", "desc"] = "asc",
        verbose: bool = False,
    ):
        if verbose:
            args_dict = {
                "collection": collection,
                "filter_index": filter_index,
                "filter_op": filter_op,
                "filter_range": filter_range,
                "sort_index": sort_index,
                "sort_order": sort_order,
            }
            if filter_index and filter_index.lower() in ["pubdate", "insert_at"]:
                if isinstance(filter_range, (tuple, list)):
                    filter_range_str = [ts_to_str(i) for i in filter_range]
                elif isinstance(filter_range, int):
                    filter_range_str = ts_to_str(filter_range)
                else:
                    filter_range_str = filter_range
                args_dict["filter_range_str"] = filter_range_str
            if self.verbose:
                logger.note(f"> Getting cursor with args:")
                logger.mesg(dict_to_str(args_dict), indent=2)

        filter = {}
        if filter_index:
            if filter_op == "range":
                if filter_range and isinstance(filter_range, (tuple, list)):
                    filter[filter_index] = {
                        "$gte": min(filter_range),
                        "$lte": max(filter_range),
                    }
                else:
                    raise ValueError(f"× Invalid filter_range: {filter_range}")
            elif filter_op in ["gt", "lt", "gte", "lte"]:
                filter[filter_index] = {f"${filter_op}": filter_range}
            else:
                raise ValueError(f"× Invalid filter_op: {filter_op}")

        cursor = self.db[collection].find(filter)

        if sort_index:
            if sort_order and sort_order.lower().startswith("desc"):
                order = pymongo.DESCENDING
            else:
                order = pymongo.ASCENDING
            cursor = cursor.sort(sort_index, order)

        return cursor
