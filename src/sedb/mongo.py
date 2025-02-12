import pymongo
import threading

from pathlib import Path
from tclogger import TCLogger, logstr, FileLogger
from tclogger import get_now_str, ts_to_str, str_to_ts, dict_to_str
from typing import Literal, Union, TypedDict

from .filters import to_mongo_filter

logger = TCLogger()


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

    def log_args(
        self,
        args_dict: dict,
        date_fields: list[str] = ["pubdate", "insert_at", "index_at"],
    ):
        filter_index = args_dict["filter_index"]
        filter_range = args_dict["filter_range"]
        if filter_index and filter_index.lower() in date_fields:
            if isinstance(filter_range, (tuple, list)):
                filter_range_ts = [
                    str_to_ts(i) if isinstance(i, str) else i for i in filter_range
                ]
                filter_range_str = [
                    ts_to_str(i) if isinstance(i, int) else i for i in filter_range
                ]
            elif isinstance(filter_range, int):
                filter_range_ts = filter_range
                filter_range_str = ts_to_str(filter_range)
            elif isinstance(filter_range, str):
                filter_range_ts = str_to_ts(filter_range)
                filter_range_str = filter_range
            else:
                filter_range_ts = filter_range
                filter_range_str = filter_range
            args_dict["filter_range_ts"] = filter_range_ts
            args_dict["filter_range_str"] = filter_range_str
        logger.note(f"> Getting cursor with args:")
        logger.mesg(dict_to_str(args_dict), indent=logger.log_indent + 2)

    def get_total_count(
        self,
        collection: str,
        filter_index: Literal["insert_at", "pubdate"] = "insert_at",
        filter_op: Literal["gt", "lt", "gte", "lte", "range"] = "gte",
        filter_range: Union[int, str, tuple, list] = None,
        estimate_count: bool = False,
    ) -> int:
        logger.note(f"> Counting docs:", end=" ", verbose=self.verbose)
        db_collect = self.db[collection]
        filter_params = {
            "filter_index": filter_index,
            "filter_op": filter_op,
            "filter_range": filter_range,
        }

        if filter_range is None or estimate_count:
            total_count = db_collect.estimated_document_count()
            logger.success(
                f"[{total_count}] {logstr.file('(estimated)')}", verbose=self.verbose
            )
        else:
            filter_dict = to_mongo_filter(**filter_params)
            total_count = db_collect.count_documents(filter_dict)
            logger.success(f"[{total_count}]", verbose=self.verbose)

        return total_count

    def get_cursor(
        self,
        collection: str,
        filter_index: str = None,
        filter_op: Literal["gt", "lt", "gte", "lte", "range"] = "gte",
        filter_range: Union[int, str, tuple, list] = None,
        sort_index: str = None,
        sort_order: Literal["asc", "desc"] = "asc",
        is_date_index: bool = None,
    ):
        filter_dict = to_mongo_filter(
            filter_index=filter_index,
            filter_op=filter_op,
            filter_range=filter_range,
            is_date_index=is_date_index,
        )
        if self.verbose:
            args_dict = {
                "collection": collection,
                "filter_index": filter_index,
                "filter_op": filter_op,
                "filter_range": filter_range,
                "sort_index": sort_index,
                "sort_order": sort_order,
                "filter_dict": filter_dict,
            }
            self.log_args(args_dict)

        cursor = self.db[collection].find(filter_dict)

        if sort_index:
            if sort_order and sort_order.lower().startswith("desc"):
                order = pymongo.DESCENDING
            else:
                order = pymongo.ASCENDING
            cursor = cursor.sort(sort_index, order)

        return cursor
