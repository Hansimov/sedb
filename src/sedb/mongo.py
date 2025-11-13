import pymongo
import threading

from tclogger import TCLogger, logstr, FileLogger, PathType
from tclogger import get_now_str, ts_to_str, str_to_ts, dict_to_str

from .mongo_types import MongoConfigsType, DATE_FIELDS
from .mongo_types import FilterOpType, FilterRangeType, FilterIndexType, SortOrderType
from .mongo_filter import filter_params_to_mongo_filter, update_mongo_filter
from .mongo_pipeline import to_mongo_projection
from .message import ConnectMessager

logger = TCLogger()


class MongoOperator:
    def __init__(
        self,
        configs: MongoConfigsType,
        connect_at_init: bool = True,
        connect_msg: str = None,
        connect_cls: type = None,
        lock: threading.Lock = None,
        log_path: PathType = None,
        verbose: bool = True,
        verbose_args: bool = True,
        indent: int = 0,
    ):
        self.configs = configs
        self.connect_at_init = connect_at_init
        self.connect_msg = connect_msg
        self.verbose = verbose
        self.verbose_args = verbose_args
        self.indent = indent
        self.init_configs()
        self.date_fields = DATE_FIELDS
        self.msgr = ConnectMessager(
            msg=connect_msg,
            cls=connect_cls,
            opr=self,
            dbt="mongo",
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
        self.host = self.configs["host"]
        self.port = self.configs["port"]
        self.dbname = self.configs["dbname"]
        self.endpoint = f"mongodb://{self.host}:{self.port}"

    def connect(self):
        self.msgr.log_endpoint()
        self.msgr.log_now()
        self.msgr.log_msg()
        self.client = pymongo.MongoClient(self.endpoint)
        try:
            self.db = self.client[self.dbname]
            self.msgr.log_dbname()
        except Exception as e:
            raise e

    def log_error(self, docs: list = None, e: Exception = None):
        error_info = {"datetime": get_now_str(), "doc": docs, "error": repr(e)}
        if self.verbose:
            logger.err(f"× Mongo Error: {logstr.warn(error_info)}")
        if self.file_logger:
            error_str = dict_to_str(error_info, is_colored=False)
            self.file_logger.log(error_str, "error")

    def log_args(self, args_dict: dict):
        filter_index = args_dict["filter_index"]
        filter_range = args_dict["filter_range"]
        if filter_index and filter_index.lower() in self.date_fields:
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
        filter_index: FilterIndexType = "insert_at",
        filter_op: FilterOpType = "gte",
        filter_range: FilterRangeType = None,
        extra_filters: list[dict] = None,
        estimate_count: bool = False,
    ) -> int:
        logger.note(f"> Counting docs:", end=" ", verbose=self.verbose)
        db_collect = self.db[collection]
        filter_params = {
            "filter_index": filter_index,
            "filter_op": filter_op,
            "filter_range": filter_range,
            "date_fields": self.date_fields,
        }

        if filter_range is None or estimate_count:
            total_count = db_collect.estimated_document_count()
            logger.success(
                f"[{total_count}] {logstr.file('(estimated)')}", verbose=self.verbose
            )
        else:
            filter_dict = filter_params_to_mongo_filter(**filter_params)
            if extra_filters:
                filter_dict = update_mongo_filter(
                    filter_dict, extra_filters=extra_filters
                )
            total_count = db_collect.count_documents(filter_dict)
            logger.success(f"[{total_count}]", verbose=self.verbose)

        return total_count

    def get_cursor(
        self,
        collection: str,
        filter_index: str = None,
        filter_op: FilterOpType = "gte",
        filter_range: FilterRangeType = None,
        include_fields: list[str] = None,
        exclude_fields: list[str] = None,
        sort_index: str = None,
        sort_order: SortOrderType = "asc",
        skip_count: int = None,
        extra_filters: list[dict] = None,
        is_date_field: bool = None,
        no_cursor_timeout: bool = False,
    ):
        filter_dict = filter_params_to_mongo_filter(
            filter_index=filter_index,
            filter_op=filter_op,
            filter_range=filter_range,
            is_date_field=is_date_field,
        )
        if extra_filters:
            filter_dict = update_mongo_filter(filter_dict, extra_filters=extra_filters)
        projection = to_mongo_projection(
            include_fields=include_fields, exclude_fields=exclude_fields
        )
        if self.verbose_args:
            args_dict = {
                "collection": collection,
                "filter_index": filter_index,
                "filter_op": filter_op,
                "filter_range": filter_range,
                "sort_index": sort_index,
                "sort_order": sort_order,
                "filter_dict": filter_dict,
                "skip_count": skip_count,
                "extra_filters": extra_filters,
                "include_fields": include_fields,
                "exclude_fields": exclude_fields,
                "no_cursor_timeout": no_cursor_timeout,
            }
            self.log_args(args_dict)

        cursor = self.db[collection].find(
            filter_dict, projection=projection, no_cursor_timeout=no_cursor_timeout
        )

        if sort_index:
            if sort_order and sort_order.lower().startswith("desc"):
                order = pymongo.DESCENDING
            else:
                order = pymongo.ASCENDING
            cursor = cursor.sort(sort_index, order)
        if skip_count:
            cursor = cursor.skip(skip_count)

        return cursor

    def get_agg_cursor(
        self, collection: str, pipeline: list[dict], batch_size: int = 10000
    ):
        return self.db[collection].aggregate(pipeline, batchSize=batch_size)

    def get_docs(
        self,
        collection: str,
        ids: list[str],
        id_field: str,
        include_fields: list[str] = None,
        exclude_fields: list[str] = None,
    ) -> list[dict]:
        if not isinstance(ids, list):
            id_filter = {id_field: ids}
        else:
            id_filter = {id_field: {"$in": ids}}
        projection = to_mongo_projection(
            include_fields=include_fields, exclude_fields=exclude_fields
        )
        cursor = self.db[collection].find(filter=id_filter, projection=projection)
        return list(cursor)
