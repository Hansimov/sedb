import threading

from pathlib import Path
from pymilvus import MilvusClient, DataType
from tclogger import TCLogger, logstr, FileLogger
from tclogger import get_now_str, ts_to_str, str_to_ts, dict_to_str
from typing import Literal, Union, TypedDict


logger = TCLogger()

# pymilvus/client/types.py
# https://milvus.io/api-reference/pymilvus/v2.5.x/MilvusClient/Collections/DataType.md
MILVUS_DTYPES = {
    0: "NONE",
    1: "BOOL",
    2: "INT8",
    3: "INT16",
    4: "INT32",
    5: "INT64",
    10: "FLOAT",
    11: "DOUBLE",
    20: "STRING",
    21: "VARCHAR",
    22: "ARRAY",
    23: "JSON",
    100: "BINARY_VECTOR",
    101: "FLOAT_VECTOR",
    102: "FLOAT16_VECTOR",
    103: "BFLOAT16_VECTOR",
    104: "SPARSE_FLOAT_VECTOR",
    999: "UNKNOWN",
}


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

    def log_error(self, docs: list = None, e: Exception = None):
        error_info = {"datetime": get_now_str(), "doc": docs, "error": repr(e)}
        if self.verbose:
            logger.err(f"× Milvus Error: {logstr.warn(error_info)}")
        if self.file_logger:
            error_str = dict_to_str(error_info, is_colored=False)
            self.file_logger.log(error_str, "error")

    def get_collection_info(self, collection: str) -> dict:
        return self.client.describe_collection(collection)

    def get_collection_fields_info(self, collection: str) -> list[str]:
        collection_info = self.get_collection_info(collection)
        fields_info = collection_info["fields"]
        reduced_fields_info = {}
        for field_info in fields_info:
            field_name = field_info["name"]
            field_dtype_int = field_info["type"]
            field_dtype_str = MILVUS_DTYPES[field_dtype_int]
            field_params = field_info["params"]
            reduced_fields_info[field_name] = {
                "dtype_int": field_dtype_int,
                "dtype_str": field_dtype_str,
                "params": field_params,
            }
        return reduced_fields_info

    def get_db_info(self) -> dict:
        server_version = self.client.get_server_version()
        collections = self.client.list_collections()
        collections_indexes = {}
        collections_dtypes = {}
        if collections:
            for collection in collections:
                collections_dtypes[collection] = self.get_collection_fields_info(
                    collection
                )
                collections_indexes[collection] = self.client.list_indexes(collection)
        users = self.client.list_users()
        db_info = {
            "dbname": self.dbname,
            "collections": collections,
            "fields": collections_dtypes,
            "indexes": collections_indexes,
            "users": users,
            "version": server_version,
        }
        return db_info

    def get_expr_of_list_contain(
        self, field: str, values: list[Union[str, int]]
    ) -> str:
        if isinstance(values[0], str):
            value_strs = [f"'{value}'" for value in values]
        else:
            value_strs = [str(value) for value in values]
        return f"{field} in [{','.join(value_strs)}]"

    def get_expr_of_any_field_false(self, fields: list[str], default_value=None) -> str:
        field_exprs = []
        for field in fields:
            if default_value is not None:
                comp_expr = f" == {default_value}"
            else:
                field_dtype_str = self.get_collection_fields_info(field)["dtype_str"]
                if field_dtype_str == "BOOL":
                    comp_expr = " == FALSE"
                elif field_dtype_str.lower().startswith("int"):
                    comp_expr = " == 0"
                elif field_dtype_str.lower().startswith("float"):
                    comp_expr = " == 0.0"
                elif field_dtype_str.lower() in ["string", "varchar"]:
                    comp_expr = " == ''"
                # elif field_dtype_str.lower() in ["array"]:
                #     check_expr = " == []"
                # elif field_dtype_str.lower() in ["json"]:
                #     check_expr = " == {}"
                else:
                    comp_expr = " == NULL"
            field_exprs.append(f"{field}{comp_expr}")
        return " || ".join(field_exprs)
