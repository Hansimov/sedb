import redis
import threading

from copy import deepcopy
from tclogger import TCLogger, FileLogger, PathType
from typing import TypedDict, Union

from .message import ConnectMessager

logger = TCLogger()


class RedisConfigsType(TypedDict):
    host: str
    port: int
    db: int
    username: str
    password: str


DEFAULT_REDIS_CONFIGS = {
    "host": "localhost",
    "port": 6379,
    "db": 0,
    "username": "default",
    "password": "defaultpass",
}


class RedisOperator:
    """redis/redis-py: Redis Python client
    * https://github.com/redis/redis-py
    """

    def __init__(
        self,
        configs: RedisConfigsType,
        connect_at_init: bool = True,
        connect_msg: str = None,
        connect_cls: type = None,
        lock: threading.Lock = None,
        log_path: PathType = None,
        verbose: bool = True,
        indent: int = 0,
    ):
        self.configs = deepcopy(DEFAULT_REDIS_CONFIGS)
        self.configs.update(configs)
        self.connect_at_init = connect_at_init
        self.connect_msg = connect_msg
        self.verbose = verbose
        self.indent = indent
        self.init_configs()
        self.msgr = ConnectMessager(
            msg=connect_msg,
            cls=connect_cls,
            opr=self,
            dbt="redis",
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
        self.db = self.configs["db"]
        self.username = self.configs["username"]
        self.password = self.configs["password"]
        self.dbname = f"db{self.db}"
        self.endpoint = f"redis://{self.host}:{self.port}/{self.db}"

    def connect(self):
        self.msgr.log_endpoint()
        self.msgr.log_now()
        self.msgr.log_msg()
        self.client = redis.Redis(
            host=self.host,
            port=self.port,
            db=self.db,
            username=self.username,
            password=self.password,
        )
        try:
            self.client.ping()
            self.msgr.log_dbname()
        except Exception as e:
            raise e

    def key_to_name_field(
        self, key: str, is_hash: bool = True, sep: str = ":"
    ) -> tuple[str, Union[str, None]]:
        """Convert key to hash_name and hash_field"""
        if not key or not is_hash:
            return key, None
        if sep in key:
            hash_name, hash_field = key.split(sep, 1)
            return hash_name, hash_field
        else:
            return key, None

    def is_key_exist(self, key: str) -> bool:
        if not key:
            return False
        return bool(self.client.exists(key))

    def is_hash_exist(self, name_field: tuple[str, str]) -> bool:
        if not name_field:
            return False
        return bool(self.client.hexists(name_field[0], name_field[1]))

    def is_keys_exist(self, keys: list[str]) -> list[bool]:
        if not keys:
            return []
        pipeline = self.client.pipeline()
        for key in keys:
            pipeline.exists(key)
        results = pipeline.execute()
        return [bool(result) for result in results]

    def is_hashes_exist(self, name_fields: list[tuple[str, str]]) -> list[bool]:
        if not name_fields:
            return []
        pipeline = self.client.pipeline()
        for name, field in name_fields:
            pipeline.hexists(name, field)
        results = pipeline.execute()
        return [bool(result) for result in results]

    def set_key_exist(self, key: str):
        if not key:
            return
        self.client.set(key, 1)

    def set_hash_exist(self, name_field: tuple[str, str]):
        if not name_field:
            return
        self.client.hset(name_field[0], name_field[1], 1)

    def set_keys_exist(self, keys: list[str]):
        if not keys:
            return
        pipeline = self.client.pipeline()
        for key in keys:
            pipeline.set(key, 1)
        pipeline.execute()

    def set_hashes_exist(self, name_fields: list[tuple[str, str]]):
        if not name_fields:
            return
        pipeline = self.client.pipeline()
        for name, field in name_fields:
            pipeline.hset(name, field, 1)
        pipeline.execute()

    def get_exist_keys(self, keys: list[str]) -> list[str]:
        results = self.is_keys_exist(keys)
        exist_keys = [key for key, is_exist in zip(keys, results) if is_exist]
        return exist_keys

    def get_exist_hashes(
        self, name_fields: list[tuple[str, str]]
    ) -> list[tuple[str, str]]:
        results = self.is_hashes_exist(name_fields)
        exist_name_fields = [
            name_field for name_field, is_exist in zip(name_fields, results) if is_exist
        ]
        return exist_name_fields

    def get_non_exist_keys(self, keys: list[str]) -> list[str]:
        results = self.is_keys_exist(keys)
        non_exist_keys = [key for key, is_exist in zip(keys, results) if not is_exist]
        return non_exist_keys

    def get_non_exist_hashes(
        self, name_fields: list[tuple[str, str]]
    ) -> list[tuple[str, str]]:
        results = self.is_hashes_exist(name_fields)
        non_exist_name_fields = [
            name_field
            for name_field, is_exist in zip(name_fields, results)
            if not is_exist
        ]
        return non_exist_name_fields
