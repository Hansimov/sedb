import redis
import threading

from copy import deepcopy
from tclogger import TCLogger, FileLogger, PathType
from typing import TypedDict

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
