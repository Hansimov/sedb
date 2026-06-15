from __future__ import annotations

import threading

from contextlib import contextmanager
from copy import deepcopy
from typing import Any, Generator, Iterable, TypedDict
from urllib.parse import quote

from tclogger import TCLogger, FileLogger, PathType

from .message import ConnectMessager

logger = TCLogger()


class PostgresConfigsType(TypedDict, total=False):
    host: str
    port: int
    dbname: str
    user: str
    password: str
    sslmode: str
    connect_timeout: int
    application_name: str


DEFAULT_POSTGRES_CONFIGS: PostgresConfigsType = {
    "host": "127.0.0.1",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "",
    "sslmode": "prefer",
    "connect_timeout": 10,
    "application_name": "sedb",
}


def _import_psycopg2():
    try:
        import psycopg2
        from psycopg2.extras import RealDictCursor
    except ImportError as exc:  # pragma: no cover - depends on optional deps.
        raise ImportError(
            "PostgreSQL support requires psycopg2. "
            "Install it with `pip install sedb[postgres]` or `pip install psycopg2-binary`."
        ) from exc
    return psycopg2, RealDictCursor


def normalize_postgres_configs(
    configs: PostgresConfigsType | None = None,
) -> PostgresConfigsType:
    normalized = deepcopy(DEFAULT_POSTGRES_CONFIGS)
    if configs:
        normalized.update({key: value for key, value in configs.items() if value is not None})
    normalized["host"] = str(normalized.get("host") or "127.0.0.1").strip()
    normalized["port"] = int(normalized.get("port") or 5432)
    normalized["dbname"] = str(normalized.get("dbname") or "postgres").strip()
    normalized["user"] = str(normalized.get("user") or "postgres").strip()
    normalized["password"] = str(normalized.get("password") or "")
    normalized["sslmode"] = str(normalized.get("sslmode") or "prefer").strip()
    normalized["connect_timeout"] = int(normalized.get("connect_timeout") or 10)
    normalized["application_name"] = str(
        normalized.get("application_name") or "sedb"
    ).strip()
    return normalized


def postgres_dsn(
    configs: PostgresConfigsType | None = None,
    *,
    hide_password: bool = False,
) -> str:
    payload = normalize_postgres_configs(configs)
    user = quote(str(payload["user"]))
    password = str(payload.get("password") or "")
    password_part = ""
    if password:
        password_part = ":***" if hide_password else f":{quote(password)}"
    host = str(payload["host"])
    port = int(payload["port"])
    dbname = quote(str(payload["dbname"]))
    query_parts = []
    for key in ["sslmode", "connect_timeout", "application_name"]:
        value = str(payload.get(key) or "").strip()
        if value:
            query_parts.append(f"{key}={quote(value)}")
    query = f"?{'&'.join(query_parts)}" if query_parts else ""
    return f"postgresql://{user}{password_part}@{host}:{port}/{dbname}{query}"


class PostgresOperator:
    """Small psycopg2 wrapper for service code that needs explicit transactions."""

    def __init__(
        self,
        configs: PostgresConfigsType | None = None,
        connect_at_init: bool = True,
        connect_msg: str | None = None,
        connect_cls: type | None = None,
        lock: threading.Lock | None = None,
        log_path: PathType | None = None,
        verbose: bool = True,
        indent: int = 0,
    ):
        self.configs = normalize_postgres_configs(configs)
        self.connect_at_init = connect_at_init
        self.connect_msg = connect_msg
        self.verbose = verbose
        self.indent = indent
        self.lock = lock or threading.Lock()
        self.file_logger = FileLogger(log_path) if log_path else None
        self.conn = None
        self.cursor_factory = None
        self.init_configs()
        self.msgr = ConnectMessager(
            msg=connect_msg,
            cls=connect_cls,
            opr=self,
            dbt="postgres",
            verbose=verbose,
            indent=indent,
        )
        if self.connect_at_init:
            self.connect()

    def init_configs(self) -> None:
        self.host = str(self.configs["host"])
        self.port = int(self.configs["port"])
        self.dbname = str(self.configs["dbname"])
        self.user = str(self.configs["user"])
        self.password = str(self.configs.get("password") or "")
        self.endpoint = postgres_dsn(self.configs, hide_password=True)

    def connect(self):
        psycopg2, RealDictCursor = _import_psycopg2()
        self.cursor_factory = RealDictCursor
        self.msgr.log_endpoint()
        self.msgr.log_now()
        self.msgr.log_msg()
        self.conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            sslmode=str(self.configs.get("sslmode") or "prefer"),
            connect_timeout=int(self.configs.get("connect_timeout") or 10),
            application_name=str(self.configs.get("application_name") or "sedb"),
        )
        self.msgr.log_dbname()
        return self.conn

    def close(self) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def ensure_connected(self):
        if self.conn is None:
            return self.connect()
        return self.conn

    @contextmanager
    def transaction(self) -> Generator[Any, None, None]:
        conn = self.ensure_connected()
        with self.lock:
            try:
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    @contextmanager
    def cursor(self):
        conn = self.ensure_connected()
        cur = conn.cursor(cursor_factory=self.cursor_factory)
        try:
            yield cur
        finally:
            cur.close()

    def execute(
        self,
        query: str,
        params: Iterable[Any] | dict[str, Any] | None = None,
        *,
        commit: bool = False,
    ) -> int:
        with self.cursor() as cur:
            cur.execute(query, params)
            rowcount = int(cur.rowcount or 0)
        if commit:
            self.ensure_connected().commit()
        return rowcount

    def fetch_one(
        self,
        query: str,
        params: Iterable[Any] | dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        with self.cursor() as cur:
            cur.execute(query, params)
            row = cur.fetchone()
        return dict(row) if row is not None else None

    def fetch_all(
        self,
        query: str,
        params: Iterable[Any] | dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        with self.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
        return [dict(row) for row in rows]

    def ping(self) -> bool:
        row = self.fetch_one("SELECT 1 AS ok")
        return bool(row and row.get("ok") == 1)
