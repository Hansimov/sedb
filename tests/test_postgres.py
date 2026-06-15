import importlib
import sys
import types

from sedb.postgres import normalize_postgres_configs, postgres_dsn


def test_postgres_dsn_hides_password_when_requested():
    dsn = postgres_dsn(
        {
            "host": "db.local",
            "port": 15432,
            "dbname": "blbl_account",
            "user": "account user",
            "password": "test-secret/pass",
            "application_name": "account app",
        },
        hide_password=True,
    )

    assert "account%20user" in dsn
    assert ":***@" in dsn
    assert "secret" not in dsn
    assert "application_name=account%20app" in dsn


def test_normalize_postgres_configs_applies_defaults():
    configs = normalize_postgres_configs({"port": "15432", "dbname": "app"})

    assert configs["host"] == "127.0.0.1"
    assert configs["port"] == 15432
    assert configs["dbname"] == "app"
    assert configs["sslmode"] == "prefer"


def test_import_sedb_does_not_eagerly_import_psycopg2():
    for name in list(sys.modules):
        if name == "sedb" or name.startswith("sedb."):
            sys.modules.pop(name, None)
    sys.modules.pop("psycopg2", None)

    sedb = importlib.import_module("sedb")

    assert sedb is not None
    assert "psycopg2" not in sys.modules


def test_postgres_operator_uses_injected_psycopg2(monkeypatch):
    calls = {}

    class FakeCursor:
        rowcount = 1

        def execute(self, query, params=None):
            calls["query"] = query
            calls["params"] = params

        def fetchone(self):
            return {"ok": 1}

        def fetchall(self):
            return [{"value": 1}, {"value": 2}]

        def close(self):
            calls["cursor_closed"] = True

    class FakeConnection:
        def cursor(self, cursor_factory=None):
            calls["cursor_factory"] = cursor_factory
            return FakeCursor()

        def commit(self):
            calls["committed"] = True

        def rollback(self):
            calls["rolled_back"] = True

        def close(self):
            calls["closed"] = True

    def fake_connect(**kwargs):
        calls["connect"] = kwargs
        return FakeConnection()

    fake_psycopg2 = types.ModuleType("psycopg2")
    fake_psycopg2.connect = fake_connect
    fake_extras = types.ModuleType("psycopg2.extras")
    fake_extras.RealDictCursor = object

    monkeypatch.setitem(sys.modules, "psycopg2", fake_psycopg2)
    monkeypatch.setitem(sys.modules, "psycopg2.extras", fake_extras)

    from sedb.postgres import PostgresOperator

    operator = PostgresOperator({"port": 15432}, verbose=False)

    assert operator.ping() is True
    assert operator.fetch_all("SELECT value FROM demo") == [{"value": 1}, {"value": 2}]
    assert calls["connect"]["port"] == 15432
    assert calls["cursor_closed"] is True
