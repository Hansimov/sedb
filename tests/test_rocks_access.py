"""Tests for RocksOperator multi-process access modes.

Tests cover:
- read_write mode (default behavior)
- read_only mode (concurrent read access)
- secondary mode (read with catch_up from primary)
- write guards in non-writable modes
- invalid access_type validation
"""

import os
import shutil
import subprocess
import sys
import tempfile

import pytest

from sedb.rocks import (
    RocksOperator,
    ACCESS_READ_WRITE,
    ACCESS_READ_ONLY,
    ACCESS_SECONDARY,
)


@pytest.fixture
def tmp_db_path(tmp_path):
    """Create a temporary directory for RocksDB."""
    db_path = tmp_path / "test.rkdb"
    yield db_path
    # Cleanup
    if db_path.exists():
        shutil.rmtree(db_path)


@pytest.fixture
def populated_db(tmp_db_path):
    """Create and populate a RocksDB with test data, then close it."""
    db = RocksOperator(
        configs={"db_path": str(tmp_db_path)},
        verbose=False,
    )
    db.mset({"key1": "value1", "key2": "value2", "key3": "value3"})
    db.flush()
    db.close()
    return tmp_db_path


class TestAccessTypeConfig:
    """Test access_type configuration handling."""

    def test_default_access_type_is_read_write(self, tmp_db_path):
        db = RocksOperator(
            configs={"db_path": str(tmp_db_path)},
            verbose=False,
        )
        assert db.access_type == ACCESS_READ_WRITE
        assert db.is_read_write is True
        assert db.is_read_only is False
        assert db.is_secondary is False
        assert db.is_writable is True
        db.close()

    def test_invalid_access_type_raises(self, tmp_db_path):
        with pytest.raises(ValueError, match="Invalid access_type"):
            RocksOperator(
                configs={"db_path": str(tmp_db_path), "access_type": "invalid"},
                verbose=False,
            )

    def test_read_only_not_writable(self, populated_db):
        db = RocksOperator(
            configs={"db_path": str(populated_db), "access_type": ACCESS_READ_ONLY},
            verbose=False,
        )
        assert db.access_type == ACCESS_READ_ONLY
        assert db.is_read_only is True
        assert db.is_read_write is False
        assert db.is_secondary is False
        assert db.is_writable is False
        db.close()

    def test_secondary_not_writable(self, populated_db):
        db = RocksOperator(
            configs={"db_path": str(populated_db), "access_type": ACCESS_SECONDARY},
            verbose=False,
        )
        assert db.access_type == ACCESS_SECONDARY
        assert db.is_secondary is True
        assert db.is_read_write is False
        assert db.is_read_only is False
        assert db.is_writable is False
        db.close()


class TestReadWriteMode:
    """Test default read_write mode."""

    def test_basic_read_write(self, tmp_db_path):
        db = RocksOperator(
            configs={"db_path": str(tmp_db_path)},
            verbose=False,
        )
        db.set("hello", "world")
        assert db.get("hello") == "world"
        db.close()

    def test_mset_and_mget(self, tmp_db_path):
        db = RocksOperator(
            configs={"db_path": str(tmp_db_path)},
            verbose=False,
        )
        db.mset({"a": 1, "b": 2, "c": 3})
        results = db.mget(["a", "b", "c"])
        assert results == [1, 2, 3]
        db.close()

    def test_creates_db_if_missing(self, tmp_db_path):
        assert not tmp_db_path.exists()
        db = RocksOperator(
            configs={"db_path": str(tmp_db_path)},
            verbose=False,
        )
        assert tmp_db_path.exists()
        db.close()


class TestReadOnlyMode:
    """Test read_only access mode."""

    def test_read_only_can_read(self, populated_db):
        db = RocksOperator(
            configs={"db_path": str(populated_db), "access_type": ACCESS_READ_ONLY},
            verbose=False,
        )
        assert db.get("key1") == "value1"
        assert db.get("key2") == "value2"
        db.close()

    def test_read_only_mget(self, populated_db):
        db = RocksOperator(
            configs={"db_path": str(populated_db), "access_type": ACCESS_READ_ONLY},
            verbose=False,
        )
        results = db.mget(["key1", "key2", "key3"])
        assert results == ["value1", "value2", "value3"]
        db.close()

    def test_read_only_set_raises(self, populated_db):
        db = RocksOperator(
            configs={"db_path": str(populated_db), "access_type": ACCESS_READ_ONLY},
            verbose=False,
        )
        with pytest.raises(RuntimeError, match="Cannot set in 'read_only' mode"):
            db.set("new_key", "new_value")
        db.close()

    def test_read_only_mset_raises(self, populated_db):
        db = RocksOperator(
            configs={"db_path": str(populated_db), "access_type": ACCESS_READ_ONLY},
            verbose=False,
        )
        with pytest.raises(RuntimeError, match="Cannot mset in 'read_only' mode"):
            db.mset({"new_key": "new_value"})
        db.close()

    def test_read_only_nonexistent_raises(self, tmp_db_path):
        with pytest.raises(FileNotFoundError, match="Cannot open non-existent DB"):
            RocksOperator(
                configs={
                    "db_path": str(tmp_db_path),
                    "access_type": ACCESS_READ_ONLY,
                },
                verbose=False,
            )

    def test_read_only_flush_is_noop(self, populated_db):
        """flush() should silently do nothing in read_only mode."""
        db = RocksOperator(
            configs={"db_path": str(populated_db), "access_type": ACCESS_READ_ONLY},
            verbose=False,
        )
        db.flush()  # Should not raise
        db.close()

    def test_multiple_read_only_instances(self, populated_db):
        """Multiple read_only instances can coexist on the same DB."""
        db1 = RocksOperator(
            configs={"db_path": str(populated_db), "access_type": ACCESS_READ_ONLY},
            verbose=False,
        )
        db2 = RocksOperator(
            configs={"db_path": str(populated_db), "access_type": ACCESS_READ_ONLY},
            verbose=False,
        )
        assert db1.get("key1") == "value1"
        assert db2.get("key1") == "value1"
        db1.close()
        db2.close()


class TestSecondaryMode:
    """Test secondary access mode."""

    def test_secondary_can_read(self, populated_db):
        db = RocksOperator(
            configs={"db_path": str(populated_db), "access_type": ACCESS_SECONDARY},
            verbose=False,
        )
        assert db.get("key1") == "value1"
        db.close()

    def test_secondary_set_raises(self, populated_db):
        db = RocksOperator(
            configs={"db_path": str(populated_db), "access_type": ACCESS_SECONDARY},
            verbose=False,
        )
        with pytest.raises(RuntimeError, match="Cannot set in 'secondary' mode"):
            db.set("new_key", "new_value")
        db.close()

    def test_secondary_auto_path(self, populated_db):
        db = RocksOperator(
            configs={"db_path": str(populated_db), "access_type": ACCESS_SECONDARY},
            verbose=False,
        )
        expected_suffix = f".secondary.{os.getpid()}"
        assert str(db.secondary_path).endswith(expected_suffix)
        db.close()

    def test_secondary_custom_path(self, populated_db, tmp_path):
        custom_path = tmp_path / "custom_secondary"
        db = RocksOperator(
            configs={
                "db_path": str(populated_db),
                "access_type": ACCESS_SECONDARY,
                "secondary_path": str(custom_path),
            },
            verbose=False,
        )
        assert db.secondary_path == custom_path
        db.close()

    def test_secondary_catch_up(self, populated_db):
        db = RocksOperator(
            configs={"db_path": str(populated_db), "access_type": ACCESS_SECONDARY},
            verbose=False,
        )
        db.catch_up()  # Should not raise
        db.close()

    def test_catch_up_wrong_mode_raises(self, populated_db):
        db = RocksOperator(
            configs={"db_path": str(populated_db), "access_type": ACCESS_READ_ONLY},
            verbose=False,
        )
        with pytest.raises(RuntimeError, match="only available in 'secondary' mode"):
            db.catch_up()
        db.close()

    def test_secondary_nonexistent_raises(self, tmp_db_path):
        with pytest.raises(FileNotFoundError, match="Cannot open non-existent DB"):
            RocksOperator(
                configs={
                    "db_path": str(tmp_db_path),
                    "access_type": ACCESS_SECONDARY,
                },
                verbose=False,
            )


class TestConcurrentAccess:
    """Test concurrent access patterns (same process, simulating multi-process)."""

    def test_read_write_with_read_only(self, tmp_db_path):
        """A read_write and read_only instance can coexist."""
        # Open primary in read_write
        primary = RocksOperator(
            configs={"db_path": str(tmp_db_path)},
            verbose=False,
        )
        primary.mset({"x": 10, "y": 20})
        primary.flush()

        # Open secondary in read_only
        reader = RocksOperator(
            configs={"db_path": str(tmp_db_path), "access_type": ACCESS_READ_ONLY},
            verbose=False,
        )
        assert reader.get("x") == 10
        assert reader.get("y") == 20

        reader.close()
        primary.close()

    def test_read_write_with_secondary(self, tmp_db_path):
        """A read_write and secondary instance can coexist, secondary sees updates via catch_up."""
        # Open primary in read_write
        primary = RocksOperator(
            configs={"db_path": str(tmp_db_path)},
            verbose=False,
        )
        primary.mset({"a": 1, "b": 2})
        primary.flush()

        # Open secondary
        secondary = RocksOperator(
            configs={"db_path": str(tmp_db_path), "access_type": ACCESS_SECONDARY},
            verbose=False,
        )
        # Should see initial data
        secondary.catch_up()
        assert secondary.get("a") == 1

        # Write more data from primary
        primary.mset({"c": 3, "d": 4})
        primary.flush()

        # Secondary catches up and sees new data
        secondary.catch_up()
        assert secondary.get("c") == 3
        assert secondary.get("d") == 4

        secondary.close()
        primary.close()

    def test_iteration_in_read_only(self, populated_db):
        """Iteration works in read_only mode."""
        db = RocksOperator(
            configs={"db_path": str(populated_db), "access_type": ACCESS_READ_ONLY},
            verbose=False,
        )
        all_keys = []
        for batch in db.iter_keys(batch_size=10):
            all_keys.extend(batch)
        assert set(all_keys) == {"key1", "key2", "key3"}
        db.close()


_READER_SCRIPT = """
import sys
from sedb.rocks import RocksOperator, ACCESS_READ_ONLY
db_path = sys.argv[1]
db = RocksOperator(
    configs={"db_path": db_path, "access_type": ACCESS_READ_ONLY},
    verbose=False,
)
val = db.get("key1")
db.close()
assert val == "value1", f"Expected 'value1', got {val!r}"
print("OK")
"""


class TestMultiProcess:
    """Test actual multi-process concurrent access using subprocesses."""

    def test_multi_process_read_only(self, populated_db):
        """Multiple separate processes can read the same DB concurrently."""
        processes = []
        for _ in range(3):
            p = subprocess.Popen(
                [sys.executable, "-c", _READER_SCRIPT, str(populated_db)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            processes.append(p)

        for p in processes:
            stdout, stderr = p.communicate(timeout=30)
            assert p.returncode == 0, (
                f"Process failed with code {p.returncode}:\n"
                f"stdout: {stdout.decode()}\nstderr: {stderr.decode()}"
            )
            assert "OK" in stdout.decode()


class TestContextManager:
    """Test context manager and cleanup behavior."""

    def test_context_manager_read_write(self, tmp_path):
        """Context manager opens and closes DB properly."""
        db_path = str(tmp_path / "ctx_rw.rkdb")
        with RocksOperator(configs={"db_path": db_path}, verbose=False) as db:
            db.set("k1", "v1")
            assert db.get("k1") == "v1"
        # After exit, db should be None (closed)
        assert db.db is None

    def test_context_manager_secondary_cleanup(self, populated_db, tmp_path):
        """Secondary mode cleans up secondary_path on close."""
        sec_path = tmp_path / "sec_cleanup"
        with RocksOperator(
            configs={
                "db_path": str(populated_db),
                "access_type": ACCESS_SECONDARY,
                "secondary_path": str(sec_path),
            },
            verbose=False,
        ) as db:
            assert db.get("key1") == "value1"
            assert sec_path.exists()
        # secondary_path should be cleaned up after close
        assert not sec_path.exists()

    def test_double_close_is_safe(self, tmp_path):
        """Calling close() twice does not raise."""
        db_path = str(tmp_path / "double_close.rkdb")
        db = RocksOperator(configs={"db_path": db_path}, verbose=False)
        db.set("k", "v")
        db.close()
        db.close()  # Should not raise


# python -m pytest tests/test_rocks_access.py
