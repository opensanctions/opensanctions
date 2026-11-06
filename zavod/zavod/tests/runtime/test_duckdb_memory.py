import pytest
import structlog
from nomenklatura import duck

from zavod.runtime import duckdb_memory
from zavod.runtime.duckdb_memory import (
    memory_held_ratio,
    parse_size,
    warn_memory_pressure,
)


def test_parse_size() -> None:
    assert parse_size("0 bytes") == 0
    assert parse_size("599.8 MiB") == int(599.8 * 2**20)
    assert parse_size("2.3 GiB") == int(2.3 * 2**30)
    assert parse_size("500 MB") == 500 * 10**6
    with pytest.raises(ValueError):
        parse_size("lots")
    with pytest.raises(ValueError):
        parse_size("2.3 parsecs")


def test_memory_held_ratio_counts_in_memory_tables() -> None:
    conn = duck.connect(memory_mb=1000)
    empty = memory_held_ratio(conn)
    conn.execute(
        "CREATE TABLE t AS SELECT range AS i, md5(range::VARCHAR) AS h "
        "FROM range(200000)"
    )
    conn.execute("CREATE INDEX t_h ON t (h)")
    held = memory_held_ratio(conn)
    assert held > empty
    assert 0 < held < 1
    conn.close()


def test_memory_held_ratio_ignores_file_caches(tmp_path) -> None:
    path = tmp_path / "test.duckdb"
    conn = duck.connect(path, memory_mb=1000)
    conn.execute(
        "CREATE TABLE t AS SELECT range AS i, md5(range::VARCHAR) AS h "
        "FROM range(200000)"
    )
    conn.execute("CHECKPOINT")
    conn.execute("SELECT count(DISTINCT h) FROM t").fetchone()
    # Table blocks of a database file are BASE_TABLE, which can be re-read.
    assert memory_held_ratio(conn) < 0.01
    conn.close()


def test_warn_memory_pressure(monkeypatch) -> None:
    conn = duck.connect(memory_mb=1000)
    conn.execute(
        "CREATE TABLE t AS SELECT range AS i, md5(range::VARCHAR) AS h "
        "FROM range(200000)"
    )
    with structlog.testing.capture_logs() as logs:
        warn_memory_pressure(conn, "test")
    assert logs == []

    monkeypatch.setattr(duckdb_memory, "WARN_RATIO", 0.0)
    with structlog.testing.capture_logs() as logs:
        warn_memory_pressure(conn, "test")
    assert len(logs) == 1
    assert logs[0]["log_level"] == "warning"
    assert logs[0]["instance"] == "test"
    assert logs[0]["memory_held_ratio"] > 0
    conn.close()


def test_warn_memory_pressure_unreadable_limit(monkeypatch) -> None:
    def fail(text: str) -> int:
        raise ValueError(f"Unrecognised DuckDB size: {text!r}")

    monkeypatch.setattr(duckdb_memory, "parse_size", fail)
    conn = duck.connect(memory_mb=1000)
    with structlog.testing.capture_logs() as logs:
        warn_memory_pressure(conn, "test")
    assert len(logs) == 1
    assert logs[0]["log_level"] == "warning"
    assert "Could not check" in logs[0]["event"]
    conn.close()
