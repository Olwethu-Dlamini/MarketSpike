import sqlite3
from marketspike.store.db import open_db, apply_schema, SCHEMA_VERSION


def test_apply_schema_creates_all_tables(tmp_path):
    conn = open_db(str(tmp_path / "t.db"))
    apply_schema(conn)
    names = {
        row[0]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    assert {
        "ticks", "regime_events", "client_latency", "calc_log",
        "training_samples", "model_registry", "calendar_events", "schema_version",
    } <= names


def test_apply_schema_is_idempotent(tmp_path):
    conn = open_db(str(tmp_path / "t.db"))
    apply_schema(conn)
    apply_schema(conn)
    rows = list(conn.execute("SELECT version FROM schema_version"))
    assert rows == [(SCHEMA_VERSION,)]


def test_wal_mode_enabled(tmp_path):
    conn = open_db(str(tmp_path / "t.db"))
    assert conn.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
