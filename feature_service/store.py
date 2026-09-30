"""SQLite 持久化层。

三层模型，互不混用：

1. ``feature_definitions`` / ``feature_versions``
   特征定义只做版本追加，已发布版本不可修改。
2. ``event_log``
   原始事件账本，append-only。UPSERT/RETRACT 是账本上的新记录，
   历史记录永不删除，保证可以完整重放。
3. ``sample_features``
   每次按特征版本物化出的样本特征值，随版本冻结。

增量计算缓存（``projection_entries`` / ``checkpoint``）由引擎层创建，
随时可以删除并由账本整批重放重建。
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Iterable


SCHEMA = """
CREATE TABLE IF NOT EXISTS feature_definitions (
    name         TEXT PRIMARY KEY,
    created_at   INTEGER NOT NULL,
    current_ver  INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS feature_versions (
    name          TEXT NOT NULL,
    version       INTEGER NOT NULL,
    entity        TEXT NOT NULL,
    windows       TEXT NOT NULL,   -- JSON list[int]，毫秒窗口
    aggregations  TEXT NOT NULL,   -- JSON list[str]
    value_field   TEXT,            -- 取值字段，count 时可为 NULL
    sources       TEXT,            -- JSON list[str]，行为来源
    state_source  TEXT,            -- 最近状态来源
    state_field   TEXT,            -- 最近状态取值字段
    created_at    INTEGER NOT NULL,
    PRIMARY KEY (name, version)
);

CREATE TABLE IF NOT EXISTS event_log (
    seq          INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id     TEXT NOT NULL,
    entity       TEXT NOT NULL,
    source       TEXT NOT NULL DEFAULT 'default',
    op           TEXT NOT NULL CHECK (op IN ('UPSERT', 'RETRACT')),
    event_time   INTEGER NOT NULL,
    arrival_time INTEGER NOT NULL,
    payload      TEXT NOT NULL,    -- JSON 对象
    UNIQUE (event_id, entity, source, op, event_time, arrival_time, payload)
);

CREATE TABLE IF NOT EXISTS samples (
    sample_id   TEXT PRIMARY KEY,
    entity_type TEXT NOT NULL,   -- 与 feature_versions.entity 对应（如 user）
    entity_id   TEXT NOT NULL,   -- 具体实体标识（如 u1）
    as_of       INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS sample_features (
    sample_id    TEXT NOT NULL,
    feature_name TEXT NOT NULL,
    version      INTEGER NOT NULL,
    feature_key  TEXT NOT NULL,
    value        TEXT,            -- JSON 标量，空结果为 NULL
    computed_at  INTEGER NOT NULL,
    PRIMARY KEY (sample_id, feature_name, version, feature_key)
);

CREATE TABLE IF NOT EXISTS sample_feature_evidence (
    sample_id    TEXT NOT NULL,
    feature_name TEXT NOT NULL,
    version      INTEGER NOT NULL,
    feature_key  TEXT NOT NULL,
    seq          INTEGER NOT NULL,
    role         TEXT NOT NULL CHECK (role IN ('behavior', 'state')),
    PRIMARY KEY (sample_id, feature_name, version, feature_key, role, seq)
);
"""


def _column_names(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _add_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    if column not in _column_names(conn, table):
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")


def _migrate_event_log_unique(conn: sqlite3.Connection) -> None:
    """升级首版索引：同键不同负载是修订，不应被旧唯一约束拒绝。"""
    indexes = [r for r in conn.execute("PRAGMA index_list(event_log)").fetchall() if r["unique"]]
    unique_columns: set[str] = set()
    for index in indexes:
        index_name = str(index["name"]).replace('"', '""')
        unique_columns = {
            r["name"]
            for r in conn.execute(f'PRAGMA index_info("{index_name}")').fetchall()
        }
        break
    columns = _column_names(conn, "event_log")
    expected = {"event_id", "entity", "source", "op", "event_time", "arrival_time", "payload"}
    if "source" in columns and unique_columns == expected:
        return
    conn.execute("ALTER TABLE event_log RENAME TO event_log_v1")
    conn.execute(
        """CREATE TABLE event_log (
            seq          INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id     TEXT NOT NULL,
            entity       TEXT NOT NULL,
            source       TEXT NOT NULL DEFAULT 'default',
            op           TEXT NOT NULL CHECK (op IN ('UPSERT', 'RETRACT')),
            event_time   INTEGER NOT NULL,
            arrival_time INTEGER NOT NULL,
            payload      TEXT NOT NULL,
            UNIQUE (event_id, entity, source, op, event_time, arrival_time, payload)
        )"""
    )
    source_select = "source" if "source" in columns else "'default'"
    conn.execute(
        f"""INSERT INTO event_log
        (seq, event_id, entity, source, op, event_time, arrival_time, payload)
        SELECT seq, event_id, entity, {source_select},
               op, event_time, arrival_time, payload
        FROM event_log_v1"""
    )
    conn.execute("DROP TABLE event_log_v1")


def connect(db_path: str | Path) -> sqlite3.Connection:
    path = Path(db_path)
    if path.parent != Path("."):
        path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(SCHEMA)
    # 兼容首版数据库：新版增加来源维度，但旧数据仍归入 default 来源。
    _migrate_event_log_unique(conn)
    _add_column(conn, "feature_versions", "sources", "TEXT")
    _add_column(conn, "feature_versions", "state_source", "TEXT")
    _add_column(conn, "feature_versions", "state_field", "TEXT")
    return conn


def dumps(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def loads(raw: str | None) -> Any:
    if raw is None:
        return None
    return json.loads(raw)


def iter_rows(conn: sqlite3.Connection, sql: str, params: Iterable[Any] = ()) -> list[sqlite3.Row]:
    return list(conn.execute(sql, tuple(params)))
