"""双时间特征引擎。

时间语义
--------
* ``event_time``：事件实际发生时间，决定记录落在哪个滚动窗口；
* ``arrival_time``：记录到达（被系统知道）的时间，决定某个查询时点
  能否看到它。查询 ``as_of`` 只能使用 ``arrival_time <= as_of`` 的记录，
  迟到数据与迟到修订不会穿越到历史样本。

账本归约（同一业务事件的重复 / 修订 / 撤回）
------------------------------------------------
以 ``(entity, event_id)`` 为键。对给定 ``as_of``：

* 只看 ``arrival_time <= as_of`` 的账本记录，按 ``seq``（写入顺序）取
  该键最后一条；
* 最后一条是 ``UPSERT`` 则贡献其值，``RETRACT`` 则该事件抵消为空；
* 完全相同的重复投递（同 op/event_time/负载）在写入时即被幂等丢弃。

滚动窗口为左闭右开 ``[as_of - window, as_of)``。
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass

from .aggregations import get_aggregation
from .store import dumps, loads


@dataclass(frozen=True)
class FeatureSpec:
    name: str
    version: int
    entity: str
    windows: tuple[int, ...]
    aggregations: tuple[str, ...]
    value_field: str | None
    sources: tuple[str, ...] = ("default",)
    state_source: str | None = None
    state_field: str | None = None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "FeatureSpec":
        return cls(
            name=row["name"],
            version=row["version"],
            entity=row["entity"],
            windows=tuple(json.loads(row["windows"])),
            aggregations=tuple(json.loads(row["aggregations"])),
            value_field=row["value_field"],
            sources=tuple(json.loads(row["sources"])) if "sources" in row.keys() and row["sources"] else ("default",),
            state_source=row["state_source"] if "state_source" in row.keys() else None,
            state_field=row["state_field"] if "state_field" in row.keys() else None,
        )


def feature_keys(spec: FeatureSpec) -> list[str]:
    keys = [f"{spec.name}_w{w}_{agg}" for w in spec.windows for agg in spec.aggregations]
    if spec.state_source is not None:
        keys.append(f"{spec.name}_last_state")
    return keys


def _column(row: sqlite3.Row, name: str, default=None):
    return row[name] if name in row.keys() else default


def _payload_value(row: sqlite3.Row, field: str | None):
    payload = loads(row["payload"])
    return payload if field is None else payload[field]


def _reduce_visible(rows: list[sqlite3.Row], value_field: str | None) -> list[sqlite3.Row]:
    """按 (来源,业务事件 ID) 归约为查询时点的最新版本。"""
    latest: dict[str, sqlite3.Row] = {}
    for row in rows:  # 已按 seq 升序
        latest[(_column(row, "source", "default"), row["event_id"])] = row
    out: list[sqlite3.Row] = []
    for row in latest.values():
        if row["op"] != "RETRACT":
            out.append(row)
    return out


def explain_from_rows(spec: FeatureSpec, as_of: int, rows: list[sqlite3.Row]) -> dict[str, object]:
    """纯函数：账本行 → 特征值及每个值使用的账本 seq。

    批路径与增量路径都调用本函数，保证两条路径同一套语义。
    """
    visible_rows = [r for r in rows if r["arrival_time"] <= as_of]
    behavior_rows = [
        r for r in visible_rows
        if _column(r, "source", "default") in spec.sources
    ]
    active_rows = _reduce_visible(behavior_rows, spec.value_field)
    result: dict[str, object] = {}
    evidence: dict[str, dict[str, list[int]]] = {}
    for window in spec.windows:
        lo, hi = as_of - window, as_of
        window_rows = [r for r in active_rows if lo <= r["event_time"] < hi]
        values = [float(_payload_value(r, spec.value_field)) for r in window_rows]
        for agg_name in spec.aggregations:
            agg = get_aggregation(agg_name)
            key = f"{spec.name}_w{window}_{agg_name}"
            result[key] = agg.compute(values) if values else agg.empty
            evidence[key] = {"behavior": [r["seq"] for r in window_rows], "state": []}

    if spec.state_source is not None:
        state_rows = [
            r for r in visible_rows
            if _column(r, "source", "default") == spec.state_source
        ]
        active_states = _reduce_visible(state_rows, spec.state_field)
        current_states = [r for r in active_states if r["event_time"] <= as_of]
        state_row = max(
            current_states,
            key=lambda r: (r["event_time"], r["arrival_time"], r["seq"]),
            default=None,
        )
        key = f"{spec.name}_last_state"
        result[key] = None if state_row is None else _payload_value(state_row, spec.state_field)
        evidence[key] = {"behavior": [], "state": [] if state_row is None else [state_row["seq"]]}

    return {"values": result, "evidence": evidence}


def compute_from_rows(spec: FeatureSpec, as_of: int, rows: list[sqlite3.Row]) -> dict[str, object]:
    """兼容旧接口：仅返回特征值字典。"""
    return explain_from_rows(spec, as_of, rows)["values"]


class IncrementalProjection:
    """账本的增量镜像（checkpoint 后只追加新 seq）。

    镜像保留每个 ``(entity, event_id)`` 的全部版本（含 arrival_time 与
    op），因此它和原始账本对任何历史 ``as_of`` 都等价；它是可随时丢弃
    的缓存——删掉后由整批重放重建，结果不变。
    """

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        conn.execute(
            """CREATE TABLE IF NOT EXISTS projection_entries (
                seq          INTEGER PRIMARY KEY,
                event_id     TEXT NOT NULL,
                entity       TEXT NOT NULL,
                source       TEXT NOT NULL DEFAULT 'default',
                op           TEXT NOT NULL,
                event_time   INTEGER NOT NULL,
                arrival_time INTEGER NOT NULL,
                payload      TEXT NOT NULL
            )"""
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS checkpoint ("
            "id INTEGER PRIMARY KEY CHECK (id = 1), last_seq INTEGER NOT NULL)"
        )
        conn.commit()
        columns = {r[1] for r in conn.execute("PRAGMA table_info(projection_entries)").fetchall()}
        if "source" not in columns:
            conn.execute("ALTER TABLE projection_entries ADD COLUMN source TEXT NOT NULL DEFAULT 'default'")
            conn.commit()

    def checkpoint(self) -> int:
        row = self.conn.execute("SELECT last_seq FROM checkpoint WHERE id = 1").fetchone()
        return -1 if row is None else row["last_seq"]

    def catch_up(self) -> int:
        """把 checkpoint 之后的账本记录追加进镜像，返回新的 last_seq。"""
        last_seq = self.checkpoint()
        rows = self.conn.execute(
            "SELECT seq, event_id, entity, source, op, event_time, arrival_time, payload "
            "FROM event_log WHERE seq > ? ORDER BY seq",
            (last_seq,),
        ).fetchall()
        self.conn.executemany(
            "INSERT OR IGNORE INTO projection_entries "
            "(seq, event_id, entity, source, op, event_time, arrival_time, payload) "
            "VALUES (?,?,?,?,?,?,?,?)",
            [
                (r["seq"], r["event_id"], r["entity"], r["source"], r["op"],
                 r["event_time"], r["arrival_time"], r["payload"])
                for r in rows
            ],
        )
        new_last = self.conn.execute("SELECT MAX(seq) AS m FROM event_log").fetchone()["m"]
        if new_last is not None:
            self.conn.execute(
                "INSERT INTO checkpoint (id, last_seq) VALUES (1, ?) "
                "ON CONFLICT(id) DO UPDATE SET last_seq = excluded.last_seq",
                (new_last,),
            )
        self.conn.commit()
        return new_last if new_last is not None else last_seq

    def rebuild(self) -> None:
        """整批重放：清空镜像与 checkpoint，从账本头开始重建。"""
        self.conn.execute("DELETE FROM projection_entries")
        self.conn.execute("DELETE FROM checkpoint")
        self.conn.commit()
        self.catch_up()

    def read_rows(self, entity: str) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT seq, event_id, entity, source, op, event_time, arrival_time, payload "
            "FROM projection_entries WHERE entity = ? ORDER BY seq",
            (entity,),
        ).fetchall()


def read_log_rows(conn: sqlite3.Connection, entity: str) -> list[sqlite3.Row]:
    """整批路径：直接读原始事件账本。"""
    return conn.execute(
        "SELECT seq, event_id, entity, source, op, event_time, arrival_time, payload "
        "FROM event_log WHERE entity = ? ORDER BY seq",
        (entity,),
    ).fetchall()
