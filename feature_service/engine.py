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

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "FeatureSpec":
        return cls(
            name=row["name"],
            version=row["version"],
            entity=row["entity"],
            windows=tuple(json.loads(row["windows"])),
            aggregations=tuple(json.loads(row["aggregations"])),
            value_field=row["value_field"],
        )


def feature_keys(spec: FeatureSpec) -> list[str]:
    return [f"{spec.name}_w{w}_{agg}" for w in spec.windows for agg in spec.aggregations]


def _reduce_visible(rows: list[sqlite3.Row], value_field: str | None) -> list[tuple[int, float]]:
    """把账本行归约成 (event_time, value) 的当前有效事件列表。"""
    latest: dict[str, sqlite3.Row] = {}
    for row in rows:  # 已按 seq 升序
        latest[row["event_id"]] = row
    out: list[tuple[int, float]] = []
    for row in latest.values():
        if row["op"] == "RETRACT":
            continue
        if value_field is None:
            value: float = 1.0
        else:
            payload = loads(row["payload"])
            value = payload[value_field]
        out.append((row["event_time"], float(value)))
    return out


def compute_from_rows(spec: FeatureSpec, as_of: int, rows: list[sqlite3.Row]) -> dict[str, object]:
    """纯函数：账本行（已按 as_of 过滤或自带过滤）→ 特征值字典。

    批路径与增量路径都调用本函数，保证两条路径同一套语义。
    """
    visible_rows = [r for r in rows if r["arrival_time"] <= as_of]
    events = _reduce_visible(visible_rows, spec.value_field)
    result: dict[str, object] = {}
    for window in spec.windows:
        lo, hi = as_of - window, as_of
        values = [v for et, v in events if lo <= et < hi]
        for agg_name in spec.aggregations:
            agg = get_aggregation(agg_name)
            key = f"{spec.name}_w{window}_{agg_name}"
            result[key] = agg.compute(values) if values else agg.empty
    return result


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

    def checkpoint(self) -> int:
        row = self.conn.execute("SELECT last_seq FROM checkpoint WHERE id = 1").fetchone()
        return -1 if row is None else row["last_seq"]

    def catch_up(self) -> int:
        """把 checkpoint 之后的账本记录追加进镜像，返回新的 last_seq。"""
        last_seq = self.checkpoint()
        rows = self.conn.execute(
            "SELECT seq, event_id, entity, op, event_time, arrival_time, payload "
            "FROM event_log WHERE seq > ? ORDER BY seq",
            (last_seq,),
        ).fetchall()
        self.conn.executemany(
            "INSERT OR IGNORE INTO projection_entries "
            "(seq, event_id, entity, op, event_time, arrival_time, payload) "
            "VALUES (?,?,?,?,?,?,?)",
            [
                (r["seq"], r["event_id"], r["entity"], r["op"], r["event_time"],
                 r["arrival_time"], r["payload"])
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
            "SELECT event_id, entity, op, event_time, arrival_time, payload "
            "FROM projection_entries WHERE entity = ? ORDER BY seq",
            (entity,),
        ).fetchall()


def read_log_rows(conn: sqlite3.Connection, entity: str) -> list[sqlite3.Row]:
    """整批路径：直接读原始事件账本。"""
    return conn.execute(
        "SELECT event_id, entity, op, event_time, arrival_time, payload "
        "FROM event_log WHERE entity = ? ORDER BY seq",
        (entity,),
    ).fetchall()
