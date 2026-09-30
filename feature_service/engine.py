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
from typing import Any

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
    event_sources: tuple[str, ...] = ("default",)
    state_sources: tuple[str, ...] = ()

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "FeatureSpec":
        keys = row.keys()
        return cls(
            name=row["name"],
            version=row["version"],
            entity=row["entity"],
            windows=tuple(json.loads(row["windows"])),
            aggregations=tuple(json.loads(row["aggregations"])),
            value_field=row["value_field"],
            event_sources=tuple(json.loads(row["event_sources"])) if "event_sources" in keys else ("default",),
            state_sources=tuple(json.loads(row["state_sources"])) if "state_sources" in keys else (),
        )


def feature_keys(spec: FeatureSpec) -> list[str]:
    keys = [f"{spec.name}_w{w}_{agg}" for w in spec.windows for agg in spec.aggregations]
    keys.extend(f"{spec.name}_state_{src}" for src in spec.state_sources)
    return keys


@dataclass(frozen=True)
class FeatureValue:
    value: object
    evidence: tuple[int, ...]  # 产出该值的账本 seq（winning 版本）


def _field_value(row: sqlite3.Row, value_field: str | None) -> float:
    if value_field is None:
        return 1.0
    return float(loads(row["payload"])[value_field])


def _winners(visible_rows: list[sqlite3.Row]) -> list[sqlite3.Row]:
    """按 (source, event_id) 归约，返回每个业务身份的当前有效版本。

    重复身份冲突：同一业务身份的多条 UPSERT 并非重复投递（op/event_time/
    到达时间或负载不同），按账本 seq（写入顺序）归约到最后一条——
    即“后到的更正覆盖早先值”，且该覆盖同样受 arrival_time 约束。
    RETRACT 胜出时该身份无效（被撤回）。
    """
    latest: dict[tuple[str, str], sqlite3.Row] = {}
    for row in visible_rows:  # 已按 seq 升序
        latest[(row["source"], row["event_id"])] = row
    return [r for r in latest.values() if r["op"] != "RETRACT"]


def compute_from_rows(
    spec: FeatureSpec, as_of: int, rows: list[sqlite3.Row]
) -> dict[str, FeatureValue]:
    """纯函数：账本行 → {feature_key: FeatureValue(value, evidence)}。

    批路径与增量路径都调用本函数，保证两条路径同一套语义。
    evidence 是产出每个值的账本 seq 列表：逐条接入与整批重放必须一致，
    重启后仍能据此说明特征值来自哪些可见记录。
    """
    visible_rows = [r for r in rows if r["arrival_time"] <= as_of]
    event_winners = _winners([r for r in visible_rows if r["source"] in spec.event_sources])
    state_winners = _winners([r for r in visible_rows if r["source"] in spec.state_sources])
    result: dict[str, FeatureValue] = {}

    # 行为源：滚动窗口 [as_of - window, as_of)，左闭右开。
    # 边界时刻相同的消息：event_time == as_of - window 计入，== as_of 不计入。
    for window in spec.windows:
        lo, hi = as_of - window, as_of
        in_window = [r for r in event_winners if lo <= r["event_time"] < hi]
        for agg_name in spec.aggregations:
            agg = get_aggregation(agg_name)
            key = f"{spec.name}_w{window}_{agg_name}"
            if in_window:
                value = agg.compute([_field_value(r, spec.value_field) for r in in_window])
            else:
                value = agg.empty
            result[key] = FeatureValue(value, tuple(sorted(r["seq"] for r in in_window)))

    # 状态源：as_of 之前同一实体最近一次“有效状态”（event_time <= as_of）。
    # 同 event_time 的多条状态按 seq 决胜（后写入的更正为准）。
    for src in spec.state_sources:
        key = f"{spec.name}_state_{src}"
        candidates = [r for r in state_winners if r["source"] == src and r["event_time"] <= as_of]
        if candidates:
            chosen = max(candidates, key=lambda r: (r["event_time"], r["seq"]))
            result[key] = FeatureValue(_field_value(chosen, spec.value_field), (chosen["seq"],))
        else:
            result[key] = FeatureValue(None, ())
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
                source       TEXT NOT NULL,
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
            "SELECT seq, source, event_id, entity, op, event_time, arrival_time, payload "
            "FROM event_log WHERE seq > ? ORDER BY seq",
            (last_seq,),
        ).fetchall()
        self.conn.executemany(
            "INSERT OR IGNORE INTO projection_entries "
            "(seq, source, event_id, entity, op, event_time, arrival_time, payload) "
            "VALUES (?,?,?,?,?,?,?,?)",
            [
                (r["seq"], r["source"], r["event_id"], r["entity"], r["op"], r["event_time"],
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
            "SELECT seq, source, event_id, entity, op, event_time, arrival_time, payload "
            "FROM projection_entries WHERE entity = ? ORDER BY seq",
            (entity,),
        ).fetchall()


def read_log_rows(conn: sqlite3.Connection, entity: str) -> list[sqlite3.Row]:
    """整批路径：直接读原始事件账本。"""
    return conn.execute(
        "SELECT seq, source, event_id, entity, op, event_time, arrival_time, payload "
        "FROM event_log WHERE entity = ? ORDER BY seq",
        (entity,),
    ).fetchall()
