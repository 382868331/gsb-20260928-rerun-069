"""特征计算服务对外接口。"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from .aggregations import get_aggregation, registered_names
from .engine import (
    FeatureSpec,
    IncrementalProjection,
    compute_from_rows,
    read_log_rows,
)
from .store import connect, dumps, loads


UPSERT = "UPSERT"
RETRACT = "RETRACT"


class FeatureService:
    """所有时间均为整数毫秒时间戳，由调用方显式给出（便于确定性测试）。"""

    def __init__(self, db_path: str | Path = ":memory:"):
        self.conn = connect(db_path)
        self.projection = IncrementalProjection(self.conn)
        self._ensure_source("default", "event", 0)

    def _ensure_source(self, name: str, kind: str, created_at: int) -> None:
        row = self.conn.execute("SELECT kind FROM sources WHERE name = ?", (name,)).fetchone()
        if row is None:
            self.conn.execute(
                "INSERT INTO sources (name, kind, created_at) VALUES (?,?,?)",
                (name, kind, created_at),
            )
            self.conn.commit()
        elif row["kind"] != kind:
            raise ValueError(f"来源 {name!r} 已注册为 {row['kind']!r}，不能按 {kind!r} 使用")

    def register_source(
        self, name: str, kind: str, *, created_at: int | None = None
    ) -> None:
        """注册数据来源。``kind`` 为 'event'（行为流）或 'state'（状态源）。"""
        if name == "default":
            raise ValueError("'default' 是内置行为来源名")
        if kind not in ("event", "state"):
            raise ValueError("kind 必须为 'event' 或 'state'")
        created_at = int(time.time() * 1000) if created_at is None else created_at
        self._ensure_source(name, kind, created_at)

    # ---- 特征定义：版本化、不可变 -------------------------------------

    def register_feature(
        self,
        name: str,
        *,
        entity: str,
        windows: list[int] | tuple[int, ...],
        aggregations: list[str] | tuple[str, ...],
        value_field: str | None = None,
        event_sources: list[str] | tuple[str, ...] = (),
        state_sources: list[str] | tuple[str, ...] = (),
        created_at: int | None = None,
    ) -> int:
        """登记或发布新版本的特征定义，返回版本号。

        同名特征的已发布版本不可修改；定义内容变化时自动追加新版本。
        连续重复登记同一配置则返回既有版本号（幂等）。
        """
        windows = tuple(windows)
        aggregations = tuple(aggregations)
        if not windows or any(w <= 0 for w in windows):
            raise ValueError("windows 必须为正整数列表")
        if not aggregations:
            raise ValueError("aggregations 不能为空")
        for agg in aggregations:
            get_aggregation(agg)  # 未注册会立刻报错
        event_sources = tuple(event_sources) or ("default",)
        state_sources = tuple(state_sources)
        for src in event_sources:
            self._require_source(src, "event")
        for src in state_sources:
            self._require_source(src, "state")
        created_at = int(time.time() * 1000) if created_at is None else created_at

        row = self.conn.execute(
            "SELECT current_ver FROM feature_definitions WHERE name = ?", (name,)
        ).fetchone()
        if row is None:
            version = 1
            self.conn.execute(
                "INSERT INTO feature_definitions (name, created_at, current_ver) VALUES (?,?,1)",
                (name, created_at),
            )
        else:
            version = row["current_ver"]
            latest = self._get_spec(name, version)
            same = (
                latest.entity == entity
                and tuple(latest.windows) == windows
                and tuple(latest.aggregations) == aggregations
                and latest.value_field == value_field
                and tuple(latest.event_sources) == event_sources
                and tuple(latest.state_sources) == state_sources
            )
            if same:
                return version
            version += 1
            self.conn.execute(
                "UPDATE feature_definitions SET current_ver = ? WHERE name = ?",
                (version, name),
            )

        self.conn.execute(
            "INSERT INTO feature_versions "
            "(name, version, entity, windows, aggregations, value_field, "
            "event_sources, state_sources, created_at) VALUES (?,?,?,?,?,?,?,?,?)",
            (name, version, entity, dumps(list(windows)), dumps(list(aggregations)),
             value_field, dumps(list(event_sources)), dumps(list(state_sources)), created_at),
        )
        self.conn.commit()
        return version

    def _require_source(self, name: str, kind: str) -> None:
        row = self.conn.execute("SELECT kind FROM sources WHERE name = ?", (name,)).fetchone()
        if row is None:
            raise KeyError(f"来源未注册: {name!r}")
        if row["kind"] != kind:
            raise ValueError(f"来源 {name!r} 是 {row['kind']!r}，不能作为 {kind!r} 来源使用")

    def list_features(self) -> list[tuple[str, int]]:
        return [
            (r["name"], r["current_ver"])
            for r in self.conn.execute("SELECT name, current_ver FROM feature_definitions")
        ]

    def _get_spec(self, name: str, version: int | None = None) -> FeatureSpec:
        if version is None:
            row = self.conn.execute(
                "SELECT current_ver FROM feature_definitions WHERE name = ?", (name,)
            ).fetchone()
            if row is None:
                raise KeyError(f"特征不存在: {name!r}")
            version = row["current_ver"]
        row = self.conn.execute(
            "SELECT name, version, entity, windows, aggregations, value_field, "
            "event_sources, state_sources "
            "FROM feature_versions WHERE name = ? AND version = ?",
            (name, version),
        ).fetchone()
        if row is None:
            raise KeyError(f"特征版本不存在: {name!r} v{version}")
        return FeatureSpec.from_row(row)

    # ---- 原始事件：append-only 账本 -----------------------------------

    def ingest(
        self,
        event_id: str,
        entity: str,
        *,
        event_time: int,
        arrival_time: int,
        payload: dict[str, Any] | None = None,
        op: str = UPSERT,
        source: str = "default",
    ) -> bool:
        """写入一条原始事件记录。

        返回 True 表示新增，False 表示完全重复的投递被幂等忽略。
        ``arrival_time`` 必须显式给出：它承载“系统何时知道这条记录”。
        """
        if op not in (UPSERT, RETRACT):
            raise ValueError("op 必须为 UPSERT 或 RETRACT")
        if arrival_time < event_time:
            raise ValueError("arrival_time 早于 event_time：到达不能先于发生")
        kind = self.conn.execute("SELECT kind FROM sources WHERE name = ?", (source,)).fetchone()
        if kind is None:
            raise KeyError(f"来源未注册: {source!r}（先 register_source）")
        cur = self.conn.execute(
            "INSERT OR IGNORE INTO event_log "
            "(source, event_id, entity, op, event_time, arrival_time, payload) "
            "VALUES (?,?,?,?,?,?,?)",
            (source, event_id, entity, op, event_time, arrival_time,
             dumps(payload or {})),
        )
        self.conn.commit()
        return cur.rowcount == 1

    def retract(
        self, event_id: str, entity: str, *, event_time: int, arrival_time: int,
        source: str = "default",
    ) -> bool:
        return self.ingest(
            event_id, entity, event_time=event_time, arrival_time=arrival_time,
            payload=None, op=RETRACT, source=source,
        )

    # ---- 人工样本 -----------------------------------------------------

    def register_sample(self, sample_id: str, entity_type: str, entity_id: str, as_of: int) -> None:
        """登记人工样本。

        ``entity_type`` 对齐特征定义里的实体类型（如 ``"user"``），
        ``entity_id`` 是具体实体（如 ``"u1"``）。
        """
        self.conn.execute(
            "INSERT INTO samples (sample_id, entity_type, entity_id, as_of) VALUES (?,?,?,?) "
            "ON CONFLICT(sample_id) DO UPDATE SET "
            "entity_type = excluded.entity_type, entity_id = excluded.entity_id, as_of = excluded.as_of",
            (sample_id, entity_type, entity_id, as_of),
        )
        self.conn.commit()

    def _sample(self, sample_id: str):
        row = self.conn.execute(
            "SELECT sample_id, entity_type, entity_id, as_of FROM samples WHERE sample_id = ?",
            (sample_id,),
        ).fetchone()
        if row is None:
            raise KeyError(f"样本不存在: {sample_id!r}")
        return row

    # ---- 计算：两条路径，同一套语义 -----------------------------------

    def compute_incremental(self, entity: str, spec: FeatureSpec, as_of: int) -> dict:
        """增量路径：先把账本新记录追加入投影，再从投影计算。

        进程重启后投影和 checkpoint 已持久化，只会处理上次之后的新记录。
        """
        self.projection.catch_up()
        detailed = compute_from_rows(spec, as_of, self.projection.read_rows(entity))
        return {k: fv.value for k, fv in detailed.items()}

    def compute_batch(self, entity: str, spec: FeatureSpec, as_of: int) -> dict:
        """整批路径：跳过一切缓存，直接扫描原始事件账本重放。"""
        detailed = compute_from_rows(spec, as_of, read_log_rows(self.conn, entity))
        return {k: fv.value for k, fv in detailed.items()}

    def _detailed(self, sample, spec: FeatureSpec, batch: bool) -> dict:
        if batch:
            rows = read_log_rows(self.conn, sample["entity_id"])
        else:
            self.projection.catch_up()
            rows = self.projection.read_rows(sample["entity_id"])
        return compute_from_rows(spec, sample["as_of"], rows)

    def rebuild_projection(self) -> None:
        """丢弃增量缓存并整批重放（运维/校验用）。"""
        self.projection.rebuild()

    # ---- 物化：特征值按版本冻结，不被未来修订穿越 ----------------------

    def materialize(
        self,
        sample_id: str,
        feature_name: str,
        version: int | None = None,
        *,
        batch: bool = False,
        recompute: bool = False,
        sealed: bool = True,
    ) -> dict[str, object]:
        """为样本物化某特征版本，sealed=True 时封存冻结；False 时可被重算。

        * 已封存行：之后到达的更正/撤回一律不改变它，证据也原样保留；
        * 未封存行：可由 :meth:`recompute_unsealed` 按影响范围重算；
        * ``recompute=True`` 强制按当前可见账本重算（即使已封存也可覆盖，
          用于离线核对；默认调用路径不会动封存样本）。
        """
        sample = self._sample(sample_id)
        spec = self._get_spec(feature_name, version)
        version = spec.version
        if sample["entity_type"] != spec.entity:
            raise ValueError(
                f"样本实体类型 {sample['entity_type']!r} 与特征实体 {spec.entity!r} 不一致"
            )

        rows_existing = self.conn.execute(
            "SELECT feature_key, value, sealed FROM sample_features "
            "WHERE sample_id = ? AND feature_name = ? AND version = ?",
            (sample_id, feature_name, version),
        ).fetchall()
        if rows_existing and not recompute:
            if all(r["sealed"] for r in rows_existing):
                return {r["feature_key"]: loads(r["value"]) for r in rows_existing}
            if not sealed:
                return self.recompute_unsealed(sample_id, feature_name, version, batch=batch)

        values = self._detailed(sample, spec, batch)

        now = int(time.time() * 1000)
        self.conn.executemany(
            "INSERT INTO sample_features "
            "(sample_id, feature_name, version, feature_key, value, evidence, sealed, computed_at) "
            "VALUES (?,?,?,?,?,?,?,?) "
            "ON CONFLICT(sample_id, feature_name, version, feature_key) "
            "DO UPDATE SET value = excluded.value, evidence = excluded.evidence, "
            "sealed = excluded.sealed, computed_at = excluded.computed_at",
            [
                (sample_id, feature_name, version, key, dumps(fv.value),
                 dumps(list(fv.evidence)), 1 if sealed else 0, now)
                for key, fv in values.items()
            ],
        )
        self.conn.commit()
        return {key: fv.value for key, fv in values.items()}

    def seal(self, sample_id: str, feature_name: str, version: int) -> None:
        """封存一份未冻结结果：此后任何更正/撤回都不再改变它。"""
        cur = self.conn.execute(
            "UPDATE sample_features SET sealed = 1 "
            "WHERE sample_id = ? AND feature_name = ? AND version = ? AND sealed = 0",
            (sample_id, feature_name, version),
        )
        self.conn.commit()
        if cur.rowcount == 0 and not self.conn.execute(
            "SELECT 1 FROM sample_features "
            "WHERE sample_id = ? AND feature_name = ? AND version = ?",
            (sample_id, feature_name, version),
        ).fetchone():
            raise KeyError(f"样本 {sample_id!r} 尚未物化 {feature_name!r} v{version}")

    def recompute_unsealed(
        self, sample_id: str, feature_name: str, version: int, *, batch: bool = False
    ) -> dict[str, object] | None:
        """重算单个未封存结果；已封存行原样保留原证据，返回 None。

        来源更正或撤回后可按影响范围调用（只重算受影响实体的样本）。
        """
        marker = self.conn.execute(
            "SELECT MIN(sealed) AS m FROM sample_features "
            "WHERE sample_id = ? AND feature_name = ? AND version = ?",
            (sample_id, feature_name, version),
        ).fetchone()
        if marker is None or marker["m"] is None:
            raise KeyError(f"样本 {sample_id!r} 尚未物化 {feature_name!r} v{version}")
        if marker["m"] == 1:
            return None  # 已冻结的训练样本保留原证据
        sample = self._sample(sample_id)
        spec = self._get_spec(feature_name, version)
        values = self._detailed(sample, spec, batch)
        now = int(time.time() * 1000)
        self.conn.executemany(
            "UPDATE sample_features SET value = ?, evidence = ?, computed_at = ? "
            "WHERE sample_id = ? AND feature_name = ? AND version = ? "
            "AND feature_key = ? AND sealed = 0",
            [
                (dumps(fv.value), dumps(list(fv.evidence)), now,
                 sample_id, feature_name, version, key)
                for key, fv in values.items()
            ],
        )
        self.conn.commit()
        return {key: fv.value for key, fv in values.items()}

    def explain(self, sample_id: str, feature_key: str) -> dict[str, object]:
        """说明一个已物化特征值来自哪些可见账本记录（重启后仍可追溯）。"""
        row = self.conn.execute(
            "SELECT feature_name, version, value, evidence, sealed "
            "FROM sample_features WHERE sample_id = ? AND feature_key = ?",
            (sample_id, feature_key),
        ).fetchone()
        if row is None:
            raise KeyError(f"样本 {sample_id!r} 未物化特征 {feature_key!r}")
        seqs = loads(row["evidence"]) or []
        records = [
            {
                "seq": r["seq"], "source": r["source"], "event_id": r["event_id"],
                "entity": r["entity"], "op": r["op"],
                "event_time": r["event_time"], "arrival_time": r["arrival_time"],
                "payload": loads(r["payload"]),
            }
            for r in self.conn.execute(
                f"SELECT seq, source, event_id, entity, op, event_time, arrival_time, payload "
                f"FROM event_log WHERE seq IN ({','.join('?' * len(seqs))}) ORDER BY seq",
                tuple(seqs),
            ).fetchall()
        ] if seqs else []
        return {
            "sample_id": sample_id,
            "feature_key": feature_key,
            "feature_name": row["feature_name"],
            "version": row["version"],
            "value": loads(row["value"]),
            "sealed": bool(row["sealed"]),
            "evidence_seqs": seqs,
            "records": records,
        }

    def get_features(self, sample_id: str, feature_name: str, version: int) -> dict[str, object]:
        """读取已冻结的样本特征值（训练时使用的就是这份快照）。"""
        rows = self.conn.execute(
            "SELECT feature_key, value FROM sample_features "
            "WHERE sample_id = ? AND feature_name = ? AND version = ? ORDER BY feature_key",
            (sample_id, feature_name, version),
        ).fetchall()
        if not rows:
            raise KeyError(
                f"样本 {sample_id!r} 尚未物化 {feature_name!r} v{version}"
            )
        return {r["feature_key"]: loads(r["value"]) for r in rows}

    def close(self) -> None:
        self.conn.close()
