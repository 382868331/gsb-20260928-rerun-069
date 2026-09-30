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
    explain_from_rows,
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

    # ---- 特征定义：版本化、不可变 -------------------------------------

    def register_feature(
        self,
        name: str,
        *,
        entity: str,
        windows: list[int] | tuple[int, ...],
        aggregations: list[str] | tuple[str, ...],
        value_field: str | None = None,
        sources: list[str] | tuple[str, ...] = ("default",),
        state_source: str | None = None,
        state_field: str | None = None,
        created_at: int | None = None,
    ) -> int:
        """登记或发布新版本的特征定义，返回版本号。

        同名特征的已发布版本不可修改；定义内容变化时自动追加新版本。
        连续重复登记同一配置则返回既有版本号（幂等）。
        """
        windows = tuple(windows)
        aggregations = tuple(aggregations)
        sources = tuple(sources)
        if not windows or any(w <= 0 for w in windows):
            raise ValueError("windows 必须为正整数列表")
        if not aggregations:
            raise ValueError("aggregations 不能为空")
        if not sources:
            raise ValueError("sources 不能为空")
        if state_source is not None and state_field is None:
            raise ValueError("配置 state_source 时必须指定 state_field")
        for agg in aggregations:
            get_aggregation(agg)  # 未注册会立刻报错
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
                and latest.sources == sources
                and latest.state_source == state_source
                and latest.state_field == state_field
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
            "(name, version, entity, windows, aggregations, value_field, sources, "
            "state_source, state_field, created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (name, version, entity, dumps(list(windows)), dumps(list(aggregations)),
             value_field, dumps(list(sources)), state_source, state_field, created_at),
        )
        self.conn.commit()
        return version

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
            "sources, state_source, state_field "
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
        cur = self.conn.execute(
            "INSERT OR IGNORE INTO event_log "
            "(event_id, entity, source, op, event_time, arrival_time, payload) "
            "VALUES (?,?,?,?,?,?,?)",
            (event_id, entity, source, op, event_time, arrival_time,
             dumps(payload or {})),
        )
        self.conn.commit()
        return cur.rowcount == 1

    def retract(
        self,
        event_id: str,
        entity: str,
        *,
        event_time: int,
        arrival_time: int,
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

    def compute_incremental(self, entity: str, spec: FeatureSpec, as_of: int) -> dict[str, object]:
        """增量路径：先把账本新记录追加入投影，再从投影计算。

        进程重启后投影和 checkpoint 已持久化，只会处理上次之后的新记录。
        """
        self.projection.catch_up()
        return compute_from_rows(spec, as_of, self.projection.read_rows(entity))

    def compute_batch(self, entity: str, spec: FeatureSpec, as_of: int) -> dict[str, object]:
        """整批路径：跳过一切缓存，直接扫描原始事件账本重放。"""
        return compute_from_rows(spec, as_of, read_log_rows(self.conn, entity))

    def explain_incremental(self, entity: str, spec: FeatureSpec, as_of: int) -> dict[str, object]:
        self.projection.catch_up()
        return explain_from_rows(spec, as_of, self.projection.read_rows(entity))

    def explain_batch(self, entity: str, spec: FeatureSpec, as_of: int) -> dict[str, object]:
        return explain_from_rows(spec, as_of, read_log_rows(self.conn, entity))

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
    ) -> dict[str, object]:
        """为样本物化某特征版本的值并持久化冻结。

        * 一旦物化，值即固定：之后到达的迟到事件/撤回/修订不会改变它；
        * ``batch=True`` 走整批重放路径，默认走增量路径；
        * 同一 (样本, 特征, 版本) 一经物化即封存，之后永不改写；
        * ``recompute=True`` 只用于临时核对当前账本，不会覆盖冻结快照。
        """
        sample = self._sample(sample_id)
        spec = self._get_spec(feature_name, version)
        version = spec.version
        if sample["entity_type"] != spec.entity:
            raise ValueError(
                f"样本实体类型 {sample['entity_type']!r} 与特征实体 {spec.entity!r} 不一致"
            )

        frozen = self.conn.execute(
            "SELECT feature_key, value FROM sample_features "
            "WHERE sample_id = ? AND feature_name = ? AND version = ?",
            (sample_id, feature_name, version),
        ).fetchall()
        if frozen:
            frozen_values = {r["feature_key"]: loads(r["value"]) for r in frozen}
            if not recompute:
                return frozen_values
            if batch:
                current = self.compute_batch(sample["entity_id"], spec, sample["as_of"])
            else:
                current = self.compute_incremental(sample["entity_id"], spec, sample["as_of"])
            return {"frozen": frozen_values, "current": current}

        if batch:
            explained = self.explain_batch(sample["entity_id"], spec, sample["as_of"])
        else:
            explained = self.explain_incremental(sample["entity_id"], spec, sample["as_of"])
        values = explained["values"]
        evidence = explained["evidence"]

        now = int(time.time() * 1000)
        self.conn.executemany(
            "INSERT INTO sample_features "
            "(sample_id, feature_name, version, feature_key, value, computed_at) "
            "VALUES (?,?,?,?,?,?) "
            "ON CONFLICT(sample_id, feature_name, version, feature_key) "
            "DO UPDATE SET value = excluded.value, computed_at = excluded.computed_at",
            [
                (sample_id, feature_name, version, key, dumps(value), now)
                for key, value in values.items()
            ],
        )
        self.conn.executemany(
            "INSERT INTO sample_feature_evidence "
            "(sample_id, feature_name, version, feature_key, seq, role) "
            "VALUES (?,?,?,?,?,?)",
            [
                (sample_id, feature_name, version, key, seq, role)
                for key, roles in evidence.items()
                for role, seqs in roles.items()
                for seq in seqs
            ],
        )
        self.conn.commit()
        return values

    def get_evidence(
        self, sample_id: str, feature_name: str, version: int, *, include_records: bool = True
    ) -> dict[str, dict[str, list[object]]]:
        """返回冻结特征值使用的账本 seq，可附带原始记录用于重启后审计。"""
        self._sample(sample_id)
        frozen_keys = [
            r["feature_key"]
            for r in self.conn.execute(
                "SELECT feature_key FROM sample_features "
                "WHERE sample_id = ? AND feature_name = ? AND version = ?",
                (sample_id, feature_name, version),
            ).fetchall()
        ]
        if not frozen_keys:
            raise KeyError(f"样本 {sample_id!r} 尚无 {feature_name!r} v{version} 的证据")
        rows = self.conn.execute(
            "SELECT feature_key, seq, role FROM sample_feature_evidence "
            "WHERE sample_id = ? AND feature_name = ? AND version = ? "
            "ORDER BY feature_key, role, seq",
            (sample_id, feature_name, version),
        ).fetchall()
        result: dict[str, dict[str, list[object]]] = {
            key: {"behavior": [], "state": []} for key in frozen_keys
        }
        seq_by_key: dict[str, list[int]] = {}
        for row in rows:
            result[row["feature_key"]][row["role"]].append(row["seq"])
            seq_by_key.setdefault(row["feature_key"], []).append(row["seq"])
        if include_records:
            all_seqs = sorted({seq for seqs in seq_by_key.values() for seq in seqs})
            records: dict[int, dict[str, object]] = {}
            if all_seqs:
                records = {
                    r["seq"]: {**dict(r), "payload": loads(r["payload"])}
                    for r in self.conn.execute(
                        "SELECT seq, event_id, entity, source, op, event_time, arrival_time, payload "
                        "FROM event_log WHERE seq IN (%s) ORDER BY seq"
                        % ",".join("?" for _ in all_seqs),
                        all_seqs,
                    ).fetchall()
                }
            for payload_roles in result.values():
                for role, seqs in payload_roles.items():
                    payload_roles[role] = [records[seq] for seq in seqs]
        return result

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
