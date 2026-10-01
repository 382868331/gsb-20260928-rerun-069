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

OPEN = "open"
SEALED = "sealed"


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

    # ---- 物化：开放结果随可见修订更新，封存结果保持原样 -----------------

    def _explain(self, sample, spec: FeatureSpec, batch: bool) -> dict[str, object]:
        if batch:
            return self.explain_batch(sample["entity_id"], spec, sample["as_of"])
        return self.explain_incremental(sample["entity_id"], spec, sample["as_of"])

    def _stored_rows(
        self, sample_id: str, feature_name: str, version: int
    ) -> dict[str, dict[str, object]]:
        rows = self.conn.execute(
            "SELECT feature_key, value, status FROM sample_features "
            "WHERE sample_id = ? AND feature_name = ? AND version = ?",
            (sample_id, feature_name, version),
        ).fetchall()
        return {
            r["feature_key"]: {"value": loads(r["value"]), "status": r["status"]}
            for r in rows
        }

    def _store_results(
        self,
        sample_id: str,
        spec: FeatureSpec,
        explained: dict[str, object],
        *,
        only_keys: set[str] | None = None,
        seal: bool = False,
        sealed_at: int | None = None,
    ) -> None:
        """把计算结果写入开放范围；已封存的 feature_key 永不改写。"""
        version = spec.version
        sealed_keys = {
            key for key, row in self._stored_rows(sample_id, spec.name, version).items()
            if row["status"] == SEALED
        }
        targets = [
            (key, value)
            for key, value in explained["values"].items()
            if key not in sealed_keys and (only_keys is None or key in only_keys)
        ]
        if not targets:
            return
        now = int(time.time() * 1000)
        status = SEALED if seal else OPEN
        self.conn.executemany(
            "INSERT INTO sample_features "
            "(sample_id, feature_name, version, feature_key, value, computed_at, "
            "status, sealed_at) "
            "VALUES (?,?,?,?,?,?,?,?) "
            "ON CONFLICT(sample_id, feature_name, version, feature_key) "
            "DO UPDATE SET value = excluded.value, computed_at = excluded.computed_at, "
            "status = excluded.status, sealed_at = excluded.sealed_at",
            [
                (sample_id, spec.name, version, key, dumps(value), now,
                 status, sealed_at if seal else None)
                for key, value in targets
            ],
        )
        self.conn.executemany(
            "DELETE FROM sample_feature_evidence "
            "WHERE sample_id = ? AND feature_name = ? AND version = ? AND feature_key = ?",
            [(sample_id, spec.name, version, key) for key, _ in targets],
        )
        evidence = explained["evidence"]
        self.conn.executemany(
            "INSERT INTO sample_feature_evidence "
            "(sample_id, feature_name, version, feature_key, seq, role) "
            "VALUES (?,?,?,?,?,?)",
            [
                (sample_id, spec.name, version, key, seq, role)
                for key, _ in targets
                for role, seqs in evidence[key].items()
                for seq in seqs
            ],
        )
        self.conn.commit()

    def materialize(
        self,
        sample_id: str,
        feature_name: str,
        version: int | None = None,
        *,
        batch: bool = False,
    ) -> dict[str, object]:
        """计算并保存样本某特征版本的值。

        * 保存的结果默认是**开放**的：之后追加到账本、且当时可见
          （arrival_time <= as_of）的迟到资料/撤回/修订，会在下次
          materialize 时按影响范围更新仍未封存的 feature_key；
        * 已封存的 feature_key 保持原值与原证据，永不改写；
        * 未来才到达（arrival_time > as_of）的信息任何时候都不混入；
        * 返回当前保存的全部值（开放=最新可见结果，封存=历史原样）。
        """
        sample = self._sample(sample_id)
        spec = self._get_spec(feature_name, version)
        if sample["entity_type"] != spec.entity:
            raise ValueError(
                f"样本实体类型 {sample['entity_type']!r} 与特征实体 {spec.entity!r} 不一致"
            )
        explained = self._explain(sample, spec, batch)
        self._store_results(sample_id, spec, explained)
        return self.get_features(sample_id, feature_name, spec.version)

    # ---- 封存：先预览受影响范围，再确认封存 -----------------------------

    def _log_record(self, seq: int):
        return self.conn.execute(
            "SELECT seq, event_id, entity, source, op, event_time, arrival_time, payload "
            "FROM event_log WHERE seq = ?",
            (seq,),
        ).fetchone()

    def _diff_reasons(
        self,
        old_seqs: set[int],
        new_seqs: set[int],
        as_of: int,
    ) -> list[str]:
        """对比同一 feature_key 新旧证据，给出变化原因。"""
        reasons: set[str] = set()
        for seq in new_seqs - old_seqs:
            rec = self._log_record(seq)
            if rec["arrival_time"] > rec["event_time"]:
                reasons.add("late_arrival")  # 迟到资料
            earlier = self.conn.execute(
                "SELECT 1 FROM event_log WHERE event_id = ? AND source = ? AND seq < ? "
                "LIMIT 1",
                (rec["event_id"], rec["source"], seq),
            ).fetchone()
            if earlier is not None:
                reasons.add("revision")  # 同一业务事件的修订版本
            elif rec["arrival_time"] <= rec["event_time"]:
                reasons.add("new_data")
        for seq in old_seqs - new_seqs:
            rec = self._log_record(seq)
            later = self.conn.execute(
                "SELECT op FROM event_log "
                "WHERE event_id = ? AND source = ? AND seq > ? AND arrival_time <= ? "
                "ORDER BY seq DESC LIMIT 1",
                (rec["event_id"], rec["source"], seq, as_of),
            ).fetchone()
            if later is None:
                reasons.add("window_boundary")  # 记录落出窗口边界
            elif later["op"] == RETRACT:
                reasons.add("retract")  # 状态/行为撤回
            else:
                reasons.add("revision")
        return sorted(reasons)

    def preview_seal(
        self,
        sample_id: str,
        feature_name: str,
        version: int | None = None,
        *,
        batch: bool = False,
    ) -> dict[str, object]:
        """封存前的变化预览：按 feature_key 列出开放结果的 stored/current 对照。

        返回每个仍开放的 feature_key 的窗口、聚合、已保存值、按当前账本
        重算的值、是否变化及变化原因（late_arrival / retract / revision /
        window_boundary / new_data）。已封存范围不参与预览，保持原样。
        """
        sample = self._sample(sample_id)
        spec = self._get_spec(feature_name, version)
        version = spec.version
        stored = self._stored_rows(sample_id, feature_name, version)
        explained = self._explain(sample, spec, batch)
        current_values: dict[str, object] = explained["values"]
        current_evidence: dict[str, dict[str, list[int]]] = explained["evidence"]

        old_evidence: dict[str, set[int]] = {}
        for row in self.conn.execute(
            "SELECT feature_key, seq FROM sample_feature_evidence "
            "WHERE sample_id = ? AND feature_name = ? AND version = ?",
            (sample_id, feature_name, version),
        ).fetchall():
            old_evidence.setdefault(row["feature_key"], set()).add(row["seq"])

        key_meta: dict[str, tuple[int | None, str]] = {}
        for window in spec.windows:
            for agg in spec.aggregations:
                key_meta[f"{spec.name}_w{window}_{agg}"] = (window, agg)
        if spec.state_source is not None:
            key_meta[f"{spec.name}_last_state"] = (None, "state")

        open_keys = {k for k, r in stored.items() if r["status"] == OPEN}
        missing = object()
        entries = []
        for key in sorted(open_keys | set(current_values)):
            old = stored[key]["value"] if key in open_keys else missing
            new = current_values.get(key, missing)
            changed = old != new
            reasons: list[str] = []
            if changed:
                if old is missing:
                    reasons = ["new_data"]
                elif new is missing:
                    reasons = ["window_boundary"]
                else:
                    new_seqs = {
                        seq for seqs in current_evidence.get(key, {}).values()
                        for seq in seqs
                    }
                    reasons = self._diff_reasons(
                        old_evidence.get(key, set()), new_seqs, sample["as_of"]
                    )
            window, agg = key_meta.get(key, (None, None))
            entries.append({
                "feature_key": key,
                "window": window,
                "aggregation": agg,
                "stored": None if old is missing else old,
                "current": None if new is missing else new,
                "changed": changed,
                "reasons": reasons,
            })
        return {
            "sample_id": sample_id,
            "feature_name": feature_name,
            "version": version,
            "as_of": sample["as_of"],
            "entries": entries,
            "sealed": sorted(k for k, r in stored.items() if r["status"] == SEALED),
        }

    def seal(
        self,
        sample_id: str,
        feature_name: str,
        version: int | None = None,
        feature_keys: list[str] | tuple[str, ...] | None = None,
        *,
        batch: bool = False,
        sealed_at: int | None = None,
    ) -> list[str]:
        """确认封存：把指定的开放 feature_key 冻结为当前可见结果。

        * feature_keys=None 封存全部仍开放的范围；
        * 封存值取封存时点当时可见（arrival_time <= as_of）的计算结果，
          与 preview_seal 展示的 current 一致；
        * 已封存的 key 再次指定会报错，封存历史永不改写；
        * 封存决定与历史值持久化，重启接续后仍然有效。
        """
        sample = self._sample(sample_id)
        spec = self._get_spec(feature_name, version)
        version = spec.version
        stored = self._stored_rows(sample_id, feature_name, version)
        if not stored:
            raise KeyError(
                f"样本 {sample_id!r} 尚未物化 {feature_name!r} v{version}，无可封存结果"
            )
        open_keys = {k for k, r in stored.items() if r["status"] == OPEN}
        sealed_keys = {k for k, r in stored.items() if r["status"] == SEALED}
        if feature_keys is None:
            targets = sorted(open_keys)
        else:
            targets = []
            for key in feature_keys:
                if key in sealed_keys:
                    raise ValueError(f"{key!r} 已封存，封存历史不能改写")
                if key not in open_keys:
                    raise KeyError(f"{key!r} 不是仍开放的 feature_key")
                targets.append(key)
        if not targets:
            return []
        explained = self._explain(sample, spec, batch)
        if sealed_at is None:
            sealed_at = int(time.time() * 1000)
        self._store_results(
            sample_id, spec, explained,
            only_keys=set(targets), seal=True, sealed_at=sealed_at,
        )
        return sorted(targets)

    def seal_status(
        self, sample_id: str, feature_name: str, version: int
    ) -> dict[str, str]:
        """返回每个 feature_key 的开放/封存状态。"""
        stored = self._stored_rows(sample_id, feature_name, version)
        if not stored:
            raise KeyError(f"样本 {sample_id!r} 尚未物化 {feature_name!r} v{version}")
        return {key: row["status"] for key, row in sorted(stored.items())}

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
