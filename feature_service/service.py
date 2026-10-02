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
PROPOSED = "proposed"
REJECTED = "rejected"
PUBLISHED = "published"


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
            "SELECT feature_key, value, status, sealed_at, confirmation_id "
            "FROM sample_features "
            "WHERE sample_id = ? AND feature_name = ? AND version = ?",
            (sample_id, feature_name, version),
        ).fetchall()
        return {
            r["feature_key"]: {
                "value": loads(r["value"]),
                "status": r["status"],
                "sealed_at": r["sealed_at"],
                "confirmation_id": r["confirmation_id"],
            }
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
        confirmation_id: str | None = None,
        additional_ranges: dict[str, list[tuple[int, int]]] | None = None,
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
        if seal and only_keys is not None:
            sample_row = self._sample(sample_id)
            effective_status = {}
            for key, _ in targets:
                key_range = self._key_ranges(sample_row, spec)[key]
                if key_range[0] is None:
                    effective_status[key] = SEALED
                    continue
                covered = [
                    (r["range_start"], r["range_end"])
                    for r in self._confirmation_items(sample_id, spec, key)
                ]
                covered.extend((additional_ranges or {}).get(key, []))
                if not additional_ranges or key not in (additional_ranges or {}):
                    covered.append((key_range[0], key_range[1]))
                effective_status[key] = (
                    SEALED if not self._uncovered_parts(
                        (key_range[0], key_range[1]), covered
                    ) else OPEN
                )
        else:
            effective_status = {key: status for key, _ in targets}
        self.conn.executemany(
            "INSERT INTO sample_features "
            "(sample_id, feature_name, version, feature_key, value, computed_at, "
            "status, sealed_at, confirmation_id) "
            "VALUES (?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(sample_id, feature_name, version, feature_key) "
            "DO UPDATE SET value = excluded.value, computed_at = excluded.computed_at, "
            "status = excluded.status, sealed_at = excluded.sealed_at, "
            "confirmation_id = COALESCE(excluded.confirmation_id, "
            "sample_features.confirmation_id)",
            [
                (sample_id, spec.name, version, key, dumps(value), now,
                 effective_status[key], sealed_at if seal else None,
                 confirmation_id if seal else None)
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
        confirmation_id: str | None = None,
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
        if confirmation_id is None:
            confirmation_id = self._next_confirmation_id()
        self.conn.execute(
            "INSERT OR IGNORE INTO confirmations "
            "(confirmation_id, feature_name, version, confirmed_at, scope, note) "
            "VALUES (?,?,?,?,?,?)",
            (confirmation_id, feature_name, version, sealed_at,
             dumps({"sample_ids": [sample_id], "feature_keys": sorted(targets)}),
             "seal"),
        )
        self._store_results(
            sample_id, spec, explained,
            only_keys=set(targets), seal=True, sealed_at=sealed_at,
            confirmation_id=confirmation_id,
        )
        key_ranges = self._key_ranges(sample, spec)
        self.conn.executemany(
            "INSERT INTO confirmation_items "
            "(sample_id, feature_name, version, feature_key, confirmation_id, "
            "range_start, range_end, evidence) VALUES (?,?,?,?,?,?,?,?)",
            [
                (sample_id, feature_name, version, key, confirmation_id,
                 key_ranges[key][0], key_ranges[key][1],
                 dumps(explained["evidence"][key]))
                for key in targets
            ],
        )
        self.conn.commit()
        return sorted(targets)

    def _next_confirmation_id(self) -> str:
        row = self.conn.execute(
            "SELECT COUNT(*) AS n FROM confirmations"
        ).fetchone()
        return f"confirmation-{row['n'] + 1}"

    def _key_ranges(self, sample, spec: FeatureSpec) -> dict[str, tuple[int | None, int]]:
        return {
            key: (sample["as_of"] - window if window is not None else None,
                  sample["as_of"])
            for key, (window, _agg) in self._key_meta(spec).items()
        }

    def _key_meta(self, spec: FeatureSpec) -> dict[str, tuple[int | None, str]]:
        meta = {
            f"{spec.name}_w{window}_{agg}": (window, agg)
            for window in spec.windows for agg in spec.aggregations
        }
        if spec.state_source is not None:
            meta[f"{spec.name}_last_state"] = (None, "state")
        return meta

    def _confirmation_items(self, sample_id: str, spec: FeatureSpec, feature_key: str):
        return self.conn.execute(
            "SELECT confirmation_id, range_start, range_end, evidence, "
            "(SELECT confirmed_at FROM confirmations c "
            "WHERE c.confirmation_id = confirmation_items.confirmation_id) AS confirmed_at "
            "FROM confirmation_items "
            "WHERE sample_id = ? AND feature_name = ? AND version = ? "
            "AND feature_key = ? ORDER BY confirmed_at, range_start, range_end",
            (sample_id, spec.name, spec.version, feature_key),
        ).fetchall()

    @staticmethod
    def _union_intervals(intervals: list[tuple[int, int]]) -> list[tuple[int, int]]:
        if not intervals:
            return []
        ordered = sorted((int(a), int(b)) for a, b in intervals if a is not None and a < b)
        merged = [ordered[0]]
        for start, end in ordered[1:]:
            last_start, last_end = merged[-1]
            if start <= last_end:
                merged[-1] = (last_start, max(last_end, end))
            else:
                merged.append((start, end))
        return merged

    def _uncovered_parts(
        self, full_range: tuple[int, int], covered: list[tuple[int, int]]
    ) -> list[tuple[int, int]]:
        start, end = full_range
        parts = []
        cursor = start
        for covered_start, covered_end in self._union_intervals(covered):
            if covered_end <= cursor:
                continue
            if covered_start >= end:
                break
            if covered_start > cursor:
                parts.append((cursor, min(covered_start, end)))
            cursor = max(cursor, min(covered_end, end))
            if cursor >= end:
                break
        if cursor < end:
            parts.append((cursor, end))
        return parts

    def _records_for_seqs(self, seqs: list[int]):
        if not seqs:
            return []
        return self.conn.execute(
            "SELECT seq, event_id, entity, source, op, event_time, arrival_time, payload "
            "FROM event_log WHERE seq IN (%s) ORDER BY seq"
            % ",".join("?" for _ in seqs),
            seqs,
        ).fetchall()

    def _composite_result(self, sample, spec: FeatureSpec, feature_key: str,
                          explained: dict[str, object]):
        meta = self._key_meta(spec)[feature_key]
        window, agg_name = meta
        items = self._confirmation_items(sample["sample_id"], spec, feature_key)
        if window is None:
            if not items:
                value = explained["values"][feature_key]
            else:
                item = items[-1]
                evidence = loads(item["evidence"]) or explained["evidence"][feature_key]
                seqs = evidence.get("state", [])
                value = None
                if seqs:
                    record = self._log_record(seqs[-1])
                    value = loads(record["payload"])[spec.state_field]
            return value, [
                {
                    "range_start": None,
                    "range_end": sample["as_of"],
                    "status": SEALED if items else OPEN,
                    "confirmation_id": items[-1]["confirmation_id"] if items else None,
                }
            ]
        full_range = (sample["as_of"] - window, sample["as_of"])
        values = []
        priority_items = sorted(
            items,
            key=lambda r: (
                r["confirmed_at"],
                r["range_start"] is not None,
                r["range_start"] if r["range_start"] is not None else -10**18,
                r["range_end"],
            ),
            reverse=True,
        )
        owned: list[tuple[int, int, object]] = []
        claimed: list[tuple[int, int]] = []
        for item in priority_items:
            start, end = item["range_start"], item["range_end"]
            fresh_parts = self._uncovered_parts((start, end), claimed)
            claimed = self._union_intervals(claimed + fresh_parts)
            evidence = loads(item["evidence"])
            if not evidence:
                evidence = self._evidence_sets(
                    sample["sample_id"], spec.name, spec.version, feature_key
                )
            seqs = evidence.get("behavior", [])
            record_times: dict[int, int] = {}
            for record in self._records_for_seqs(seqs):
                record_times[record["seq"]] = record["event_time"]
            for owned_start, owned_end in fresh_parts:
                owned.append((owned_start, owned_end, item))
                for seq in seqs:
                    if owned_start <= record_times.get(seq, -10**18) < owned_end:
                        values.append(float(
                            loads(self._log_record(seq)["payload"])[spec.value_field]
                        ))
        occupied = claimed
        current_evidence = explained["evidence"][feature_key].get("behavior", [])
        for record in self._records_for_seqs(current_evidence):
            if any(start <= record["event_time"] < end for start, end in occupied):
                continue
            values.append(float(loads(record["payload"])[spec.value_field]))
        parts = [
            {
                "range_start": start,
                "range_end": end,
                "status": SEALED,
                "confirmation_id": item["confirmation_id"],
            }
            for start, end, item in sorted(owned, key=lambda x: (x[0], x[1]))
        ]
        for start, end in self._uncovered_parts(full_range, occupied):
            parts.append({
                "range_start": start,
                "range_end": end,
                "status": OPEN,
                "confirmation_id": None,
            })
        agg = get_aggregation(agg_name)
        return (agg.compute(values) if values else agg.empty), sorted(
            parts, key=lambda p: ((p["range_start"] is None, p["range_start"]), p["range_end"])
        )

    def confirm_scope(
        self,
        feature_name: str,
        sample_ids: list[str] | tuple[str, ...],
        *,
        version: int | None = None,
        time_ranges: list[tuple[int, int]] | tuple[tuple[int, int], ...] | None = None,
        confirmation_id: str | None = None,
        confirmed_at: int | None = None,
        batch: bool = False,
        note: str | None = None,
    ) -> dict[str, object]:
        """一次只确认若干样本及若干时间窗口；未命中的结果仍开放。

        ``time_ranges`` 与具体特征结果的覆盖区间精确匹配；长窗口与短窗口可以
        分属不同确认批次，因此后续确认可与先前批次在时间上交叠。每条物化结果
        只有一个归属批次，同一来源在多个结果中出现时按各结果独立归约，不会
        在同一结果内重复。
        """
        spec = self._get_spec(feature_name, version)
        if not sample_ids:
            raise ValueError("sample_ids 不能为空")
        if confirmed_at is None:
            confirmed_at = int(time.time() * 1000)
        if confirmation_id is None:
            confirmation_id = self._next_confirmation_id()
        if self.conn.execute(
            "SELECT 1 FROM confirmations WHERE confirmation_id = ?",
            (confirmation_id,),
        ).fetchone():
            raise ValueError(f"确认批次已存在: {confirmation_id!r}")

        wanted_ranges = None if time_ranges is None else [
            (int(start), int(end)) for start, end in time_ranges
        ]
        selected: dict[str, dict[str, list[tuple[int, int]]]] = {}
        scope = []
        for sample_id in sample_ids:
            sample = self._sample(sample_id)
            if sample["entity_type"] != spec.entity:
                raise ValueError(
                    f"样本 {sample_id!r} 实体类型与特征 {feature_name!r} 不一致"
                )
            stored = self._stored_rows(sample_id, feature_name, spec.version)
            if not stored:
                raise KeyError(f"样本 {sample_id!r} 尚未物化 {feature_name!r}")
            key_ranges = self._key_ranges(sample, spec)
            keys = []
            for key in sorted(stored):
                full_range = key_ranges.get(key)
                candidate_ranges = [full_range] if wanted_ranges is None else wanted_ranges
                intervals = []
                if full_range[0] is not None:
                    for wanted_start, wanted_end in candidate_ranges:
                        start = max(full_range[0], wanted_start)
                        end = min(full_range[1], wanted_end)
                        if start < end:
                            intervals.append((start, end))
                elif wanted_ranges is None:
                    intervals = [(None, full_range[1])]
                if intervals:
                    selected.setdefault(sample_id, {})
                    keys.append(key)
                    selected[sample_id][key] = intervals
                    for interval_start, interval_end in intervals:
                        scope.append({
                            "sample_id": sample_id,
                            "feature_key": key,
                            "range_start": interval_start,
                            "range_end": interval_end,
                        })

        self.conn.execute(
            "INSERT INTO confirmations "
            "(confirmation_id, feature_name, version, confirmed_at, scope, note) "
            "VALUES (?,?,?,?,?,?)",
            (confirmation_id, feature_name, spec.version, confirmed_at,
             dumps(scope), note),
        )
        confirmed_items = []
        for sample_id, key_intervals in selected.items():
            sample = self._sample(sample_id)
            explained = self._explain(sample, spec, batch)
            intervals = {
                key: ranges for key, ranges in key_intervals.items()
            }
            self._store_results(
                sample_id, spec, explained, only_keys=set(intervals), seal=True,
                sealed_at=confirmed_at, confirmation_id=confirmation_id,
                additional_ranges=intervals,
            )
            key_ranges = self._key_ranges(sample, spec)
            self.conn.executemany(
                "INSERT INTO confirmation_items "
                "(sample_id, feature_name, version, feature_key, confirmation_id, "
                "range_start, range_end, evidence) VALUES (?,?,?,?,?,?,?,?)",
                [
                    (sample_id, feature_name, spec.version, key,
                     confirmation_id, interval_start, interval_end,
                     dumps(explained["evidence"][key]))
                    for key, ranges in intervals.items()
                    for interval_start, interval_end in ranges
                ],
            )
            confirmed_items.extend(
                (sample_id, key) for key in intervals
            )
        self.conn.commit()
        return {
            "confirmation_id": confirmation_id,
            "items": [
                {"sample_id": sample_id, "feature_key": key}
                for sample_id, key in confirmed_items
            ],
        }

    def seal_status(
        self, sample_id: str, feature_name: str, version: int
    ) -> dict[str, str]:
        """返回每个 feature_key 的开放/封存状态。"""
        stored = self._stored_rows(sample_id, feature_name, version)
        if not stored:
            raise KeyError(f"样本 {sample_id!r} 尚未物化 {feature_name!r} v{version}")
        return {key: row["status"] for key, row in sorted(stored.items())}

    # ---- 确认范围查询与独立修订 ---------------------------------------

    def get_results(
        self,
        sample_id: str,
        feature_name: str,
        version: int,
        *,
        view: str = "current",
    ) -> dict[str, object]:
        """按结果返回值、确认归属、可更新部分及修订归属。

        ``view='current'``：已发布修订替换原确认值，但开放结果仍可更新；
        ``view='original'``：只看原封存视角，未发布/未采纳修订绝不提前生效；
        ``view='open_only'``：只返回仍开放范围。
        """
        if view not in ("current", "original", "open_only"):
            raise ValueError("view 必须是 current、original 或 open_only")
        stored = self._stored_rows(sample_id, feature_name, version)
        if not stored:
            raise KeyError(f"样本 {sample_id!r} 尚未物化 {feature_name!r} v{version}")
        sample = self._sample(sample_id)
        spec = self._get_spec(feature_name, version)
        explained = self._explain(sample, spec, False)
        revision_rows = self.conn.execute(
            "SELECT ri.feature_key, ri.proposal_id, ri.new_value, rp.status "
            "FROM revision_items ri JOIN revision_proposals rp "
            "ON ri.proposal_id = rp.proposal_id "
            "WHERE ri.sample_id = ? AND ri.feature_name = ? AND ri.version = ? "
            "AND rp.status = 'published' ORDER BY ri.proposal_id DESC",
            (sample_id, feature_name, version),
        ).fetchall()
        latest_revision = {r["feature_key"]: r for r in revision_rows}
        result = {}
        for key in sorted(stored):
            stored_row = stored[key]
            value, ranges = self._composite_result(sample, spec, key, explained)
            fully_confirmed = bool(ranges) and all(p["status"] == SEALED for p in ranges)
            latest_confirmation = next(
                (p["confirmation_id"] for p in reversed(ranges)
                 if p["status"] == SEALED),
                stored_row["confirmation_id"],
            )
            status = SEALED if fully_confirmed else OPEN
            if view == "open_only" and fully_confirmed:
                continue
            revision = latest_revision.get(key)
            revision_id = None
            if fully_confirmed and view == "current" and revision is not None:
                value = loads(revision["new_value"])
                revision_id = revision["proposal_id"]
            elif view == "original":
                value, ranges = self._composite_result(sample, spec, key, explained)
            result[key] = {
                "value": value,
                "status": status,
                "confirmation_id": latest_confirmation,
                "revision_id": revision_id,
                "updatable": not fully_confirmed and view != "original",
                "open_ranges": [
                    [p["range_start"], p["range_end"]]
                    for p in ranges if p["status"] == OPEN
                ],
                "confirmed_ranges": [
                    {
                        "start": p["range_start"],
                        "end": p["range_end"],
                        "confirmation_id": p["confirmation_id"],
                    }
                    for p in ranges if p["status"] == SEALED
                ],
            }
        return result

    def get_confirmation(self, confirmation_id: str) -> dict[str, object]:
        row = self.conn.execute(
            "SELECT * FROM confirmations WHERE confirmation_id = ?",
            (confirmation_id,),
        ).fetchone()
        if row is None:
            raise KeyError(f"确认批次不存在: {confirmation_id!r}")
        items = [
            dict(r)
            for r in self.conn.execute(
                "SELECT sample_id, feature_name, version, feature_key, "
                "range_start, range_end FROM confirmation_items "
                "WHERE confirmation_id = ? ORDER BY sample_id, feature_key",
                (confirmation_id,),
            ).fetchall()
        ]
        return {
            "confirmation_id": confirmation_id,
            "feature_name": row["feature_name"],
            "version": row["version"],
            "confirmed_at": row["confirmed_at"],
            "scope": loads(row["scope"]),
            "note": row["note"],
            "items": items,
        }

    def propose_revision(
        self,
        proposal_id: str,
        feature_name: str,
        items: list[tuple[str, str]] | tuple[tuple[str, str], ...],
        downstream: list[str] | tuple[str, ...],
        *,
        version: int | None = None,
        proposed_at: int | None = None,
        batch: bool = False,
        note: str | None = None,
    ) -> dict[str, object]:
        """对已封存结果登记独立修订提案，但不改变当前确认视角。"""
        spec = self._get_spec(feature_name, version)
        if not items:
            raise ValueError("修订 items 不能为空")
        if self.conn.execute(
            "SELECT 1 FROM revision_proposals WHERE proposal_id = ?",
            (proposal_id,),
        ).fetchone():
            raise ValueError(f"修订提案已存在: {proposal_id!r}")
        if proposed_at is None:
            proposed_at = int(time.time() * 1000)

        details = []
        for sample_id, feature_key in items:
            sample = self._sample(sample_id)
            stored = self._stored_rows(sample_id, feature_name, spec.version)
            if feature_key not in stored:
                raise KeyError(f"{sample_id!r}/{feature_key} 尚未物化")
            if stored[feature_key]["status"] != SEALED:
                raise ValueError(f"{sample_id!r}/{feature_key} 未封存，不能提修订")
            explained = self._explain(sample, spec, batch)
            old_value = stored[feature_key]["value"]
            new_value = explained["values"][feature_key]
            details.append({
                "sample_id": sample_id,
                "feature_key": feature_key,
                "old_value": old_value,
                "new_value": new_value,
                "old_evidence": self._evidence_sets(
                    sample_id, feature_name, spec.version, feature_key
                ),
                "new_evidence": explained["evidence"][feature_key],
            })

        self.conn.execute(
            "INSERT INTO revision_proposals "
            "(proposal_id, feature_name, version, status, proposed_at, "
            "downstream, note) VALUES (?,?,?,?,?,?,?)",
            (proposal_id, feature_name, spec.version, PROPOSED, proposed_at,
             dumps(list(downstream)), note),
        )
        self.conn.executemany(
            "INSERT INTO revision_items "
            "(proposal_id, sample_id, feature_name, version, feature_key, "
            "old_value, new_value) VALUES (?,?,?,?,?,?,?)",
            [
                (proposal_id, d["sample_id"], feature_name, spec.version,
                 d["feature_key"], dumps(d["old_value"]), dumps(d["new_value"]))
                for d in details
            ],
        )
        self.conn.executemany(
            "INSERT INTO revision_evidence "
            "(proposal_id, sample_id, feature_name, version, feature_key, seq, role) "
            "VALUES (?,?,?,?,?,?,?)",
            [
                (proposal_id, d["sample_id"], feature_name, spec.version,
                 d["feature_key"], seq, role)
                for d in details for role, seqs in d["new_evidence"].items()
                for seq in seqs
            ],
        )
        self.conn.commit()
        return self.get_revision(proposal_id)

    def _evidence_sets(self, sample_id, feature_name, version, feature_key):
        out = {"behavior": [], "state": []}
        rows = self.conn.execute(
            "SELECT seq, role FROM sample_feature_evidence "
            "WHERE sample_id = ? AND feature_name = ? AND version = ? "
            "AND feature_key = ? ORDER BY role, seq",
            (sample_id, feature_name, version, feature_key),
        ).fetchall()
        for row in rows:
            out[row["role"]].append(row["seq"])
        return out

    def decide_revision(self, proposal_id: str, *, publish: bool, decided_at: int | None = None) -> str:
        """采纳时发布独立修订；拒绝时保留提案但确认视角不变。"""
        row = self.conn.execute(
            "SELECT status FROM revision_proposals WHERE proposal_id = ?",
            (proposal_id,),
        ).fetchone()
        if row is None:
            raise KeyError(f"修订提案不存在: {proposal_id!r}")
        if row["status"] != PROPOSED:
            raise ValueError("只有 proposed 状态的修订可以决定")
        status = PUBLISHED if publish else REJECTED
        if decided_at is None:
            decided_at = int(time.time() * 1000)
        self.conn.execute(
            "UPDATE revision_proposals SET status = ?, decided_at = ? "
            "WHERE proposal_id = ?",
            (status, decided_at, proposal_id),
        )
        self.conn.commit()
        return status

    def get_revision(self, proposal_id: str) -> dict[str, object]:
        row = self.conn.execute(
            "SELECT * FROM revision_proposals WHERE proposal_id = ?",
            (proposal_id,),
        ).fetchone()
        if row is None:
            raise KeyError(f"修订提案不存在: {proposal_id!r}")
        items = [
            {
                "sample_id": r["sample_id"],
                "feature_key": r["feature_key"],
                "old_value": loads(r["old_value"]),
                "new_value": loads(r["new_value"]),
            }
            for r in self.conn.execute(
                "SELECT * FROM revision_items WHERE proposal_id = ? "
                "ORDER BY sample_id, feature_key",
                (proposal_id,),
            ).fetchall()
        ]
        return {
            "proposal_id": proposal_id,
            "feature_name": row["feature_name"],
            "version": row["version"],
            "status": row["status"],
            "proposed_at": row["proposed_at"],
            "decided_at": row["decided_at"],
            "downstream": loads(row["downstream"]),
            "note": row["note"],
            "items": items,
        }

    def list_revisions(
        self, feature_name: str | None = None, version: int | None = None
    ) -> list[dict[str, object]]:
        sql = "SELECT proposal_id FROM revision_proposals"
        conditions = []
        params = []
        if feature_name is not None:
            conditions.append("feature_name = ?")
            params.append(feature_name)
        if version is not None:
            conditions.append("version = ?")
            params.append(version)
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        sql += " ORDER BY proposed_at, proposal_id"
        return [
            self.get_revision(row["proposal_id"])
            for row in self.conn.execute(sql, params).fetchall()
        ]

    def get_revision_evidence(
        self, proposal_id: str, *, include_records: bool = True
    ) -> dict[str, dict[str, list[object]]]:
        """返回已采纳/未采纳修订在提出时固定的新血缘与可见资料。"""
        proposal = self.get_revision(proposal_id)
        result = {
            item["feature_key"]: {"behavior": [], "state": []}
            for item in proposal["items"]
        }
        rows = self.conn.execute(
            "SELECT sample_id, feature_key, seq, role FROM revision_evidence "
            "WHERE proposal_id = ? ORDER BY sample_id, feature_key, role, seq",
            (proposal_id,),
        ).fetchall()
        for row in rows:
            result.setdefault(row["feature_key"], {"behavior": [], "state": []})
            result[row["feature_key"]][row["role"]].append(row["seq"])
        if include_records:
            seqs = sorted({r["seq"] for r in rows})
            records = {}
            if seqs:
                records = {
                    r["seq"]: {**dict(r), "payload": loads(r["payload"])}
                    for r in self.conn.execute(
                        "SELECT seq, event_id, entity, source, op, event_time, "
                        "arrival_time, payload FROM event_log WHERE seq IN (%s) "
                        "ORDER BY seq" % ",".join("?" for _ in seqs),
                        seqs,
                    ).fetchall()
                }
            for roles in result.values():
                for role in roles:
                    roles[role] = [records[seq] for seq in roles[role]]
        return result

    def get_evidence(
        self, sample_id: str, feature_name: str, version: int, *, include_records: bool = True
    ) -> dict[str, dict[str, list[object]]]:
        """返回冻结特征值使用的账本 seq，可附带原始记录用于重启后审计。"""
        return self.get_result_evidence(
            sample_id, feature_name, version,
            view="current", include_records=include_records,
        )

    def get_result_evidence(
        self,
        sample_id: str,
        feature_name: str,
        version: int,
        *,
        view: str = "current",
        include_records: bool = True,
    ) -> dict[str, dict[str, list[object]]]:
        """返回当前合并视角下各结果分片去重后的血缘。"""
        sample = self._sample(sample_id)
        spec = self._get_spec(feature_name, version)
        explained = self._explain(sample, spec, False)
        results = self.get_results(sample_id, feature_name, version, view=view)
        output = {}
        for key, item in results.items():
            if item.get("revision_id"):
                output[key] = self.get_revision_evidence(
                    item["revision_id"], include_records=False
                ).get(key, {"behavior": [], "state": []})
                continue
            if self._key_meta(spec)[key][0] is None:
                confirmation_id = item.get("confirmation_id")
                evidence = {"behavior": [], "state": []}
                if confirmation_id:
                    row = self.conn.execute(
                        "SELECT evidence FROM confirmation_items "
                        "WHERE sample_id=? AND feature_key=? AND confirmation_id=? "
                        "ORDER BY range_end DESC LIMIT 1",
                        (sample_id, key, confirmation_id),
                    ).fetchone()
                    evidence = loads(row["evidence"]) if row else evidence
                else:
                    evidence = explained["evidence"][key]
                output[key] = evidence
                continue
            selected_seqs: set[int] = set()
            for part in item["confirmed_ranges"]:
                rows = self._confirmation_items(sample_id, spec, key)
                for row in rows:
                    if row["confirmation_id"] != part["confirmation_id"]:
                        continue
                    overlap_start = max(part["start"], row["range_start"])
                    overlap_end = min(part["end"], row["range_end"])
                    if overlap_start >= overlap_end:
                        continue
                    for seq in loads(row["evidence"]).get("behavior", []):
                        record = self._log_record(seq)
                        if overlap_start <= record["event_time"] < overlap_end:
                            selected_seqs.add(seq)
            current_seqs = {
                seq for seq in explained["evidence"][key].get("behavior", [])
                if any(
                    start <= self._log_record(seq)["event_time"] < end
                    for start, end in [tuple(r) for r in item["open_ranges"]]
                )
            }
            output[key] = {
                "behavior": sorted(selected_seqs | current_seqs),
                "state": [],
            }
        if include_records:
            all_seqs = sorted({
                seq for roles in output.values()
                for role_seqs in roles.values() for seq in role_seqs
            })
            records = {r["seq"]: {**dict(r), "payload": loads(r["payload"])}
                       for r in self._records_for_seqs(all_seqs)}
            output = {
                key: {
                    role: [records[seq] for seq in seqs]
                    for role, seqs in roles.items()
                }
                for key, roles in output.items()
            }
        return output

    def get_confirmation_evidence(
        self, confirmation_id: str, *, include_records: bool = True
    ) -> dict[str, dict[str, list[object]]]:
        """返回某次确认在其声明范围内封存时固定的资料血缘。"""
        rows = self.conn.execute(
            "SELECT sample_id, feature_key, range_start, range_end, evidence "
            "FROM confirmation_items "
            "WHERE confirmation_id = ? ORDER BY sample_id, feature_key, range_start",
            (confirmation_id,),
        ).fetchall()
        output: dict[str, dict[str, list[object]]] = {}
        all_seqs: set[int] = set()
        for row in rows:
            evidence = loads(row["evidence"])
            merged = output.setdefault(row["feature_key"], {"behavior": [], "state": []})
            scoped_seqs = set(evidence.get("behavior", []))
            if row["range_start"] is not None:
                scoped_seqs = {
                    seq for seq in scoped_seqs
                    if row["range_start"] <= self._log_record(seq)["event_time"] < row["range_end"]
                }
            for role in ("behavior", "state"):
                candidate = sorted(scoped_seqs) if role == "behavior" else evidence.get(role, [])
                for seq in candidate:
                    if seq not in merged[role]:
                        merged[role].append(seq)
                        all_seqs.add(seq)
        if include_records:
            records = {r["seq"]: {**dict(r), "payload": loads(r["payload"])}
                       for r in self._records_for_seqs(sorted(all_seqs))}
            output = {
                key: {role: [records[seq] for seq in seqs]
                      for role, seqs in roles.items()}
                for key, roles in output.items()
            }
        return output

    def get_features(self, sample_id: str, feature_name: str, version: int) -> dict[str, object]:
        """读取当前视角：已封存分片冻结，仍开放分片随当时可见资料更新。"""
        return {
            key: item["value"]
            for key, item in self.get_results(
                sample_id, feature_name, version, view="current"
            ).items()
        }

    def close(self) -> None:
        self.conn.close()
