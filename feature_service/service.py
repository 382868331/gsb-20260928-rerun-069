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
    feature_keys,
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

    # ---- 物化：开放结果随可见修订更新，封存结果历史冻结 ------------------

    def _stored_features(self, sample_id: str, feature_name: str, version: int):
        return self.conn.execute(
            "SELECT feature_key, value, sealed FROM sample_features "
            "WHERE sample_id = ? AND feature_name = ? AND version = ?",
            (sample_id, feature_name, version),
        ).fetchall()

    def _explain(self, sample, spec: FeatureSpec, batch: bool) -> dict[str, object]:
        if batch:
            return self.explain_batch(sample["entity_id"], spec, sample["as_of"])
        return self.explain_incremental(sample["entity_id"], spec, sample["as_of"])

    def _persist_values(
        self,
        sample_id: str,
        feature_name: str,
        version: int,
        values: dict[str, object],
        evidence: dict[str, dict[str, list[int]]],
        keys: list[str],
        *,
        sealed: bool,
    ) -> None:
        """把指定 key 的值与证据落库；开放写可重复，封存写在 key 未封存时才应调用。"""
        now = int(time.time() * 1000)
        self.conn.executemany(
            "DELETE FROM sample_feature_evidence "
            "WHERE sample_id = ? AND feature_name = ? AND version = ? AND feature_key = ?",
            [(sample_id, feature_name, version, key) for key in keys],
        )
        self.conn.executemany(
            "INSERT INTO sample_features "
            "(sample_id, feature_name, version, feature_key, value, computed_at, sealed, sealed_at) "
            "VALUES (?,?,?,?,?,?,?,?) "
            "ON CONFLICT(sample_id, feature_name, version, feature_key) DO UPDATE SET "
            "value = excluded.value, computed_at = excluded.computed_at, "
            "sealed = excluded.sealed, sealed_at = excluded.sealed_at",
            [
                (sample_id, feature_name, version, key, dumps(values[key]), now,
                 1 if sealed else 0, now if sealed else None)
                for key in keys
            ],
        )
        self.conn.executemany(
            "INSERT INTO sample_feature_evidence "
            "(sample_id, feature_name, version, feature_key, seq, role) "
            "VALUES (?,?,?,?,?,?)",
            [
                (sample_id, feature_name, version, key, seq, role)
                for key in keys
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
        recompute: bool = False,
    ) -> dict[str, object]:
        """为样本物化某特征版本的值。

        * 物化结果默认是**开放**的：之后到达且对样本 as_of 可见的
          迟到事件/撤回/修订会在下次物化或读取时反映到开放 key 上；
        * 已调用 seal() 封存的 key 保持历史原值，永不改写；
        * batch=True 走整批重放路径，默认走增量路径；
        * recompute=True 只返回 {"frozen": 已存值, "current": 当前可见值}
          对照，不落库、不改写任何已存结果。
        """
        sample = self._sample(sample_id)
        spec = self._get_spec(feature_name, version)
        version = spec.version
        if sample["entity_type"] != spec.entity:
            raise ValueError(
                f"样本实体类型 {sample['entity_type']!r} 与特征实体 {spec.entity!r} 不一致"
            )

        stored = self._stored_features(sample_id, feature_name, version)
        if stored and recompute:
            current = self._explain(sample, spec, batch)["values"]
            frozen_values = {r["feature_key"]: loads(r["value"]) for r in stored}
            return {"frozen": frozen_values, "current": current}

        explained = self._explain(sample, spec, batch)
        values = explained["values"]
        evidence = explained["evidence"]
        sealed_values = {
            r["feature_key"]: loads(r["value"]) for r in stored if r["sealed"]
        }
        open_keys = [key for key in values if key not in sealed_values]
        if open_keys:
            self._persist_values(
                sample_id, feature_name, version, values, evidence, open_keys, sealed=False
            )
        # 返回合并视图：开放 key 取当前可见值，封存 key 保持历史原值。
        return {**values, **sealed_values}

    # ---- 封存：先预览受影响范围，再确认封存 -----------------------------

    @staticmethod
    def _window_of(spec: FeatureSpec, feature_key: str) -> int | None:
        for window in spec.windows:
            if feature_key.startswith(f"{spec.name}_w{window}_"):
                return window
        return None

    def _records_by_seq(self, seqs: list[int]):
        if not seqs:
            return {}
        rows = self.conn.execute(
            "SELECT seq, event_id, entity, source, op, event_time, arrival_time, payload "
            "FROM event_log WHERE seq IN (%s)"
            % ",".join("?" for _ in seqs),
            seqs,
        ).fetchall()
        return {r["seq"]: r for r in rows}

    def _diff_reasons(
        self,
        spec: FeatureSpec,
        feature_key: str,
        entity: str,
        as_of: int,
        old_evidence: dict[str, list[int]],
        new_evidence: dict[str, list[int]],
    ) -> list[dict[str, object]]:
        """对比封存前证据与当前证据，给出每个变化的来源记录与原因分类。"""
        old_seqs = set(old_evidence.get("behavior", [])) | set(old_evidence.get("state", []))
        new_seqs = set(new_evidence.get("behavior", [])) | set(new_evidence.get("state", []))
        added = sorted(new_seqs - old_seqs)
        removed = sorted(old_seqs - new_seqs)
        records = self._records_by_seq(added + removed)
        removed_by_biz = {
            (records[seq]["source"], records[seq]["event_id"]): records[seq] for seq in removed
        }
        window = self._window_of(spec, feature_key)
        reasons: list[dict[str, object]] = []
        matched_removed: set[int] = set()

        def entry(kind: str, rec) -> dict[str, object]:
            return {
                "kind": kind,
                "seq": rec["seq"],
                "event_id": rec["event_id"],
                "source": rec["source"],
                "event_time": rec["event_time"],
                "arrival_time": rec["arrival_time"],
            }

        for seq in added:
            rec = records[seq]
            prev = removed_by_biz.get((rec["source"], rec["event_id"]))
            if rec["op"] == RETRACT:
                reasons.append(entry("retract", rec))
                continue
            if prev is None:
                # 证据 diff 之外：查账本里同一业务事件是否有更早可见版本
                # （例如修订把记录搬进了本窗口，旧版本从未出现在本 key 证据里）。
                prev = self.conn.execute(
                    "SELECT seq, event_id, source, op, event_time, arrival_time "
                    "FROM event_log WHERE entity = ? AND source = ? AND event_id = ? "
                    "AND seq < ? AND arrival_time <= ? ORDER BY seq DESC LIMIT 1",
                    (entity, rec["source"], rec["event_id"], seq, as_of),
                ).fetchone()
            else:
                matched_removed.add(prev["seq"])
            if prev is not None:
                reasons.append(entry("revision", rec))
                crosses = False
                if window is not None and prev["event_time"] != rec["event_time"]:
                    lo, hi = as_of - window, as_of
                    crosses = (lo <= prev["event_time"] < hi) != (lo <= rec["event_time"] < hi)
                if crosses:
                    reasons.append(
                        {**entry("boundary_shift", rec),
                         "previous_event_time": prev["event_time"]}
                    )
            else:
                reasons.append(entry("late_arrival", rec))
        for seq in removed:
            if seq in matched_removed:
                continue
            rec = records[seq]
            successor = self.conn.execute(
                "SELECT seq, event_id, source, op, event_time, arrival_time FROM event_log "
                "WHERE entity = ? AND source = ? AND event_id = ? AND seq > ? "
                "AND arrival_time <= ? ORDER BY seq DESC LIMIT 1",
                (entity, rec["source"], rec["event_id"], seq, as_of),
            ).fetchone()
            if successor is not None and successor["op"] == "UPSERT":
                reasons.append(entry("revision", successor))
            else:
                reasons.append(entry("retract", successor if successor is not None else rec))
        return reasons

    def preview_seal(
        self,
        sample_id: str,
        feature_name: str,
        version: int | None = None,
        *,
        batch: bool = False,
    ) -> dict[str, object]:
        """封存前预览：哪些开放 key 会被当前可见资料改变，以及为什么。

        对比“上次物化落库的值/证据”与“按当前账本重算的值/证据”，
        逐项列出受影响的特征 key、所属窗口（左闭右开区间）与原因
        （late_arrival 迟到资料 / retract 撤回 / revision 修订 /
        boundary_shift 窗口边界变化）。已封存 key 不出现在变化里。
        本方法只读，不落库。
        """
        sample = self._sample(sample_id)
        spec = self._get_spec(feature_name, version)
        stored = self._stored_features(sample_id, spec.name, spec.version)
        if not stored:
            raise KeyError(
                f"样本 {sample_id!r} 尚未物化 {spec.name!r} v{spec.version}，无封存前基准"
            )
        stored_values = {r["feature_key"]: loads(r["value"]) for r in stored}
        sealed_keys = sorted(r["feature_key"] for r in stored if r["sealed"])
        stored_evidence: dict[str, dict[str, list[int]]] = {}
        for row in self.conn.execute(
            "SELECT feature_key, seq, role FROM sample_feature_evidence "
            "WHERE sample_id = ? AND feature_name = ? AND version = ?",
            (sample_id, spec.name, spec.version),
        ).fetchall():
            stored_evidence.setdefault(row["feature_key"], {"behavior": [], "state": []})[
                row["role"]
            ].append(row["seq"])

        explained = self._explain(sample, spec, batch)
        values = explained["values"]
        evidence = explained["evidence"]
        as_of = sample["as_of"]
        changes: list[dict[str, object]] = []
        unchanged: list[str] = []
        for key in feature_keys(spec):
            if key in sealed_keys:
                continue
            old_ev = stored_evidence.get(key, {"behavior": [], "state": []})
            new_ev = evidence[key]
            if stored_values.get(key) == values[key] and old_ev == new_ev:
                unchanged.append(key)
                continue
            window = self._window_of(spec, key)
            changes.append({
                "feature_key": key,
                "window": window,
                "window_range": None if window is None else [as_of - window, as_of],
                "stored_value": stored_values.get(key),
                "current_value": values[key],
                "reasons": self._diff_reasons(
                    spec, key, sample["entity_id"], as_of, old_ev, new_ev
                ),
            })
        return {
            "sample_id": sample_id,
            "feature_name": spec.name,
            "version": spec.version,
            "as_of": as_of,
            "changes": changes,
            "unchanged": unchanged,
            "sealed": sealed_keys,
        }

    def seal(
        self,
        sample_id: str,
        feature_name: str,
        version: int | None = None,
        *,
        feature_keys: list[str] | tuple[str, ...] | None = None,
        batch: bool = False,
    ) -> dict[str, object]:
        """把指定范围（默认全部仍开放的 key）按当前可见值封存。

        封存后这些 key 的值与证据历史冻结，重启接续保留封存决定；
        未封存的 key 继续保持开放，随可见资料修订。返回本次封存的值。
        """
        sample = self._sample(sample_id)
        spec = self._get_spec(feature_name, version)
        if sample["entity_type"] != spec.entity:
            raise ValueError(
                f"样本实体类型 {sample['entity_type']!r} 与特征实体 {spec.entity!r} 不一致"
            )
        explained = self._explain(sample, spec, batch)
        values = explained["values"]
        evidence = explained["evidence"]
        if feature_keys is None:
            stored = self._stored_features(sample_id, spec.name, spec.version)
            sealed_keys = {r["feature_key"] for r in stored if r["sealed"]}
            targets = [key for key in values if key not in sealed_keys]
        else:
            unknown = sorted(set(feature_keys) - set(values))
            if unknown:
                raise KeyError(f"特征 {spec.name!r} v{spec.version} 不存在 key: {unknown}")
            targets = list(feature_keys)
        if targets:
            self._persist_values(
                sample_id, spec.name, spec.version, values, evidence, targets, sealed=True
            )
        return {key: values[key] for key in targets}

    def get_evidence(
        self, sample_id: str, feature_name: str, version: int, *, include_records: bool = True
    ) -> dict[str, dict[str, list[object]]]:
        """返回特征值使用的账本 seq，可附带原始记录用于重启后审计。

        封存 key 取封存时落库的证据（历史原样）；开放 key 按当前可见
        账本实时重算，随可见资料修订。
        """
        sample = self._sample(sample_id)
        stored = self._stored_features(sample_id, feature_name, version)
        if not stored:
            raise KeyError(f"样本 {sample_id!r} 尚无 {feature_name!r} v{version} 的证据")
        sealed_keys = [r["feature_key"] for r in stored if r["sealed"]]
        open_keys = [r["feature_key"] for r in stored if not r["sealed"]]
        result: dict[str, dict[str, list[object]]] = {
            key: {"behavior": [], "state": []} for key in sealed_keys
        }
        if sealed_keys:
            rows = self.conn.execute(
                "SELECT feature_key, seq, role FROM sample_feature_evidence "
                "WHERE sample_id = ? AND feature_name = ? AND version = ? "
                "ORDER BY feature_key, role, seq",
                (sample_id, feature_name, version),
            ).fetchall()
            for row in rows:
                if row["feature_key"] in result:
                    result[row["feature_key"]][row["role"]].append(row["seq"])
        if open_keys:
            spec = self._get_spec(feature_name, version)
            live = self.explain_batch(sample["entity_id"], spec, sample["as_of"])["evidence"]
            for key in open_keys:
                result[key] = {
                    "behavior": list(live[key]["behavior"]),
                    "state": list(live[key]["state"]),
                }
        if include_records:
            all_seqs = sorted(
                {seq for roles in result.values() for seqs in roles.values() for seq in seqs}
            )
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
        """读取样本特征值：封存 key 返回历史快照，开放 key 返回当前可见值。

        开放部分只使用 arrival_time <= 样本 as_of 的记录重算，
        未来才到达的信息不会混入。
        """
        rows = self.conn.execute(
            "SELECT feature_key, value, sealed FROM sample_features "
            "WHERE sample_id = ? AND feature_name = ? AND version = ? ORDER BY feature_key",
            (sample_id, feature_name, version),
        ).fetchall()
        if not rows:
            raise KeyError(
                f"样本 {sample_id!r} 尚未物化 {feature_name!r} v{version}"
            )
        merged = {r["feature_key"]: loads(r["value"]) for r in rows if r["sealed"]}
        open_keys = [r["feature_key"] for r in rows if not r["sealed"]]
        if open_keys:
            sample = self._sample(sample_id)
            spec = self._get_spec(feature_name, version)
            current = self.compute_batch(sample["entity_id"], spec, sample["as_of"])
            for key in open_keys:
                merged[key] = current[key]
        return merged

    def close(self) -> None:
        self.conn.close()
