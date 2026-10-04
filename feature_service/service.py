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

    def explain_incremental(
        self, entity: str, spec: FeatureSpec, as_of: int, *, include_blocked: bool = False
    ) -> dict[str, object]:
        self.projection.catch_up()
        return explain_from_rows(
            spec, as_of, self.projection.read_rows(entity),
            include_blocked=include_blocked,
        )

    def explain_batch(
        self, entity: str, spec: FeatureSpec, as_of: int, *, include_blocked: bool = False
    ) -> dict[str, object]:
        return explain_from_rows(
            spec, as_of, read_log_rows(self.conn, entity),
            include_blocked=include_blocked,
        )

    def rebuild_projection(self) -> None:
        """丢弃增量缓存并整批重放（运维/校验用）。"""
        self.projection.rebuild()

    # ---- 物化：开放结果随可见修订更新，封存结果保持原样 -----------------

    def _explain(
        self, sample, spec: FeatureSpec, batch: bool, *, include_blocked: bool = False
    ) -> dict[str, object]:
        if batch:
            return self.explain_batch(
                sample["entity_id"], spec, sample["as_of"],
                include_blocked=include_blocked,
            )
        return self.explain_incremental(
            sample["entity_id"], spec, sample["as_of"],
            include_blocked=include_blocked,
        )

    def _stored_rows(
        self, sample_id: str, feature_name: str, version: int
    ) -> dict[str, dict[str, object]]:
        rows = self.conn.execute(
            "SELECT feature_key, value, status, sealed_at, seal_id FROM sample_features "
            "WHERE sample_id = ? AND feature_name = ? AND version = ?",
            (sample_id, feature_name, version),
        ).fetchall()
        return {
            r["feature_key"]: {
                "value": loads(r["value"]),
                "status": r["status"],
                "sealed_at": r["sealed_at"],
                "seal_id": r["seal_id"],
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
        seal_id: int | None = None,
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
            "status, sealed_at, seal_id) "
            "VALUES (?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(sample_id, feature_name, version, feature_key) "
            "DO UPDATE SET value = excluded.value, computed_at = excluded.computed_at, "
            "status = excluded.status, sealed_at = excluded.sealed_at, "
            "seal_id = excluded.seal_id",
            [
                (sample_id, spec.name, version, key, dumps(value), now,
                 status, sealed_at if seal else None, seal_id if seal else None)
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
        note: str | None = None,
    ) -> dict[str, object]:
        """确认封存：一次调用是一批确认，可以只确认部分范围。

        * feature_keys=None 封存全部仍开放的范围；
        * 封存值取封存时点当时可见（arrival_time <= as_of）的计算结果，
          与 preview_seal 展示的 current 一致；
        * 后续批次允许与先前范围交叠：已封存的 key 保持首次确认的值与
          归属批次，在本批中记为 already_sealed，不重复计入也不改写；
        * 每批决定（含交叠明细）与历史值持久化，重启接续后仍然有效；
        * 返回 {seal_id, confirmed, already_sealed}。
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
            requested = sorted(open_keys)
        else:
            requested = []
            for key in feature_keys:
                if key not in open_keys and key not in sealed_keys:
                    raise KeyError(f"{key!r} 不是已物化的 feature_key")
                requested.append(key)
        confirmed = sorted(k for k in requested if k in open_keys)
        already = sorted(k for k in requested if k in sealed_keys)
        if not requested:
            return {"seal_id": None, "confirmed": [], "already_sealed": []}
        if sealed_at is None:
            sealed_at = int(time.time() * 1000)
        cur = self.conn.execute(
            "INSERT INTO seal_batches (sample_id, feature_name, version, sealed_at, note) "
            "VALUES (?,?,?,?,?)",
            (sample_id, feature_name, version, sealed_at, note),
        )
        seal_id = cur.lastrowid
        self.conn.executemany(
            "INSERT INTO seal_batch_keys (seal_id, feature_key, outcome) VALUES (?,?,?)",
            [(seal_id, key, "confirmed") for key in confirmed]
            + [(seal_id, key, "already_sealed") for key in already],
        )
        self.conn.commit()
        if confirmed:
            explained = self._explain(sample, spec, batch)
            self._store_results(
                sample_id, spec, explained,
                only_keys=set(confirmed), seal=True, sealed_at=sealed_at,
                seal_id=seal_id,
            )
        return {"seal_id": seal_id, "confirmed": confirmed, "already_sealed": already}

    def seal_batches(
        self, sample_id: str, feature_name: str, version: int
    ) -> list[dict[str, object]]:
        """列出某样本某特征版本的全部封存批次及每批确认/交叠明细。"""
        batches = []
        for row in self.conn.execute(
            "SELECT seal_id, sealed_at, note FROM seal_batches "
            "WHERE sample_id = ? AND feature_name = ? AND version = ? "
            "ORDER BY seal_id",
            (sample_id, feature_name, version),
        ).fetchall():
            keys = self.conn.execute(
                "SELECT feature_key, outcome FROM seal_batch_keys "
                "WHERE seal_id = ? ORDER BY feature_key",
                (row["seal_id"],),
            ).fetchall()
            batches.append({
                "seal_id": row["seal_id"],
                "sealed_at": row["sealed_at"],
                "note": row["note"],
                "confirmed": [k["feature_key"] for k in keys if k["outcome"] == "confirmed"],
                "already_sealed": [
                    k["feature_key"] for k in keys if k["outcome"] == "already_sealed"
                ],
            })
        return batches

    def query_features(
        self, sample_id: str, feature_name: str, version: int
    ) -> dict[str, dict[str, object]]:
        """逐 feature_key 说明当前结果：值、归属哪次封存确认、仍可更新部分。

        每个 key 返回 value / status / seal_id（归属的封存批次，开放为
        None）/ sealed_at / updatable（是否仍随可见资料更新）/ evidence
        （该值使用的账本 seq，按角色分组）。
        """
        stored = self._stored_rows(sample_id, feature_name, version)
        if not stored:
            raise KeyError(
                f"样本 {sample_id!r} 尚未物化 {feature_name!r} v{version}"
            )
        evidence: dict[str, dict[str, list[int]]] = {
            key: {"behavior": [], "state": []} for key in stored
        }
        for row in self.conn.execute(
            "SELECT feature_key, seq, role FROM sample_feature_evidence "
            "WHERE sample_id = ? AND feature_name = ? AND version = ? "
            "ORDER BY feature_key, role, seq",
            (sample_id, feature_name, version),
        ).fetchall():
            evidence[row["feature_key"]][row["role"]].append(row["seq"])
        return {
            key: {
                "value": row["value"],
                "status": row["status"],
                "seal_id": row["seal_id"],
                "sealed_at": row["sealed_at"],
                "updatable": row["status"] == OPEN,
                "evidence": evidence[key],
            }
            for key, row in sorted(stored.items())
        }

    # ---- 修订流程：先提出对照，再决定是否发布独立修订 -------------------

    def _evidence_records(self, seqs: list[int]) -> dict[int, dict[str, object]]:
        if not seqs:
            return {}
        return {
            r["seq"]: {**dict(r), "payload": loads(r["payload"])}
            for r in self.conn.execute(
                "SELECT seq, event_id, entity, source, op, event_time, arrival_time, payload "
                "FROM event_log WHERE seq IN (%s) ORDER BY seq"
                % ",".join("?" for _ in seqs),
                list(seqs),
            ).fetchall()
        }

    def _stored_evidence(
        self, sample_id: str, feature_name: str, version: int
    ) -> dict[str, dict[str, list[int]]]:
        evidence: dict[str, dict[str, list[int]]] = {}
        for row in self.conn.execute(
            "SELECT feature_key, seq, role FROM sample_feature_evidence "
            "WHERE sample_id = ? AND feature_name = ? AND version = ? "
            "ORDER BY feature_key, role, seq",
            (sample_id, feature_name, version),
        ).fetchall():
            evidence.setdefault(row["feature_key"], {"behavior": [], "state": []})[
                row["role"]
            ].append(row["seq"])
        return evidence

    def _downstream_of(
        self,
        changed_seqs: set[int],
        *,
        exclude: set[tuple[str, str, int, str]],
    ) -> list[dict[str, object]]:
        """找出证据用到这些账本记录的其它已保存结果（受影响下游）。

        同一条来源变动跨越多个范围时，每个下游 key 只计一次；修订对象
        自身（exclude）不计入。
        """
        if not changed_seqs:
            return []
        rows = self.conn.execute(
            "SELECT DISTINCT sample_id, feature_name, version, feature_key "
            "FROM sample_feature_evidence WHERE seq IN (%s) "
            "ORDER BY sample_id, feature_name, version, feature_key"
            % ",".join("?" for _ in changed_seqs),
            sorted(changed_seqs),
        ).fetchall()
        return [
            {
                "sample_id": r["sample_id"],
                "feature_name": r["feature_name"],
                "version": r["version"],
                "feature_key": r["feature_key"],
            }
            for r in rows
            if (r["sample_id"], r["feature_name"], r["version"], r["feature_key"])
            not in exclude
        ]

    def propose_revision(
        self,
        sample_id: str,
        feature_name: str,
        version: int | None = None,
        *,
        note: str | None = None,
        batch: bool = False,
        created_at: int | None = None,
    ) -> dict[str, object]:
        """对已封存结果提出修订：列出新旧值、采用资料与受影响下游。

        只生成一份 proposed 状态的修订单，不改写任何已保存值；未采纳前
        当前已确认视角与依赖这些特征的研究完全不受影响。
        """
        sample = self._sample(sample_id)
        spec = self._get_spec(feature_name, version)
        version = spec.version
        stored = self._stored_rows(sample_id, feature_name, version)
        sealed_keys = sorted(k for k, r in stored.items() if r["status"] == SEALED)
        if not sealed_keys:
            raise ValueError(
                f"样本 {sample_id!r} 的 {feature_name!r} v{version} 没有已封存结果可修订"
            )
        explained = self._explain(sample, spec, batch)
        current_values: dict[str, object] = explained["values"]
        current_evidence: dict[str, dict[str, list[int]]] = explained["evidence"]
        old_evidence = self._stored_evidence(sample_id, feature_name, version)

        entries = []
        for key in sealed_keys:
            old_value = stored[key]["value"]
            new_value = current_values.get(key)
            if old_value == new_value and old_evidence.get(key) == current_evidence.get(key):
                continue
            new_seqs = current_evidence.get(key, {"behavior": [], "state": []})
            old_seqs = old_evidence.get(key, {"behavior": [], "state": []})
            changed_seqs = {
                seq
                for role in ("behavior", "state")
                for seq in set(new_seqs[role]) ^ set(old_seqs[role])
            }
            downstream = self._downstream_of(
                changed_seqs,
                exclude={(sample_id, feature_name, version, key)},
            )
            entries.append({
                "feature_key": key,
                "old_value": old_value,
                "new_value": new_value,
                "evidence": new_seqs,
                "downstream": downstream,
            })
        if created_at is None:
            created_at = int(time.time() * 1000)
        cur = self.conn.execute(
            "INSERT INTO revisions "
            "(sample_id, feature_name, version, status, note, created_at) "
            "VALUES (?,?,?,?,?,?)",
            (sample_id, feature_name, version, "proposed", note, created_at),
        )
        revision_id = cur.lastrowid
        self.conn.executemany(
            "INSERT INTO revision_entries "
            "(revision_id, feature_key, old_value, new_value, evidence, downstream) "
            "VALUES (?,?,?,?,?,?)",
            [
                (revision_id, e["feature_key"], dumps(e["old_value"]),
                 dumps(e["new_value"]), dumps(e["evidence"]), dumps(e["downstream"]))
                for e in entries
            ],
        )
        self.conn.commit()
        return self.get_revision(revision_id)

    def _revision_row(self, revision_id: int):
        row = self.conn.execute(
            "SELECT revision_id, sample_id, feature_name, version, status, note, "
            "created_at, published_at FROM revisions WHERE revision_id = ?",
            (revision_id,),
        ).fetchone()
        if row is None:
            raise KeyError(f"修订不存在: {revision_id}")
        return row

    def get_revision(self, revision_id: int) -> dict[str, object]:
        """读取修订单：新旧值对照、新值采用的原始资料、受影响下游。"""
        row = self._revision_row(revision_id)
        entries = []
        for e in self.conn.execute(
            "SELECT feature_key, old_value, new_value, evidence, downstream "
            "FROM revision_entries WHERE revision_id = ? ORDER BY feature_key",
            (revision_id,),
        ).fetchall():
            evidence = loads(e["evidence"])
            seqs = sorted({s for role_seqs in evidence.values() for s in role_seqs})
            entries.append({
                "feature_key": e["feature_key"],
                "old_value": loads(e["old_value"]),
                "new_value": loads(e["new_value"]),
                "evidence": evidence,
                "materials": list(self._evidence_records(seqs).values()),
                "downstream": loads(e["downstream"]),
            })
        return {
            "revision_id": row["revision_id"],
            "sample_id": row["sample_id"],
            "feature_name": row["feature_name"],
            "version": row["version"],
            "status": row["status"],
            "note": row["note"],
            "created_at": row["created_at"],
            "published_at": row["published_at"],
            "entries": entries,
        }

    def list_revisions(
        self,
        sample_id: str | None = None,
        feature_name: str | None = None,
        version: int | None = None,
        *,
        status: str | None = None,
    ) -> list[dict[str, object]]:
        """按样本/特征/状态过滤修订单（不含条目明细）。"""
        sql = (
            "SELECT revision_id, sample_id, feature_name, version, status, note, "
            "created_at, published_at FROM revisions WHERE 1=1"
        )
        params: list[object] = []
        if sample_id is not None:
            sql += " AND sample_id = ?"
            params.append(sample_id)
        if feature_name is not None:
            sql += " AND feature_name = ?"
            params.append(feature_name)
        if version is not None:
            sql += " AND version = ?"
            params.append(version)
        if status is not None:
            sql += " AND status = ?"
            params.append(status)
        sql += " ORDER BY revision_id"
        return [dict(r) for r in self.conn.execute(sql, params).fetchall()]

    def publish_revision(
        self, revision_id: int, *, published_at: int | None = None
    ) -> dict[str, object]:
        """发布修订：成为独立可查的修订视角，已封存的原始值保持原样。"""
        row = self._revision_row(revision_id)
        if row["status"] != "proposed":
            raise ValueError(f"修订 {revision_id} 当前为 {row['status']}，不能发布")
        if published_at is None:
            published_at = int(time.time() * 1000)
        self.conn.execute(
            "UPDATE revisions SET status = 'published', published_at = ? "
            "WHERE revision_id = ?",
            (published_at, revision_id),
        )
        self.conn.commit()
        return self.get_revision(revision_id)

    def reject_revision(self, revision_id: int) -> dict[str, object]:
        """不采纳修订：标记为 rejected，不改变任何当前视角。"""
        row = self._revision_row(revision_id)
        if row["status"] != "proposed":
            raise ValueError(f"修订 {revision_id} 当前为 {row['status']}，不能驳回")
        self.conn.execute(
            "UPDATE revisions SET status = 'rejected' WHERE revision_id = ?",
            (revision_id,),
        )
        self.conn.commit()
        return self.get_revision(revision_id)

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

    # ---- 固定研究数据集：跨样本/跨特征的不可变快照 ----------------------

    def _dataset_targets(self, selection: list[dict[str, object]] | None):
        if selection is None:
            samples = self.conn.execute(
                "SELECT sample_id FROM samples ORDER BY sample_id"
            ).fetchall()
            targets = [
                {"sample_id": r["sample_id"], "feature_name": name,
                 "feature_version": version}
                for r in samples
                for name, version in self.list_features()
            ]
        else:
            targets = [dict(t) for t in selection]
        grouped: dict[tuple[str, str, int], dict[str, object]] = {}
        for target in targets:
            sample_id = str(target["sample_id"])
            sample = self._sample(sample_id)
            feature_name = target.get("feature_name")
            if feature_name is None:
                names = [
                    (name, version) for name, version in self.list_features()
                ]
            else:
                version = target.get("feature_version", target.get("version"))
                names = [(str(feature_name), self._get_spec(str(feature_name), version).version)]
            for feature_name, version in names:
                spec = self._get_spec(feature_name, version)
                if sample["entity_type"] != spec.entity and feature_name is not None:
                    raise ValueError(
                        f"样本 {sample_id!r} 实体类型 {sample['entity_type']!r} "
                        f"与特征 {feature_name!r} 的 {spec.entity!r} 不匹配"
                    )
                if sample["entity_type"] != spec.entity:
                    continue
                key = (sample_id, feature_name, spec.version)
                wanted = target.get("feature_keys")
                entry = grouped.setdefault(key, {
                    "sample_id": sample_id,
                    "feature_name": feature_name,
                    "feature_version": spec.version,
                    "keys": set(feature_keys(spec)),
                    "revision_id": target.get("revision_id"),
                    "adopt_current": bool(target.get("adopt_current", False)),
                })
                if wanted is not None:
                    entry["keys"] &= {str(k) for k in wanted}
                if entry.get("revision_id") is None:
                    entry["revision_id"] = target.get("revision_id")
                entry["adopt_current"] = entry["adopt_current"] or bool(
                    target.get("adopt_current", False)
                )
        return [grouped[k] for k in sorted(grouped)]

    def _source_events_for_seqs(self, seqs: set[int] | list[int]) -> list[dict[str, object]]:
        records = self._evidence_records(sorted(set(seqs)))
        events = {
            (r["entity"], r["source"], r["event_id"]): {
                "entity": r["entity"], "source": r["source"],
                "event_id": r["event_id"],
            }
            for r in records.values()
        }
        return [events[k] for k in sorted(events)]

    def _dataset_items(
        self,
        selection: list[dict[str, object]] | None,
        *,
        batch: bool,
        adopt_published: bool = False,
    ) -> list[dict[str, object]]:
        items: list[dict[str, object]] = []
        for target in self._dataset_targets(selection):
            sample_id = target["sample_id"]
            feature_name = target["feature_name"]
            version = int(target["feature_version"])
            sample = self._sample(sample_id)
            spec = self._get_spec(feature_name, version)
            explained = self._explain(sample, spec, batch, include_blocked=True)
            stored = self._stored_rows(sample_id, feature_name, version)
            stored_evidence = self._stored_evidence(sample_id, feature_name, version)
            revision_entries: dict[str, dict[str, object]] = {}
            revision_id = target.get("revision_id")
            if revision_id is None and adopt_published:
                revision_row = self.conn.execute(
                    "SELECT revision_id FROM revisions "
                    "WHERE sample_id = ? AND feature_name = ? AND version = ? "
                    "AND status = 'published' ORDER BY revision_id DESC LIMIT 1",
                    (sample_id, feature_name, version),
                ).fetchone()
                if revision_row is not None:
                    revision_id = revision_row["revision_id"]
            if revision_id is not None:
                revision = self.get_revision(int(revision_id))
                if revision["sample_id"] != sample_id or revision["feature_name"] != feature_name \
                        or revision["version"] != version or revision["status"] != "published":
                    raise ValueError(f"修订 {revision_id} 不能用于该特征视角")
                revision_entries = {e["feature_key"]: e for e in revision["entries"]}
            for key in sorted(target["keys"]):
                blocked = explained.get("blocked_evidence", {}).get(key, [])
                if revision_id is not None:
                    if key not in revision_entries:
                        continue
                    entry = revision_entries[key]
                    value = entry["new_value"]
                    evidence = entry["evidence"]
                    perspective = "published_revision"
                    adopted_revision_id = int(revision_id)
                    seal_id = None
                    confirmation_state = "adopted_revision"
                else:
                    current_row = stored.get(key)
                    if current_row is not None and current_row["status"] == SEALED \
                            and not target["adopt_current"]:
                        value = current_row["value"]
                        evidence = stored_evidence.get(key, {"behavior": [], "state": []})
                        perspective = "stored"
                        adopted_revision_id = None
                        seal_id = current_row["seal_id"]
                        confirmation_state = "sealed"
                    else:
                        value = explained["values"].get(key)
                        evidence = explained["evidence"].get(
                            key, {"behavior": [], "state": []}
                        )
                        perspective = "stored"
                        adopted_revision_id = None
                        seal_id = None if current_row is None else current_row["seal_id"]
                        confirmation_state = "unconfirmed"
                all_seqs = {s for seqs in evidence.values() for s in seqs}
                has_evidence = bool(all_seqs)
                if blocked and confirmation_state == "unconfirmed":
                    evidence_state = "time_restricted"
                elif has_evidence or confirmation_state in ("sealed", "adopted_revision"):
                    evidence_state = "available"
                else:
                    evidence_state = "no_evidence"
                items.append({
                    "sample_id": sample_id,
                    "feature_name": feature_name,
                    "feature_version": version,
                    "feature_key": key,
                    "value": value,
                    "as_of": sample["as_of"],
                    "perspective": perspective,
                    "adopted_revision_id": adopted_revision_id,
                    "seal_id": seal_id,
                    "confirmation_state": confirmation_state,
                    "evidence_state": evidence_state,
                    "eligible": confirmation_state != "unconfirmed",
                    "reasons": [] if confirmation_state != "unconfirmed" else (
                        ["time_restricted"] if evidence_state == "time_restricted" else
                        ["no_evidence"] if evidence_state == "no_evidence" else
                        ["unconfirmed"]
                    ),
                    "evidence": evidence,
                    "source_events": self._source_events_for_seqs(all_seqs),
                    "blocked_records": [dict(r) for r in self._evidence_records(blocked).values()],
                })
        return items

    def preview_research_dataset(
        self,
        selection: list[dict[str, object]] | None = None,
        *,
        batch: bool = False,
        adopt_revisions: bool = False,
    ) -> dict[str, object]:
        """预览固定研究数据集：说明每个值来源、确认状态和不可纳入原因。"""
        items = self._dataset_items(
            selection, batch=batch, adopt_published=adopt_revisions
        )
        sample_ids = sorted({item["sample_id"] for item in items})
        samples = []
        for sample_id in sample_ids:
            own = [item for item in items if item["sample_id"] == sample_id]
            reasons = sorted({reason for item in own for reason in item["reasons"]})
            samples.append({
                "sample_id": sample_id,
                "complete": all(item["eligible"] for item in own) and bool(own),
                "included_items": sum(1 for item in own if item["eligible"]),
                "pending_items": sum(1 for item in own if not item["eligible"]),
                "reasons": reasons,
            })
        complete_ids = {s["sample_id"] for s in samples if s["complete"]}
        return {
            "items": items,
            "samples": samples,
            "complete_samples": [s["sample_id"] for s in samples if s["complete"]],
            "incomplete_samples": [s["sample_id"] for s in samples if not s["complete"]],
            "sample_coverage": len(complete_ids),
            "source_event_count": len({
                (e["entity"], e["source"], e["event_id"])
                for item in items if item["eligible"] and item["sample_id"] in complete_ids
                for e in item["source_events"]
            }),
        }

    def freeze_research_dataset(
        self,
        dataset_id: str,
        selection: list[dict[str, object]] | None = None,
        *,
        include_unconfirmed: bool = False,
        batch: bool = False,
        adopt_revisions: bool = False,
        frozen_at: int | None = None,
        note: str | None = None,
    ) -> dict[str, object]:
        """冻结本次采用的样本与特征视角；未确认范围默认不纳入。"""
        preview = self.preview_research_dataset(
            selection, batch=batch, adopt_revisions=adopt_revisions
        )
        items = [
            item for item in preview["items"]
            if item["eligible"] or include_unconfirmed
        ]
        if preview["incomplete_samples"] and not include_unconfirmed:
            raise ValueError(
                "存在不完整样本: " + ", ".join(preview["incomplete_samples"])
            )
        if not items:
            raise ValueError("没有可冻结的数据集项目")
        version_row = self.conn.execute(
            "SELECT COALESCE(MAX(version), 0) + 1 AS v FROM research_datasets WHERE dataset_id = ?",
            (dataset_id,),
        ).fetchone()
        version = int(version_row["v"])
        frozen_at = int(time.time() * 1000) if frozen_at is None else frozen_at
        last_seq = self.conn.execute("SELECT COALESCE(MAX(seq), -1) AS s FROM event_log").fetchone()["s"]
        self.conn.execute(
            "INSERT INTO research_datasets "
            "(dataset_id, version, status, created_at, frozen_at, last_seq, selection, "
            "pending_differences, note) VALUES (?,?, 'frozen', ?, ?, ?, ?, ?, ?)",
            (dataset_id, version, frozen_at, frozen_at, last_seq, dumps(selection),
             dumps([]), note),
        )
        self.conn.executemany(
            "INSERT INTO research_dataset_items "
            "(dataset_id, dataset_version, sample_id, feature_name, feature_version, "
            "feature_key, value, as_of, perspective, adopted_revision_id, seal_id, "
            "confirmation_state, evidence_state, evidence, source_events, blocked_records) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                (dataset_id, version, item["sample_id"], item["feature_name"],
                 item["feature_version"], item["feature_key"], dumps(item["value"]),
                 item["as_of"], item["perspective"], item["adopted_revision_id"],
                 item["seal_id"], item["confirmation_state"], item["evidence_state"],
                 dumps(item["evidence"]), dumps(item["source_events"]),
                 dumps(item["blocked_records"]))
                for item in items
            ],
        )
        self.conn.commit()
        return self.get_research_dataset(dataset_id, version)

    def get_research_dataset(self, dataset_id: str, version: int | None = None) -> dict[str, object]:
        """逐项读取不可变研究数据集及其样本/来源事件覆盖统计。"""
        if version is None:
            row = self.conn.execute(
                "SELECT MAX(version) AS v FROM research_datasets WHERE dataset_id = ?",
                (dataset_id,),
            ).fetchone()
            if row["v"] is None:
                raise KeyError(f"研究数据集不存在: {dataset_id!r}")
            version = int(row["v"])
        meta = self.conn.execute(
            "SELECT dataset_id, version, status, created_at, frozen_at, last_seq, selection, "
            "pending_differences, note FROM research_datasets "
            "WHERE dataset_id = ? AND version = ?",
            (dataset_id, version),
        ).fetchone()
        if meta is None:
            raise KeyError(f"研究数据集不存在: {dataset_id!r} v{version}")
        items = []
        for r in self.conn.execute(
            "SELECT sample_id, feature_name, feature_version, feature_key, value, as_of, "
            "perspective, adopted_revision_id, seal_id, confirmation_state, evidence_state, "
            "evidence, source_events, blocked_records FROM research_dataset_items "
            "WHERE dataset_id = ? AND dataset_version = ? "
            "ORDER BY sample_id, feature_name, feature_version, feature_key",
            (dataset_id, version),
        ).fetchall():
            items.append({
                "sample_id": r["sample_id"],
                "feature_name": r["feature_name"],
                "feature_version": r["feature_version"],
                "feature_key": r["feature_key"],
                "value": loads(r["value"]),
                "as_of": r["as_of"],
                "perspective": r["perspective"],
                "adopted_revision_id": r["adopted_revision_id"],
                "seal_id": r["seal_id"],
                "confirmation_state": r["confirmation_state"],
                "evidence_state": r["evidence_state"],
                "evidence": loads(r["evidence"]),
                "source_events": loads(r["source_events"]),
                "blocked_records": loads(r["blocked_records"]),
            })
        source_events = {
            (e["entity"], e["source"], e["event_id"])
            for item in items for e in item["source_events"]
        }
        return {
            **{k: meta[k] for k in meta.keys()},
            "selection": loads(meta["selection"]),
            "pending_differences": loads(meta["pending_differences"]),
            "items": items,
            "sample_coverage": len({item["sample_id"] for item in items}),
            "source_event_count": len(source_events),
        }

    def update_research_dataset(
        self,
        dataset_id: str,
        selection: list[dict[str, object]] | None = None,
        *,
        adopt_revisions: bool = False,
        adopt_visible: bool = False,
        include_unconfirmed: bool = False,
        batch: bool = False,
        frozen_at: int | None = None,
        note: str | None = None,
    ) -> dict[str, object]:
        """发布同一研究数据集的新版本，并保留未采用差异。"""
        old = self.get_research_dataset(dataset_id)
        old_version = int(old["version"])
        if selection is None:
            selection = old["selection"]
            if selection is None:
                selection = None
        current_preview = self.preview_research_dataset(
            selection, batch=batch, adopt_revisions=adopt_revisions
        )
        revision_preview = self.preview_research_dataset(
            selection, batch=batch, adopt_revisions=True
        )
        old_by_key = {
            (i["sample_id"], i["feature_name"], i["feature_version"], i["feature_key"]): i
            for i in old["items"]
        }
        new_by_key = {
            (i["sample_id"], i["feature_name"], i["feature_version"], i["feature_key"]): i
            for i in current_preview["items"]
        }
        revised_by_key = {
            (i["sample_id"], i["feature_name"], i["feature_version"], i["feature_key"]): i
            for i in revision_preview["items"]
        }
        effective: list[dict[str, object]] = []
        differences: list[dict[str, object]] = []
        for key in sorted(set(old_by_key) | set(new_by_key)):
            old_item = old_by_key.get(key)
            current_item = new_by_key.get(key)
            if old_item is None or current_item is None:
                category = "sample_scope"
            else:
                revised_item = revised_by_key.get(key)
                if revised_item is not None and revised_item.get("adopted_revision_id") is not None:
                    category = "feature_revision"
                elif old_item["perspective"] != current_item["perspective"]:
                    category = "feature_revision"
                elif current_item["confirmation_state"] == "unconfirmed":
                    category = "visible_material_selection"
                else:
                    category = "visible_material_selection"
            if old_item is None:
                chosen = current_item
            elif current_item is None:
                chosen = None
            else:
                revised_item = revised_by_key.get(key)
                comparison_item = revised_item if category == "feature_revision" else current_item
                use_current = (
                    adopt_visible and current_item["confirmation_state"] == "unconfirmed"
                ) or (
                    adopt_revisions and comparison_item["perspective"] == "published_revision"
                )
                chosen = dict(comparison_item if use_current else old_item)
                if not use_current:
                    if comparison_item["confirmation_state"] == "unconfirmed" or (
                        comparison_item["perspective"] == "published_revision"
                    ):
                        chosen["confirmation_state"] = old_item["confirmation_state"]
                        chosen["evidence_state"] = old_item["evidence_state"]
                        chosen["perspective"] = old_item["perspective"]
                        chosen["adopted_revision_id"] = old_item["adopted_revision_id"]
                        chosen["seal_id"] = old_item["seal_id"]
                if old_item["value"] != comparison_item["value"] or \
                        old_item["evidence"] != comparison_item["evidence"] or \
                        old_item["confirmation_state"] != comparison_item["confirmation_state"]:
                    adopted = use_current
                    differences.append({
                        "sample_id": key[0],
                        "feature_name": key[1],
                        "feature_version": key[2],
                        "feature_key": key[3],
                        "category": category,
                        "old_value": old_item["value"],
                        "new_value": comparison_item["value"],
                        "adopted": adopted,
                    })
            if chosen is not None and (chosen["confirmation_state"] != "unconfirmed" or include_unconfirmed):
                chosen = dict(chosen)
                chosen["eligible"] = chosen["confirmation_state"] != "unconfirmed"
                effective.append(chosen)

        frozen_at = int(time.time() * 1000) if frozen_at is None else frozen_at
        last_seq = self.conn.execute("SELECT COALESCE(MAX(seq), -1) AS s FROM event_log").fetchone()["s"]
        new_version = old_version + 1
        self.conn.execute(
            "INSERT INTO research_datasets "
            "(dataset_id, version, status, created_at, frozen_at, last_seq, selection, "
            "pending_differences, note) VALUES (?,?, 'frozen', ?, ?, ?, ?, ?, ?)",
            (dataset_id, new_version, frozen_at, frozen_at, last_seq, dumps(selection),
             dumps([d for d in differences if not d["adopted"]]), note),
        )
        self.conn.executemany(
            "INSERT INTO research_dataset_items "
            "(dataset_id, dataset_version, sample_id, feature_name, feature_version, "
            "feature_key, value, as_of, perspective, adopted_revision_id, seal_id, "
            "confirmation_state, evidence_state, evidence, source_events, blocked_records) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                (dataset_id, new_version, item["sample_id"], item["feature_name"],
                 item["feature_version"], item["feature_key"], dumps(item["value"]),
                 item["as_of"], item["perspective"], item["adopted_revision_id"],
                 item["seal_id"], item["confirmation_state"], item["evidence_state"],
                 dumps(item["evidence"]), dumps(item["source_events"]),
                 dumps(item["blocked_records"]))
                for item in effective
            ],
        )
        self.conn.commit()
        result = self.get_research_dataset(dataset_id, new_version)
        result["changes"] = differences
        return result

    def close(self) -> None:
        self.conn.close()
