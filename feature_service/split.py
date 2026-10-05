"""按来源泄漏约束划分训练集/验证集（train/validation split）。

在已冻结的研究数据集之上提供“提出方案 → 比较方案 → 明确采用”的流程：

* 研究者指定时间界限、目标组比例、类别标签与必须共同归组的来源关系；
* 共享同一来源事件 (entity, source, event_id) 或同一主体的样本被归入
  同一组，整组只落在一侧，绝不跨侧拆分；
* 同一来源事件被同一样本的多个特征合法引用时按身份只计一次，既不删除
  也不当作独立证据跨侧分配；
* 时间界限之后才可见的样本一律排除并说明原因，不用未来资料补齐；
* 约束冲突（归组关系指向不可用样本）与历史可见样本不足时，给出无法
  满足的范围，而不是静默放松约束；
* 方案是快照：迟到资料到账后只能形成新方案，旧方案原样保留可比较；
  每个数据集最多一份“已采用”方案，采用新方案时旧方案转为 superseded。
"""

from __future__ import annotations

import time

from .dataset_store import ensure_dataset_tables
from .store import dumps, loads


class SplitError(RuntimeError):
    pass


class LeakageSplitService:
    """在一个已冻结数据集上提出、比较并采用防泄漏切分方案。"""

    def __init__(self, dataset_service):
        self.ds = dataset_service
        self.svc = dataset_service.svc
        self.conn = dataset_service.conn
        ensure_dataset_tables(self.conn)

    @staticmethod
    def _now(value):
        return int(time.time() * 1000) if value is None else value

    # ---- 分组 --------------------------------------------------------

    def _sample_event_identities(self, dataset_id):
        """{sample_id: {(entity, source, event_id)}}：按来源事件身份归集。

        同一事件被同一样本的多个特征/多个键引用只出现一次。
        """
        identities = {}
        for row in self.ds._cells(dataset_id):
            seqs = loads(row["evidence"])
            bucket = identities.setdefault(row["sample_id"], set())
            for seq in seqs.get("behavior", []) + seqs.get("state", []):
                rec = self.svc._log_record(seq)
                bucket.add((rec["entity"], rec["source"], rec["event_id"]))
        return identities

    def _build_groups(self, eligible, identities, co_groups):
        """并查集：同主体 / 共享来源事件 / 显式归组关系 -> 同一组。"""
        parent = {sid: sid for sid in eligible}

        def find(x):
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def union(a, b):
            ra, rb = find(a), find(b)
            if ra != rb:
                parent[max(ra, rb)] = min(ra, rb)

        by_entity = {}
        for sid in eligible:
            by_entity.setdefault(eligible[sid]["entity_id"], []).append(sid)
        for sids in by_entity.values():
            for other in sids[1:]:
                union(sids[0], other)

        # 跨主体共享同一原事件（同 source + event_id，如一次转账双方各自
        # 入账）也归为一组；不同来源合法复用同一 event_id 不算共享。
        by_event = {}
        for sid, idents in identities.items():
            if sid not in eligible:
                continue
            for ident in idents:
                by_event.setdefault((ident[1], ident[2]), []).append(sid)
        for sids in by_event.values():
            for other in sids[1:]:
                union(sids[0], other)

        conflicts = []
        for a, b in co_groups:
            if a not in eligible or b not in eligible:
                conflicts.append({
                    "type": "co_group_conflict",
                    "pair": [a, b],
                    "detail": "归组关系指向不可用样本（未知或超出时间界限），该关系无法生效",
                })
                continue
            union(a, b)

        groups = {}
        for sid in eligible:
            groups.setdefault(find(sid), []).append(sid)
        return [sorted(sids) for sids in groups.values()], conflicts

    # ---- 提出方案 ----------------------------------------------------

    def propose_split(self, dataset_id, *, time_boundary, train_ratio,
                      labels=None, co_groups=(), name=None, created_at=None,
                      note=None):
        """提出一份切分方案（快照，之后不被迟到资料改写）。

        * time_boundary：只纳入 as_of <= 界限的已冻结样本；界限之后的
          样本记为 excluded(beyond_time_boundary)，不用未来资料补齐。
        * train_ratio：训练侧目标占比 (0, 1)。
        * labels：{sample_id: 类别}，用于尽量保留两侧类别覆盖。
        * co_groups：必须同侧的样本对 [[a, b], ...]。
        """
        meta = self.ds.get_dataset(dataset_id)
        if meta["status"] != "frozen":
            raise SplitError("只能对已冻结数据集提出切分方案")
        if not 0 < train_ratio < 1:
            raise ValueError("train_ratio 必须在 (0, 1) 之间")
        labels = dict(labels or {})
        co_groups = [list(pair) for pair in co_groups]

        included = self.ds.included_samples(dataset_id)
        eligible, excluded = {}, []
        for sid in included:
            sample = self.svc._sample(sid)
            if sample["as_of"] <= time_boundary:
                eligible[sid] = sample
            else:
                excluded.append({"sample_id": sid,
                                 "reason": "beyond_time_boundary",
                                 "as_of": sample["as_of"]})

        identities = self._sample_event_identities(dataset_id)
        groups, conflicts = self._build_groups(eligible, identities, co_groups)

        unmet = list(conflicts)
        total = len(eligible)
        if total < 2:
            unmet.append({
                "type": "insufficient_history",
                "detail": "时间界限 %d 之前只有 %d 个可用样本，无法形成两侧切分；不能用未来资料补齐"
                          % (time_boundary, total),
            })

        # 组不可再分：计算可实现的训练侧占比，给出无法满足的范围。
        sizes = sorted(len(g) for g in groups)
        reachable = {0}
        for size in sizes:
            reachable |= {r + size for r in list(reachable)}
        target_count = train_ratio * total
        achievable = sorted(r for r in reachable if r <= total)
        train_target = min(achievable, key=lambda r: (abs(r - target_count), r)) if achievable else 0
        if total and abs(train_target - target_count) > 1e-9:
            unmet.append({
                "type": "unreachable_ratio",
                "target_train_count": target_count,
                "nearest_train_count": train_target,
                "nearest_ratio": train_target / total,
                "detail": "归组约束下样本组不可再分，目标比例无法精确达到，取最近可实现比例",
            })

        # 类别覆盖：只出现在一个组的类别无法两侧覆盖（极小类别）。
        label_groups = {}
        for g in groups:
            for lab in {labels.get(sid) for sid in g}:
                label_groups.setdefault(lab, []).append(g)
        tiny_classes = sorted(str(lab) for lab, gs in label_groups.items()
                              if lab is not None and len(gs) == 1)
        for lab in tiny_classes:
            unmet.append({
                "type": "tiny_class",
                "label": lab,
                "detail": "类别 %s 只出现在一个样本组，无法两侧都覆盖" % lab,
            })

        # 分配：先保证可覆盖类别两侧都有组，再按目标比例贪心归侧。
        side = {}
        ordered = sorted(groups, key=lambda g: (-len(g), g[0]))
        for lab, gs in label_groups.items():
            if lab is None or len(gs) < 2:
                continue
            first, second = sorted(gs, key=lambda g: (-len(g), g[0]))[:2]
            for g, s in ((first, "train"), (second, "validation")):
                key = tuple(g)
                if key not in side:
                    side[key] = s
        train_count = sum(len(g) for g in groups if side.get(tuple(g)) == "train")
        for g in ordered:
            key = tuple(g)
            if key in side:
                continue
            side[key] = "train" if train_count < train_target else "validation"
            if side[key] == "train":
                train_count += len(g)

        assignments = []
        for gid, g in enumerate(ordered):
            for sid in g:
                assignments.append({"sample_id": sid, "side": side[tuple(g)],
                                    "group_id": gid, "reason": None})
        for ex in excluded:
            assignments.append({"sample_id": ex["sample_id"], "side": "excluded",
                                "group_id": None, "reason": ex["reason"]})

        ledger_seq = self.conn.execute(
            "SELECT COALESCE(MAX(seq), 0) AS m FROM event_log").fetchone()["m"]
        cur = self.conn.execute(
            "INSERT INTO research_split_schemes "
            "(dataset_id, name, status, time_boundary, train_ratio, labels, "
            " co_groups, ledger_seq, created_at, note) "
            "VALUES (?,?, 'proposed', ?,?,?,?,?,?,?)",
            (dataset_id, name, time_boundary, train_ratio, dumps(labels),
             dumps(co_groups), ledger_seq, self._now(created_at), note),
        )
        scheme_id = cur.lastrowid
        for a in assignments:
            self.conn.execute(
                "INSERT INTO research_split_assignments "
                "(scheme_id, sample_id, side, group_id, reason) "
                "VALUES (?,?,?,?,?)",
                (scheme_id, a["sample_id"], a["side"], a["group_id"], a["reason"]),
            )
        self.conn.commit()
        return {"scheme_id": scheme_id, "unmet": unmet,
                "summary": self.verify_split(scheme_id)}

    # ---- 查询 / 比较 / 采用 ------------------------------------------

    def _scheme(self, scheme_id):
        row = self.conn.execute(
            "SELECT * FROM research_split_schemes WHERE scheme_id = ?",
            (scheme_id,)).fetchone()
        if row is None:
            raise KeyError("切分方案不存在: %s" % scheme_id)
        return dict(row)

    def _assignments(self, scheme_id):
        return [dict(r) for r in self.conn.execute(
            "SELECT * FROM research_split_assignments WHERE scheme_id = ? "
            "ORDER BY sample_id", (scheme_id,))]

    def list_schemes(self, dataset_id):
        return [dict(r) for r in self.conn.execute(
            "SELECT * FROM research_split_schemes WHERE dataset_id = ? "
            "ORDER BY scheme_id", (dataset_id,))]

    def get_scheme(self, scheme_id):
        scheme = self._scheme(scheme_id)
        scheme["labels"] = loads(scheme["labels"])
        scheme["co_groups"] = loads(scheme["co_groups"])
        scheme["assignments"] = self._assignments(scheme_id)
        return scheme

    def verify_split(self, scheme_id):
        """核对来源隔离与实际覆盖：跨主体共享事件、极小类别、交叠封存。

        任何来源事件身份或主体跨侧出现即报告泄漏；同一事件在同侧被多个
        特征/样本合法引用只计一次，不算泄漏也不删除。
        """
        scheme = self._scheme(scheme_id)
        labels = loads(scheme["labels"]) or {}
        assignments = self._assignments(scheme_id)
        sides = {a["sample_id"]: a["side"] for a in assignments}
        train = sorted(s for s, v in sides.items() if v == "train")
        valid = sorted(s for s, v in sides.items() if v == "validation")

        identities = self._sample_event_identities(scheme["dataset_id"])
        event_sides, entity_sides = {}, {}
        for sid in train + valid:
            side = sides[sid]
            entity = self.svc._sample(sid)["entity_id"]
            entity_sides.setdefault(entity, set()).add(side)
            for ident in identities.get(sid, ()):
                event_sides.setdefault((ident[1], ident[2]), set()).add(side)
        shared_events = sorted(e for e, s in event_sides.items() if len(s) > 1)
        shared_entities = sorted(e for e, s in entity_sides.items() if len(s) > 1)

        coverage = {}
        for sid in train + valid:
            lab = labels.get(sid)
            if lab is None:
                continue
            bucket = coverage.setdefault(lab, {"train": 0, "validation": 0})
            bucket[sides[sid]] += 1
        tiny = sorted(str(l) for l, c in coverage.items()
                      if c["train"] == 0 or c["validation"] == 0)

        total = len(train) + len(valid)
        current_seq = self.conn.execute(
            "SELECT COALESCE(MAX(seq), 0) AS m FROM event_log").fetchone()["m"]
        stale = current_seq > scheme["ledger_seq"]
        return {
            "scheme_id": scheme_id,
            "dataset_id": scheme["dataset_id"],
            "status": scheme["status"],
            "train": train, "validation": valid,
            "excluded": [a for a in assignments if a["side"] == "excluded"],
            "train_ratio": (len(train) / total) if total else None,
            "target_ratio": scheme["train_ratio"],
            "class_coverage": coverage,
            "tiny_classes": tiny,
            "shared_events_across_sides": [list(e) for e in shared_events],
            "shared_entities_across_sides": shared_entities,
            "leak_free": not shared_events and not shared_entities,
            "stale": stale,
            "stale_detail": ("方案提出后有迟到资料到账；旧方案不变，需要时以其为基础提出新方案"
                             if stale else None),
        }

    def compare_schemes(self, dataset_id):
        """并列比较同一数据集的全部方案，便于明确选择采用哪一份。"""
        out = []
        for scheme in self.list_schemes(dataset_id):
            summary = self.verify_split(scheme["scheme_id"])
            out.append({
                "scheme_id": scheme["scheme_id"],
                "name": scheme["name"],
                "status": scheme["status"],
                "time_boundary": scheme["time_boundary"],
                "target_ratio": scheme["train_ratio"],
                "actual_ratio": summary["train_ratio"],
                "train_count": len(summary["train"]),
                "validation_count": len(summary["validation"]),
                "excluded_count": len(summary["excluded"]),
                "leak_free": summary["leak_free"],
                "tiny_classes": summary["tiny_classes"],
                "stale": summary["stale"],
            })
        return out

    def adopt_scheme(self, scheme_id):
        """明确采用一份方案；同数据集其它方案转为 superseded。"""
        scheme = self._scheme(scheme_id)
        summary = self.verify_split(scheme_id)
        if not summary["leak_free"]:
            raise SplitError("方案存在跨侧来源泄漏，不能采用")
        self.conn.execute(
            "UPDATE research_split_schemes SET status = 'superseded' "
            "WHERE dataset_id = ? AND status = 'adopted'",
            (scheme["dataset_id"],),
        )
        self.conn.execute(
            "UPDATE research_split_schemes SET status = 'adopted' "
            "WHERE scheme_id = ?", (scheme_id,),
        )
        self.conn.commit()
        return self.get_scheme(scheme_id)
