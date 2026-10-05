"""按来源泄漏约束划分训练集/验证集的研究流程。

在已冻结的研究数据集之上，研究者指定：

* 时间界限 time_bound：只使用 as_of 不晚于界限的样本（历史可见样本），
  超出界限的样本被排除并说明，不能用未来资料补齐；
* 组比例 target_ratio：训练侧样本数占比目标；
* 共同归组来源关系 relations：研究者显式声明必须同侧的样本组合，
  加上系统自动识别的“共享原事件 / 同一主体”关系。

系统将共享原事件（来源事件身份 (entity, source, event_id) 相同）或同一
主体（entity_id 相同）的样本放在同一侧，在尽量接近目标比例的同时保留
类别覆盖（每个类别尽量两侧都有）。多个特征对同一事件的合法引用按身份
只计一次，不能被删除，也不能被当成独立证据跨侧分配。

约束冲突或历史可见样本不足时，在方案的 unsatisfiable 中给出无法满足的
范围；方案一旦生成即不可变——新增迟到资料只能形成新方案，可比较不同
方案并显式采用其中一份。
"""

from __future__ import annotations

import time

from .dataset import ResearchDatasetError
from .store import dumps, loads


SPLIT_SCHEMA = """
CREATE TABLE IF NOT EXISTS split_schemes (
    scheme_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset_id   INTEGER NOT NULL,
    name         TEXT,
    status       TEXT NOT NULL CHECK (status IN ('proposed', 'adopted')),
    time_bound   INTEGER NOT NULL,
    target_ratio REAL NOT NULL,
    spec         TEXT NOT NULL,
    result       TEXT NOT NULL,
    created_at   INTEGER NOT NULL
);
"""


def _ensure_tables(conn):
    conn.executescript(SPLIT_SCHEMA)
    conn.commit()


class _UnionFind:
    def __init__(self):
        self.parent = {}

    def find(self, x):
        self.parent.setdefault(x, x)
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[max(ra, rb)] = min(ra, rb)


class SplitService:
    """对一个 frozen 研究数据集提出、比较并采用训练/验证划分方案。"""

    def __init__(self, dataset_service):
        self.ds = dataset_service
        self.svc = dataset_service.svc
        self.conn = dataset_service.conn
        _ensure_tables(self.conn)

    # ---- 提出方案 ----------------------------------------------------

    def propose_split(self, dataset_id, *, time_bound, target_ratio,
                      labels=None, relations=None, name=None,
                      created_at=None):
        """生成一份划分方案（不可变，迟到资料只能形成新方案）。

        labels: {sample_id: 类别}，用于类别覆盖；缺省按单一类别处理。
        relations: [[sample_id, ...], ...] 研究者声明必须同侧的样本组合。
        返回方案（含 train/validation 分组、实际比例、类别覆盖与
        unsatisfiable 无法满足范围）。
        """
        ds = self.ds.get_dataset(dataset_id)
        if ds["status"] != "frozen":
            raise ResearchDatasetError("只能对已冻结数据集提出划分方案")
        if not 0.0 < target_ratio < 1.0:
            raise ValueError("target_ratio 必须在 (0, 1) 之间")
        labels = dict(labels or {})
        relations = [list(r) for r in (relations or [])]

        included = self.ds.included_samples(dataset_id)
        samples = {sid: self.svc._sample(sid) for sid in included}
        eligible = sorted(sid for sid in included
                          if samples[sid]["as_of"] <= time_bound)
        excluded = [{"sample_id": sid, "reason": "time_bound",
                     "as_of": samples[sid]["as_of"]}
                    for sid in included if samples[sid]["as_of"] > time_bound]

        # 每个样本的来源事件身份与证据 seq（同一事件被多特征引用只计一次）。
        identities, seqs, seals = self._sample_evidence(dataset_id, eligible)

        # 归组：共享原事件 / 同一主体 / 研究者显式关系。
        uf = _UnionFind()
        for sid in eligible:
            uf.find(sid)
        by_event, by_subject = {}, {}
        for sid in eligible:
            for ident in identities[sid]:
                by_event.setdefault(ident, []).append(sid)
            by_subject.setdefault(samples[sid]["entity_id"], []).append(sid)
        for bucket in list(by_event.values()) + list(by_subject.values()):
            for other in bucket[1:]:
                uf.union(bucket[0], other)

        unsatisfiable = []
        eligible_set = set(eligible)
        for rel in relations:
            missing = [s for s in rel if s not in eligible_set]
            known = [s for s in rel if s in eligible_set]
            if missing:
                unsatisfiable.append({
                    "type": "relation_conflict", "relation": sorted(rel),
                    "missing": sorted(missing),
                    "detail": "共同归组关系引用了时间界限外或未纳入的样本，"
                              "该关系涉及的范围无法满足",
                })
            for other in known[1:]:
                uf.union(known[0], other)

        groups = {}
        for sid in eligible:
            groups.setdefault(uf.find(sid), []).append(sid)
        group_list = [sorted(members) for _, members in
                      sorted(groups.items(), key=lambda kv: kv[0])]

        # 类别覆盖：每个类别尽量两侧都有；只有一组的极小类别无法满足。
        class_groups = {}
        for members in group_list:
            classes = {labels.get(sid, "__default__") for sid in members}
            for cls in classes:
                class_groups.setdefault(cls, set()).add(tuple(members))
        tiny = sorted(c for c, gs in class_groups.items() if len(gs) == 1)
        for c in tiny:
            unsatisfiable.append({
                "type": "class_coverage", "label": c,
                "detail": "极小类别只有一个来源组，无法两侧同时覆盖",
            })

        if len(eligible) < 2:
            unsatisfiable.append({
                "type": "insufficient_samples",
                "eligible": len(eligible),
                "detail": "时间界限内历史可见样本不足，无法形成两侧划分；"
                          "不能使用未来资料补齐",
            })
        elif len(group_list) < 2:
            unsatisfiable.append({
                "type": "unsplittable",
                "detail": "所有样本被来源关系连成同一组，无法在不泄漏的"
                          "前提下分成两侧",
            })

        train, validation = self._assign(
            group_list, labels, target_ratio, class_groups)

        result = {
            "dataset_id": dataset_id,
            "time_bound": time_bound,
            "target_ratio": target_ratio,
            "train": sorted(train),
            "validation": sorted(validation),
            "excluded": excluded,
            "groups": group_list,
            "actual_ratio": (len(train) / (len(train) + len(validation))
                             if train or validation else None),
            "class_coverage": self._class_coverage(train, validation, labels),
            "unsatisfiable": unsatisfiable,
        }
        spec = {"labels": labels, "relations": relations}
        cur = self.conn.execute(
            "INSERT INTO split_schemes (dataset_id, name, status, time_bound, "
            "target_ratio, spec, result, created_at) "
            "VALUES (?,?, 'proposed', ?,?,?,?,?)",
            (dataset_id, name, time_bound, target_ratio, dumps(spec),
             dumps(result),
             int(time.time() * 1000) if created_at is None else created_at),
        )
        self.conn.commit()
        result["scheme_id"] = cur.lastrowid
        result["status"] = "proposed"
        return result

    @staticmethod
    def _assign(group_list, labels, target_ratio, class_groups):
        """先保类别两侧覆盖，再按大小贪心逼近目标比例（确定性）。"""
        train, validation = set(), set()
        assigned = set()
        # 第一轮：每个类别最大的两组分置两侧（若该类别有多组）。
        for cls in sorted(class_groups):
            uniq = sorted(class_groups[cls], key=lambda g: (-len(g), g))
            if len(uniq) < 2:
                continue
            for side, g in ((train, uniq[0]), (validation, uniq[1])):
                if g not in assigned:
                    side.update(g)
                    assigned.add(g)
        # 第二轮：剩余组按大小降序，放到能使实际比例更接近目标的一侧。
        remaining = [g for g in group_list if tuple(g) not in assigned]
        remaining.sort(key=lambda g: (-len(g), g))
        total = sum(len(g) for g in group_list)
        for g in remaining:
            after_train = (len(train) + len(g)) / total
            after_val = len(train) / total
            if abs(after_train - target_ratio) <= abs(after_val - target_ratio):
                train.update(g)
            else:
                validation.update(g)
        return train, validation

    @staticmethod
    def _class_coverage(train, validation, labels):
        coverage = {}
        for sid in list(train) + list(validation):
            cls = labels.get(sid, "__default__")
            entry = coverage.setdefault(cls, {"train": 0, "validation": 0})
            entry["train" if sid in train else "validation"] += 1
        return coverage

    def _sample_evidence(self, dataset_id, sample_ids):
        """样本 -> 来源事件身份集合 / 证据 seq 集合 / seal_id 集合。"""
        identities, seqs, seals = {}, {}, {}
        for sid in sample_ids:
            idents, sseqs, sseals = set(), set(), set()
            for cell in self.ds._cells(dataset_id, sid):
                ev = loads(cell["evidence"])
                all_seqs = ev.get("behavior", []) + ev.get("state", [])
                sseqs.update(all_seqs)
                if cell["seal_id"] is not None:
                    sseals.add(cell["seal_id"])
            if sseqs:
                placeholders = ",".join("?" for _ in sseqs)
                for rec in self.conn.execute(
                    "SELECT DISTINCT entity, source, event_id FROM event_log "
                    "WHERE seq IN (" + placeholders + ")", sorted(sseqs),
                ):
                    idents.add((rec["entity"], rec["source"], rec["event_id"]))
            identities[sid] = idents
            seqs[sid] = sseqs
            seals[sid] = sseals
        return identities, seqs, seals

    # ---- 读取 / 比较 / 采用 ------------------------------------------

    def get_scheme(self, scheme_id):
        row = self.conn.execute(
            "SELECT * FROM split_schemes WHERE scheme_id = ?", (scheme_id,),
        ).fetchone()
        if row is None:
            raise KeyError("划分方案不存在: %s" % scheme_id)
        out = loads(row["result"])
        out.update({"scheme_id": row["scheme_id"], "name": row["name"],
                    "status": row["status"], "created_at": row["created_at"]})
        return out

    def list_schemes(self, dataset_id):
        return [self.get_scheme(r["scheme_id"]) for r in self.conn.execute(
            "SELECT scheme_id FROM split_schemes WHERE dataset_id = ? "
            "ORDER BY scheme_id", (dataset_id,),
        )]

    def compare_schemes(self, scheme_ids):
        """并排比较多个方案：比例偏差、类别覆盖、跨侧泄漏核对。"""
        compared = []
        for scheme_id in scheme_ids:
            scheme = self.get_scheme(scheme_id)
            metrics = self.validate_scheme(scheme_id)
            compared.append({
                "scheme_id": scheme_id,
                "name": scheme.get("name"),
                "status": scheme["status"],
                "target_ratio": scheme["target_ratio"],
                "actual_ratio": scheme["actual_ratio"],
                "ratio_gap": (None if scheme["actual_ratio"] is None else
                              abs(scheme["actual_ratio"] - scheme["target_ratio"])),
                "train_count": len(scheme["train"]),
                "validation_count": len(scheme["validation"]),
                "class_coverage": scheme["class_coverage"],
                "unsatisfiable": scheme["unsatisfiable"],
                "isolation": metrics,
            })
        return {"schemes": compared}

    def adopt_scheme(self, scheme_id):
        """显式采用一份方案；同一数据集其余方案回到 proposed。"""
        scheme = self.get_scheme(scheme_id)
        self.conn.execute(
            "UPDATE split_schemes SET status = 'proposed' WHERE dataset_id = ?",
            (scheme["dataset_id"],),
        )
        self.conn.execute(
            "UPDATE split_schemes SET status = 'adopted' WHERE scheme_id = ?",
            (scheme_id,),
        )
        self.conn.commit()
        return self.get_scheme(scheme_id)

    # ---- 来源隔离与实际覆盖核对 --------------------------------------

    def validate_scheme(self, scheme_id):
        """核对来源隔离与实际覆盖。

        * 跨主体共享事件：同一来源事件身份出现在两侧 -> 泄漏；
        * 同一主体出现在两侧 -> 泄漏；
        * 交叠封存：同一 seal_id / 同一证据 seq 出现在两侧 -> 泄漏；
        * 类别实际覆盖：每类别两侧样本数；
        * 未来资料：任何证据 arrival_time 不得晚于样本 as_of。
        """
        scheme = self.get_scheme(scheme_id)
        dataset_id = scheme["dataset_id"]
        train = set(scheme["train"])
        validation = set(scheme["validation"])
        all_sids = sorted(train | validation)
        identities, seqs, seals = self._sample_evidence(dataset_id, all_sids)

        def side_of(sid):
            return "train" if sid in train else "validation"

        cross_events, cross_subjects, cross_seals, cross_seqs = [], [], [], []
        seen_events, seen_subjects, seen_seals, seen_seqs = {}, {}, {}, {}
        for sid in all_sids:
            side = side_of(sid)
            subject = self.svc._sample(sid)["entity_id"]
            for ident in identities[sid]:
                prev = seen_events.get(ident)
                if prev is not None and prev[0] != side:
                    cross_events.append({"identity": list(ident),
                                         "sides": [prev[1], sid]})
                seen_events.setdefault(ident, (side, sid))
            prev = seen_subjects.get(subject)
            if prev is not None and prev[0] != side:
                cross_subjects.append({"subject": subject,
                                       "sides": [prev[1], sid]})
            seen_subjects.setdefault(subject, (side, sid))
            for seal_id in seals[sid]:
                prev = seen_seals.get(seal_id)
                if prev is not None and prev[0] != side:
                    cross_seals.append({"seal_id": seal_id,
                                        "sides": [prev[1], sid]})
                seen_seals.setdefault(seal_id, (side, sid))
            for seq in seqs[sid]:
                prev = seen_seqs.get(seq)
                if prev is not None and prev[0] != side:
                    cross_seqs.append({"seq": seq, "sides": [prev[1], sid]})
                seen_seqs.setdefault(seq, (side, sid))

        future = []
        for sid in all_sids:
            as_of = self.svc._sample(sid)["as_of"]
            for seq in seqs[sid]:
                rec = self.svc._log_record(seq)
                if rec["arrival_time"] > as_of:
                    future.append({"sample_id": sid, "seq": seq})

        # 独立证据身份计数：同一事件被多特征/多样本引用只计一次。
        distinct_events = {i for sid in all_sids for i in identities[sid]}
        return {
            "isolated": not (cross_events or cross_subjects or cross_seals
                             or cross_seqs or future),
            "cross_side_events": cross_events,
            "cross_side_subjects": cross_subjects,
            "cross_side_seals": cross_seals,
            "cross_side_evidence": cross_seqs,
            "future_data": future,
            "class_coverage": scheme["class_coverage"],
            "distinct_source_events": len(distinct_events),
        }

