"""固定研究数据集（research dataset）组装与冻结服务。

在 feature_service.FeatureService 的“开放/封存特征 + 修订单”之上，提供：

* 每个样本按自己的 as_of 观察时刻取数，跨样本不统一取最新值；
* 每个值说明来源：某次封存（sealed）或显式采用的已发布修订（revision）；
* 区分“尚未确认”（有可见值但范围仍开放）与“根本没有可用证据”；
* 冻结后值与血缘原样持久化，迟到资料与历史修订不自动进入旧数据集；
* 更新建议按 sample_scope / revision / visible_selection 分类，
  未采用的差异原样保留在新数据集上；
* 样本覆盖按样本身份计数；独立来源证据按来源事件身份
  (entity, source, event_id) 计数，同一事件被多特征引用不多计。
"""

from __future__ import annotations

import time

from .dataset_store import ensure_dataset_tables
from .engine import feature_keys
from .store import dumps, loads


class ResearchDatasetError(RuntimeError):
    pass


class ResearchDatasetService:
    """以一个已有的 FeatureService 连接组装固定研究数据集。"""

    def __init__(self, feature_service):
        self.svc = feature_service
        self.conn = feature_service.conn
        ensure_dataset_tables(self.conn)

    # ---- 数据集定义 --------------------------------------------------

    def create_dataset(self, name=None, *, created_at=None, note=None):
        created_at = self._now(created_at)
        cur = self.conn.execute(
            "INSERT INTO research_datasets (name, status, created_at, note) "
            "VALUES (?, 'draft', ?, ?)",
            (name, created_at, note),
        )
        self.conn.commit()
        return cur.lastrowid

    @staticmethod
    def _now(value):
        return int(time.time() * 1000) if value is None else value

    def get_dataset(self, dataset_id):
        row = self.conn.execute(
            "SELECT * FROM research_datasets WHERE dataset_id = ?", (dataset_id,)
        ).fetchone()
        if row is None:
            raise KeyError(f"数据集不存在: {dataset_id}")
        return dict(row)

    def _require_draft(self, dataset_id):
        if self.get_dataset(dataset_id)["status"] != "draft":
            raise ResearchDatasetError("数据集已冻结，不能修改")

    def add_feature_view(self, dataset_id, feature_name, version=None,
                         revision_ids=()):
        """声明数据集采用的特征视角。

        revision_ids 是本数据集显式采用的已发布修订单（绑定具体样本）。
        未列入修订单的样本/键回退到其已封存视角。
        """
        self._require_draft(dataset_id)
        spec = self.svc._get_spec(feature_name, version)
        for revision_id in revision_ids or ():
            rev = self.svc._revision_row(revision_id)
            if rev["feature_name"] != feature_name or rev["version"] != spec.version:
                raise ValueError("修订单与特征视角不一致")
            if rev["status"] != "published":
                raise ValueError("只能采用已发布的修订")
        ids = list(revision_ids or ()) if revision_ids else [None]
        for revision_id in ids:
            self.conn.execute(
                "INSERT OR IGNORE INTO research_dataset_features "
                "(dataset_id, feature_name, version, revision_id) VALUES (?,?,?,?)",
                (dataset_id, feature_name, spec.version, revision_id),
            )
        self.conn.commit()
        return spec.version

    def add_samples(self, dataset_id, sample_ids):
        """登记候选样本（含可能因时间/封存范围不足而无法纳入者）。"""
        self._require_draft(dataset_id)
        for sample_id in sample_ids:
            self.svc._sample(sample_id)
            self.conn.execute(
                "INSERT OR IGNORE INTO research_dataset_samples "
                "(dataset_id, sample_id, included) VALUES (?, ?, 0)",
                (dataset_id, sample_id),
            )
        self.conn.commit()

    def _views(self, dataset_id):
        return list(self.conn.execute(
            "SELECT feature_name, version, revision_id FROM research_dataset_features "
            "WHERE dataset_id = ? ORDER BY feature_name, version, revision_id",
            (dataset_id,),
        ))

    def _candidate_samples(self, dataset_id):
        return list(self.conn.execute(
            "SELECT s.sample_id, s.entity_type, s.entity_id, s.as_of "
            "FROM research_dataset_samples d JOIN samples s USING (sample_id) "
            "WHERE d.dataset_id = ? ORDER BY s.sample_id",
            (dataset_id,),
        ))

    # ---- 视角解析 ----------------------------------------------------

    def _revision_view(self, dataset_id):
        """{sample_id: {(feature_name, version, key): revision_entry + id}}。"""
        view = {}
        for v in self._views(dataset_id):
            if v["revision_id"] is None:
                continue
            rev = self.svc.get_revision(v["revision_id"])
            bucket = view.setdefault(rev["sample_id"], {})
            for entry in rev["entries"]:
                bucket[(v["feature_name"], v["version"], entry["feature_key"])] = {
                    "revision_id": v["revision_id"], "entry": entry,
                }
        return view

    def _resolve_view(self, sample, feature_name, version, rev_bucket):
        """解析一个样本在一个特征视角下每个 feature_key 的来源。"""
        spec = self.svc._get_spec(feature_name, version)
        as_of = sample["as_of"]
        ver_row = self.conn.execute(
            "SELECT created_at FROM feature_versions WHERE name = ? AND version = ?",
            (feature_name, version),
        ).fetchone()
        definition_after = ver_row is not None and ver_row["created_at"] > as_of
        stored = self.svc._stored_rows(sample["sample_id"], feature_name, version)
        stored_ev = self.svc._stored_evidence(
            sample["sample_id"], feature_name, version)
        out = {}
        for key in feature_keys(spec):
            if definition_after:
                out[key] = {"available": False, "reason": "time_limit", "value": None,
                            "evidence": {"behavior": [], "state": []}}
                continue
            adopted = rev_bucket.get((feature_name, version, key))
            if adopted is not None:
                entry = adopted["entry"]
                out[key] = {"available": True, "kind": "revision",
                            "value": entry["new_value"],
                            "revision_id": adopted["revision_id"],
                            "seal_id": None, "evidence": entry["evidence"]}
                continue
            row = stored.get(key)
            if row is None:
                out[key] = {"available": False, "reason": "no_evidence", "value": None,
                            "evidence": {"behavior": [], "state": []}}
                continue
            ev = stored_ev.get(key, {"behavior": [], "state": []})
            if row["status"] == "sealed":
                out[key] = {"available": True, "kind": "sealed",
                            "value": row["value"], "seal_id": row["seal_id"],
                            "revision_id": None, "evidence": ev}
            else:
                has_ev = bool(ev["behavior"] or ev["state"])
                out[key] = {"available": False,
                            "reason": "unconfirmed" if has_ev else "no_evidence",
                            "value": row["value"], "seal_id": None,
                            "revision_id": None, "evidence": ev}
        return out

    def preview_dataset(self, dataset_id):
        """预览样本完整性与每个值的实际来源/缺失原因。

        reasons: unconfirmed（尚未确认）、no_evidence（无可用证据）、
        time_limit（受观察时刻限制，如定义在 as_of 之后）。
        """
        self.get_dataset(dataset_id)
        rev_view = self._revision_view(dataset_id)
        sample_rows = self._candidate_samples(dataset_id)
        samples = []
        for sample in sample_rows:
            cells, reasons = {}, set()
            for view in self._views(dataset_id):
                bucket = rev_view.get(sample["sample_id"], {})
                resolved = self._resolve_view(
                    sample, view["feature_name"], view["version"], bucket)
                for key, info in resolved.items():
                    fq = f'{view["feature_name"]}:{view["version"]}:{key}'
                    cells[fq] = info
                    if not info["available"]:
                        reasons.add(info["reason"])
            samples.append({
                "sample_id": sample["sample_id"], "as_of": sample["as_of"],
                "complete": not reasons, "reasons": sorted(reasons),
                "cells": cells,
            })
        return {
            "dataset_id": dataset_id,
            "complete": [s["sample_id"] for s in samples if s["complete"]],
            "incomplete": [{"sample_id": s["sample_id"], "reasons": s["reasons"]}
                           for s in samples if not s["complete"]],
            "samples": samples,
        }

    # ---- 冻结 --------------------------------------------------------

    def freeze_dataset(self, dataset_id, *, frozen_at=None, sample_ids=None,
                       note=None):
        """冻结数据集：仅纳入完整样本，逐格复制值与血缘。

        冻结后即使账本补登迟到资料或发布历史特征修订，本数据集不变。
        ``sample_ids`` 可只冻结完整样本的一个子集；其余记为未纳入。
        """
        self._require_draft(dataset_id)
        preview = self.preview_dataset(dataset_id)
        chosen = set(sample_ids) if sample_ids is not None else None
        frozen_at = self._now(frozen_at)
        for s in preview["samples"]:
            include = s["complete"] and (chosen is None or s["sample_id"] in chosen)
            reason = None if include else ("excluded" if s["complete"] else ",".join(s["reasons"]))
            self.conn.execute(
                "INSERT INTO research_dataset_samples (dataset_id, sample_id, included, reason) "
                "VALUES (?,?,?,?) ON CONFLICT(dataset_id, sample_id) DO UPDATE SET "
                "included = excluded.included, reason = excluded.reason",
                (dataset_id, s["sample_id"], 1 if include else 0, reason),
            )
            if not include:
                continue
            for fq, info in s["cells"].items():
                feature_name, ver_text, key = fq.split(":", 2)
                self._write_cell(dataset_id, s["sample_id"], feature_name,
                                 int(ver_text), key, info)
        self.conn.execute(
            "UPDATE research_datasets SET status = 'frozen', frozen_at = ?, note = COALESCE(?, note) "
            "WHERE dataset_id = ?",
            (frozen_at, note, dataset_id),
        )
        self.conn.commit()
        return self.get_dataset(dataset_id)

    def _write_cell(self, dataset_id, sample_id, feature_name, version, key, info):
        # 防线：任何证据的 arrival_time 不得晚于样本 as_of。
        sample = self.svc._sample(sample_id)
        for seq in info["evidence"].get("behavior", []) + info["evidence"].get("state", []):
            rec = self.svc._log_record(seq)
            if rec["arrival_time"] > sample["as_of"]:
                raise ResearchDatasetError(
                    f"{key} 使用了未来资料 seq={seq}，整行不能标为当时可用")
        self.conn.execute(
            "INSERT INTO research_dataset_cells "
            "(dataset_id, sample_id, feature_name, version, feature_key, value, "
            " source_kind, seal_id, revision_id, evidence) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (dataset_id, sample_id, feature_name, version, key,
             dumps(info["value"]), info["kind"], info.get("seal_id"),
             info.get("revision_id"), dumps(info["evidence"])),
        )

    # ---- 读取 / 血缘 / 覆盖统计 --------------------------------------

    def included_samples(self, dataset_id):
        return [r["sample_id"] for r in self.conn.execute(
            "SELECT sample_id FROM research_dataset_samples "
            "WHERE dataset_id = ? AND included = 1 ORDER BY sample_id",
            (dataset_id,),
        )]

    def excluded_samples(self, dataset_id):
        return [{"sample_id": r["sample_id"], "reason": r["reason"]}
                for r in self.conn.execute(
                    "SELECT sample_id, reason FROM research_dataset_samples "
                    "WHERE dataset_id = ? AND included = 0 ORDER BY sample_id",
                    (dataset_id,),
                )]

    def read_row(self, dataset_id, sample_id):
        """逐项读取某样本冻结行：{fq: value}，重开后与冻结时一致。"""
        return {self._fq(r): loads(r["value"])
                for r in self._cells(dataset_id, sample_id)}

    @staticmethod
    def _fq(r):
        return f'{r["feature_name"]}:{r["version"]}:{r["feature_key"]}'

    def _cells(self, dataset_id, sample_id=None):
        sql = "SELECT * FROM research_dataset_cells WHERE dataset_id = ?"
        params = [dataset_id]
        if sample_id is not None:
            sql += " AND sample_id = ?"
            params.append(sample_id)
        sql += " ORDER BY sample_id, feature_name, version, feature_key"
        return list(self.conn.execute(sql, params))

    def lineage(self, dataset_id, sample_id):
        """逐格说明值的实际来源（seal_id / revision_id）与账本证据。"""
        out = {}
        for r in self._cells(dataset_id, sample_id):
            seqs = loads(r["evidence"]) 
            all_seqs = seqs.get("behavior", []) + seqs.get("state", [])
            records = self.svc._evidence_records(all_seqs)
            out[self._fq(r)] = {
                "value": loads(r["value"]), "source_kind": r["source_kind"],
                "seal_id": r["seal_id"], "revision_id": r["revision_id"],
                "evidence": seqs, "records": [records[s] for s in all_seqs],
            }
        return out

    def coverage(self, dataset_id):
        """样本覆盖按样本身份计数；独立来源证据按来源事件身份计数。

        同一来源事件被多个特征/多个键引用，只计一次；一个样本多个特征
        也不增加样本数。
        """
        cells = self._cells(dataset_id)
        sample_ids = {r["sample_id"] for r in cells}
        identities = set()
        seqs = {s for r in cells for role in loads(r["evidence"]).values() for s in role}
        if seqs:
            placeholders = ",".join("?" for _ in seqs)
            for rec in self.conn.execute(
                f"SELECT DISTINCT entity, source, event_id FROM event_log "
                f"WHERE seq IN ({placeholders})", sorted(seqs),
            ):
                identities.add((rec["entity"], rec["source"], rec["event_id"]))
        return {"sample_count": len(sample_ids),
                "sample_ids": sorted(sample_ids),
                "source_event_count": len(identities),
                "source_events": sorted(identities)}

    # ---- 更新建议与采用 ----------------------------------------------

    def _pending(self, dataset_id):
        return list(self.conn.execute(
            "SELECT * FROM research_dataset_pending_diffs WHERE dataset_id = ? "
            "ORDER BY sample_id, feature_name, version, feature_key",
            (dataset_id,),
        ))

    def propose_update(self, dataset_id, candidate_sample_ids=()):
        """对已冻结数据集提出更新（不改写任何数据）。

        * sample_scope：候选集中出现了新的、可完整纳入的样本；
        * revision：针对已冻结单元格存在“数据集尚未采用”的已发布修订；
        * visible_selection：同一封存视角在当前账本下当时可见资料的选择
          与冻结值不同、但尚无已发布修订可采用（仅提示，保留不自动采用）。

        先前仍未采用的差异会一并保留返回。
        """
        ds = self.get_dataset(dataset_id)
        if ds["status"] != "frozen":
            raise ResearchDatasetError("只能对已冻结数据集提出更新")
        diffs = {tuple(self._diff_key(r)): self._diff_dict(r)
                 for r in self._pending(dataset_id)}

        adopted_rev = {}  # (sample,fname,ver,key) -> 已采用 revision_id
        for r in self._cells(dataset_id):
            if r["revision_id"] is not None:
                adopted_rev[(r["sample_id"], r["feature_name"], r["version"],
                             r["feature_key"])] = r["revision_id"]

        # 1) revision：任何针对已冻结格的、尚未采用的已发布修订。
        for r in self._cells(dataset_id):
            key = (r["sample_id"], r["feature_name"], r["version"], r["feature_key"])
            since = adopted_rev.get(key, 0)
            rows = self.conn.execute(
                "SELECT revision_id FROM revisions WHERE sample_id=? AND feature_name=? "
                "AND version=? AND status='published' AND revision_id>? "
                "ORDER BY revision_id",
                (r["sample_id"], r["feature_name"], r["version"], since),
            ).fetchall()
            if not rows:
                continue
            rev = self.svc.get_revision(rows[-1]["revision_id"])
            entry = next((e for e in rev["entries"] if e["feature_key"] == r["feature_key"]), None)
            if entry is None:
                continue
            old_value = loads(r["value"])
            if entry["new_value"] != old_value or since == 0:
                self._put_diff(diffs, r["sample_id"], r["feature_name"], r["version"],
                               r["feature_key"], "revision", old_value,
                               entry["new_value"],
                               f'可采用修订单 #{rev["revision_id"]}')

        # 2) sample_scope：新候选样本在当前视角下可完整纳入。
        included = set(self.included_samples(dataset_id))
        for sample_id in sorted(set(candidate_sample_ids) - included):
            sample = self.svc._sample(sample_id)
            for view in self._views(dataset_id):
                if view["revision_id"] is not None:
                    continue
                resolved = self._resolve_view(
                    sample, view["feature_name"], view["version"], {})
                for fkey, info in resolved.items():
                    if info["available"]:
                        self._put_diff(diffs, sample_id, view["feature_name"],
                                       view["version"], fkey, "sample_scope",
                                       None, info["value"], "新增可纳入样本")

        # 3) visible_selection：封存视角在当前账本下重算不同（无修订可采用）。
        for r in self._cells(dataset_id):
            if r["source_kind"] != "sealed":
                continue
            key = (r["sample_id"], r["feature_name"], r["version"], r["feature_key"])
            if key in diffs:  # 已有 revision 建议
                continue
            sample = self.svc._sample(r["sample_id"])
            resolved = self._resolve_view(
                sample, r["feature_name"], r["version"], {})
            info = resolved.get(r["feature_key"])
            if info is not None and info["available"] and info["value"] != loads(r["value"]):
                self._put_diff(diffs, r["sample_id"], r["feature_name"], r["version"],
                               r["feature_key"], "visible_selection",
                               loads(r["value"]), info["value"],
                               "当前账本下当时可见资料的选择不同（尚无修订可采用）")

        return {"dataset_id": dataset_id, "diffs": list(diffs.values()),
                "by_category": self._group_category(diffs)}

    @staticmethod
    def _diff_key(r):
        return (r["sample_id"], r["feature_name"], r["version"], r["feature_key"])

    @staticmethod
    def _diff_dict(r):
        return {"sample_id": r["sample_id"], "feature_name": r["feature_name"],
                "version": r["version"], "feature_key": r["feature_key"],
                "category": r["category"], "old_value": loads(r["old_value"]),
                "new_value": loads(r["new_value"]), "detail": loads(r["detail"])}

    @staticmethod
    def _group_category(diffs):
        grouped = {"sample_scope": [], "revision": [], "visible_selection": []}
        for d in diffs.values():
            grouped[d["category"]].append(d)
        return grouped

    @staticmethod
    def _put_diff(diffs, sample_id, fname, ver, key, category, old, new, detail):
        diffs[(sample_id, fname, ver, key)] = {
            "sample_id": sample_id, "feature_name": fname, "version": ver,
            "feature_key": key, "category": category, "old_value": old,
            "new_value": new, "detail": detail,
        }
    def _newer_revision(self, sample_id, feature_name, version, after_revision_id):
        rows = self.conn.execute(
            "SELECT revision_id FROM revisions WHERE sample_id = ? AND feature_name = ? "
            "AND version = ? AND status = 'published' AND revision_id > ? "
            "ORDER BY revision_id DESC",
            (sample_id, feature_name, version, after_revision_id),
        ).fetchall()
        if not rows:
            return None
        return self.svc.get_revision(rows[0]["revision_id"])

    def adopt_update(self, parent_id, diff_keys, *, name=None, frozen_at=None,
                     note=None, created_at=None):
        """把选中的差异落到一个新的 frozen 数据集；未选中的差异保留。

        diff_keys 为 [(sample_id, feature_name, version, feature_key)]。
        只支持采用 revision / sample_scope 两类（显式确认的变化）；
        visible_selection 等其余差异原样带到新数据集的待办中。
        """
        parent = self.get_dataset(parent_id)
        if parent["status"] != "frozen":
            raise ResearchDatasetError("只能基于已冻结数据集建立更新")
        proposal = self.propose_update(parent_id)
        chosen = {tuple(k) for k in diff_keys}
        adopt_rev_ids = set()
        for d in proposal["diffs"]:
            key = (d["sample_id"], d["feature_name"], d["version"], d["feature_key"])
            if key in chosen and d["category"] == "revision":
                rid = self._parse_revision_id(d["detail"])
                if rid is not None:
                    adopt_rev_ids.add((d["feature_name"], d["version"], rid))

        new_id = self.create_dataset(name, created_at=created_at, note=note)
        self.conn.execute(
            "UPDATE research_datasets SET parent_id = ? WHERE dataset_id = ?",
            (parent_id, new_id),
        )
        # 复制视角，并叠加新采用的修订单。
        for v in self._views(parent_id):
            self.conn.execute(
                "INSERT OR IGNORE INTO research_dataset_features "
                "(dataset_id, feature_name, version, revision_id) VALUES (?,?,?,?)",
                (new_id, v["feature_name"], v["version"], v["revision_id"]),
            )
        for fname, ver, rid in adopt_rev_ids:
            self.conn.execute(
                "INSERT OR IGNORE INTO research_dataset_features "
                "(dataset_id, feature_name, version, revision_id) VALUES (?,?,?,?)",
                (new_id, fname, ver, rid),
            )
        # 样本：父集样本 + sample_scope 新样本。
        sample_ids = set(self.included_samples(parent_id))
        for d in proposal["diffs"]:
            key = (d["sample_id"], d["feature_name"], d["version"], d["feature_key"])
            if key in chosen and d["category"] == "sample_scope":
                sample_ids.add(d["sample_id"])
        self.add_samples(new_id, sorted(sample_ids))
        self.conn.commit()
        self.freeze_dataset(new_id, frozen_at=frozen_at, sample_ids=sorted(sample_ids))
        if note is not None:
            self.conn.execute(
                "UPDATE research_datasets SET note = ? WHERE dataset_id = ?",
                (note, new_id))
        # 未采用的差异（含 visible_selection）保留到新数据集待办。
        for d in proposal["diffs"]:
            key = (d["sample_id"], d["feature_name"], d["version"], d["feature_key"])
            if key not in chosen:
                self.conn.execute(
                    "INSERT OR REPLACE INTO research_dataset_pending_diffs "
                    "(dataset_id, sample_id, feature_name, version, feature_key, "
                    " category, old_value, new_value, detail) VALUES (?,?,?,?,?,?,?,?,?)",
                    (new_id, d["sample_id"], d["feature_name"], d["version"],
                     d["feature_key"], d["category"], dumps(d["old_value"]),
                     dumps(d["new_value"]), dumps(d["detail"])),
                )
        self.conn.commit()
        return new_id

    @staticmethod
    def _parse_revision_id(detail):
        marker = "修订单 #"
        if marker not in detail:
            return None
        tail = detail.split(marker, 1)[1].rstrip("）) ")
        try:
            return int(tail)
        except ValueError:
            return None

    def pending_diffs(self, dataset_id):
        """列出仍未采用并被保留下来的差异。"""
        return [self._diff_dict(r) for r in self._pending(dataset_id)]
