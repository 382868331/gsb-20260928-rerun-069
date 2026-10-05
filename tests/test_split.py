"""防泄漏切分：共享事件/同主体归组、时间界限、约束冲突、
极小类别、方案比较与采用、迟到资料只形成新方案、交叠封存隔离。
"""

import os
import tempfile
import unittest

from feature_service import (
    FeatureService,
    LeakageSplitService,
    ResearchDatasetService,
    SplitError,
)


class LeakageSplitTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "d.db")
        self.svc = FeatureService(self.db)
        self.ds = ResearchDatasetService(self.svc)
        self.sp = LeakageSplitService(self.ds)

    def tearDown(self):
        self.svc.close()
        self.tmp.cleanup()

    def _setup_dataset(self):
        # 两个特征合法引用同一来源事件（pay），同一事件不得被当成
        # 独立证据跨侧分配。
        self.svc.register_feature(
            "pay", entity="user", windows=[1000],
            aggregations=["count", "sum"], value_field="amount",
            sources=("pay",), created_at=0)
        self.svc.register_feature(
            "risk", entity="user", windows=[1000],
            aggregations=["count"], value_field="amount",
            sources=("pay",), created_at=0)
        # u3 与 u4 跨主体共享同一原事件 shared1（如一次转账双方入账）。
        events = {
            "u1": ["a1"], "u2": ["a2"], "u3": ["a3", "shared1"],
            "u4": ["a4", "shared1"], "u5": ["a5"], "u6": ["a6"],
            "u7": ["a7"],
        }
        for user, ids in events.items():
            for i, eid in enumerate(ids):
                self.svc.ingest(eid, user, source="pay",
                                event_time=100 + i, arrival_time=100 + i,
                                payload={"amount": 10})
        # 每个主体一个样本；u1 另有一个不同观察时刻的样本（同主体归组）；
        # s7 的观察时刻在时间界限之外。
        as_ofs = {"s1": 900, "s1b": 950, "s2": 900, "s3": 900, "s4": 900,
                  "s5": 900, "s6": 900, "s7": 2000}
        for sid, user in [("s1", "u1"), ("s1b", "u1"), ("s2", "u2"),
                          ("s3", "u3"), ("s4", "u4"), ("s5", "u5"),
                          ("s6", "u6"), ("s7", "u7")]:
            self.svc.register_sample(sid, "user", user, as_of=as_ofs[sid])
            self.svc.materialize(sid, "pay")
            self.svc.materialize(sid, "risk")
            # 交叠封存：两批范围交叠，首次确认的值保持不变。
            self.svc.seal(sid, "pay", 1,
                          feature_keys=["pay_w1000_count"], sealed_at=1100)
            self.svc.seal(sid, "pay", 1, sealed_at=1200)
            self.svc.seal(sid, "risk", 1, sealed_at=1200)
        did = self.ds.create_dataset("study", created_at=1300)
        self.ds.add_feature_view(did, "pay", 1)
        self.ds.add_feature_view(did, "risk", 1)
        self.ds.add_samples(did, list(as_ofs))
        self.ds.freeze_dataset(did, frozen_at=1300)
        return did

    _LABELS = {"s1": "neg", "s1b": "neg", "s2": "neg", "s3": "neg",
               "s4": "neg", "s5": "pos", "s6": "pos", "s7": "pos"}

    def test_shared_event_and_same_entity_grouped(self):
        did = self._setup_dataset()
        result = self.sp.propose_split(
            did, time_boundary=1000, train_ratio=0.6, labels=self._LABELS)
        summary = result["summary"]
        self.assertTrue(summary["leak_free"])
        sides = {a["sample_id"]: a["side"]
                 for a in self.sp.get_scheme(result["scheme_id"])["assignments"]}
        # 跨主体共享事件 shared1：s3/s4 同侧；同主体 s1/s1b 同侧。
        self.assertEqual(sides["s3"], sides["s4"])
        self.assertEqual(sides["s1"], sides["s1b"])
        self.assertEqual(sides["s7"], "excluded")
        self.assertEqual(summary["excluded"][0]["reason"],
                         "beyond_time_boundary")
        # 类别两侧都有覆盖。
        for lab in ("neg", "pos"):
            cov = summary["class_coverage"][lab]
            self.assertGreater(cov["train"], 0)
            self.assertGreater(cov["validation"], 0)

    def test_insufficient_history_reports_unmet(self):
        did = self._setup_dataset()
        result = self.sp.propose_split(
            did, time_boundary=100, train_ratio=0.5, labels=self._LABELS)
        kinds = {u["type"] for u in result["unmet"]}
        self.assertIn("insufficient_history", kinds)
        # 不用未来资料补齐：两侧样本总数不足。
        total = (len(result["summary"]["train"])
                 + len(result["summary"]["validation"]))
        self.assertLess(total, 2)

    def test_co_group_conflict_reported(self):
        did = self._setup_dataset()
        result = self.sp.propose_split(
            did, time_boundary=1000, train_ratio=0.5, labels=self._LABELS,
            co_groups=[["s1", "s7"]])  # s7 超出时间界限
        conflicts = [u for u in result["unmet"]
                     if u["type"] == "co_group_conflict"]
        self.assertEqual(len(conflicts), 1)
        self.assertEqual(conflicts[0]["pair"], ["s1", "s7"])

    def test_tiny_class_reported(self):
        did = self._setup_dataset()
        labels = dict(self._LABELS, s5="rare", s6="rare")
        # s5/s6 不共享事件也不共主体 -> rare 跨两组，可覆盖；把 s6 改回 pos
        labels["s6"] = "pos"
        result = self.sp.propose_split(
            did, time_boundary=1000, train_ratio=0.5, labels=labels)
        kinds = {u["type"] for u in result["unmet"]}
        self.assertIn("tiny_class", kinds)
        self.assertIn("rare", result["summary"]["tiny_classes"])

    def test_compare_and_adopt_scheme(self):
        did = self._setup_dataset()
        r1 = self.sp.propose_split(did, time_boundary=1000, train_ratio=0.5,
                                   labels=self._LABELS, name="half")
        r2 = self.sp.propose_split(did, time_boundary=1000, train_ratio=0.75,
                                   labels=self._LABELS, name="three-quarter")
        compared = self.sp.compare_schemes(did)
        self.assertEqual(len(compared), 2)
        self.assertTrue(all(c["leak_free"] for c in compared))
        self.sp.adopt_scheme(r1["scheme_id"])
        self.sp.adopt_scheme(r2["scheme_id"])
        schemes = {s["scheme_id"]: s["status"] for s in self.sp.list_schemes(did)}
        self.assertEqual(schemes[r1["scheme_id"]], "superseded")
        self.assertEqual(schemes[r2["scheme_id"]], "adopted")

    def test_late_arrival_only_forms_new_scheme(self):
        did = self._setup_dataset()
        result = self.sp.propose_split(
            did, time_boundary=1000, train_ratio=0.5, labels=self._LABELS)
        before = self.sp.get_scheme(result["scheme_id"])["assignments"]
        # 迟到资料到账：旧方案不变，只提示需要新方案。
        self.svc.ingest("late1", "u2", source="pay", event_time=150,
                        arrival_time=1500, payload={"amount": 99})
        summary = self.sp.verify_split(result["scheme_id"])
        self.assertTrue(summary["stale"])
        self.assertEqual(before,
                         self.sp.get_scheme(result["scheme_id"])["assignments"])
        r2 = self.sp.propose_split(did, time_boundary=1000, train_ratio=0.5,
                                   labels=self._LABELS, name="after-late")
        self.assertNotEqual(result["scheme_id"], r2["scheme_id"])

    def test_overlapping_seals_keep_isolation(self):
        did = self._setup_dataset()
        result = self.sp.propose_split(
            did, time_boundary=1000, train_ratio=0.6, labels=self._LABELS)
        summary = result["summary"]
        # 交叠封存核对：共享事件/主体零跨侧，证据身份无泄漏。
        self.assertEqual(summary["shared_events_across_sides"], [])
        self.assertEqual(summary["shared_entities_across_sides"], [])
        # 同一事件被 pay+risk 两特征引用只计一次，不增加覆盖计数。
        cov = self.ds.coverage(did)
        self.assertEqual(cov["sample_count"], 8)
        self.assertEqual(cov["source_event_count"], 8)  # a1..a7 + shared1

    def test_adopt_rejects_leaking_scheme(self):
        did = self._setup_dataset()
        result = self.sp.propose_split(
            did, time_boundary=1000, train_ratio=0.5, labels=self._LABELS)
        sid = result["scheme_id"]
        # 手工破坏分配制造跨侧泄漏，采用时必须被拒绝。
        self.sp.conn.execute(
            "UPDATE research_split_assignments SET side = 'train' "
            "WHERE scheme_id = ? AND sample_id = 's3'", (sid,))
        self.sp.conn.execute(
            "UPDATE research_split_assignments SET side = 'validation' "
            "WHERE scheme_id = ? AND sample_id = 's4'", (sid,))
        self.sp.conn.commit()
        self.assertFalse(self.sp.verify_split(sid)["leak_free"])
        with self.assertRaises(SplitError):
            self.sp.adopt_scheme(sid)


if __name__ == "__main__":
    unittest.main()
