"""按来源泄漏约束的训练/验证划分：跨主体共享事件、同一主体归组、
极小类别、交叠封存核对、时间界限、方案比较与采用、迟到资料只成新方案。
"""

import os
import tempfile
import unittest

from feature_service import FeatureService, ResearchDatasetService, SplitService


class SplitServiceTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "d.db")
        self.svc = FeatureService(self.db)
        self.ds = ResearchDatasetService(self.svc)
        self.sp = SplitService(self.ds)

    def tearDown(self):
        self.svc.close()
        self.tmp.cleanup()

    def _setup(self):
        # risk 特征同时引用 pay 来源：同一事件被两个特征合法引用。
        self.svc.register_feature(
            "pay", entity="user", windows=[1000],
            aggregations=["count", "sum"], value_field="amount",
            sources=("pay",), created_at=0)
        self.svc.register_feature(
            "risk", entity="user", windows=[1000],
            aggregations=["count"], value_field="amount",
            sources=("pay", "risk"), created_at=0)
        # u1：样本 s1(as_of=1000) 与 s1b(as_of=1500)，同一主体。
        self.svc.ingest("p1", "u1", source="pay", event_time=100,
                        arrival_time=100, payload={"amount": 10})
        self.svc.ingest("p2", "u1", source="pay", event_time=200,
                        arrival_time=200, payload={"amount": 20})
        # u2：s2(as_of=500)，pay+risk 两个来源。
        self.svc.ingest("p1", "u2", source="pay", event_time=100,
                        arrival_time=100, payload={"amount": 5})
        self.svc.ingest("r1", "u2", source="risk", event_time=150,
                        arrival_time=150, payload={"amount": 9})
        # u3：s3(as_of=1000)。
        self.svc.ingest("p9", "u3", source="pay", event_time=100,
                        arrival_time=100, payload={"amount": 7})
        # u4：s4(as_of=2000)，晚于常用时间界限。
        self.svc.ingest("p10", "u4", source="pay", event_time=100,
                        arrival_time=100, payload={"amount": 3})
        for sid, uid, as_of in [("s1", "u1", 1000), ("s1b", "u1", 1500),
                                ("s2", "u2", 500), ("s3", "u3", 1000),
                                ("s4", "u4", 2000)]:
            self.svc.register_sample(sid, "user", uid, as_of=as_of)
            self.svc.materialize(sid, "pay")
            self.svc.materialize(sid, "risk")
            self.svc.seal(sid, "pay", 1, sealed_at=as_of + 1)
            self.svc.seal(sid, "risk", 1, sealed_at=as_of + 1)

        did = self.ds.create_dataset("study", created_at=3000)
        self.ds.add_feature_view(did, "pay", 1)
        self.ds.add_feature_view(did, "risk", 1)
        self.ds.add_samples(did, ["s1", "s1b", "s2", "s3", "s4"])
        self.ds.freeze_dataset(did, frozen_at=3000)
        return did

    def _labels(self):
        return {"s1": "A", "s1b": "A", "s2": "B", "s3": "B", "s4": "B"}

    def test_same_subject_co_grouped_and_time_bound(self):
        did = self._setup()
        scheme = self.sp.propose_split(
            did, time_bound=1500, target_ratio=0.5, labels=self._labels())
        # s4 超出时间界限被排除，不能用未来资料补齐。
        self.assertEqual([e["sample_id"] for e in scheme["excluded"]], ["s4"])
        # 同一主体的 s1/s1b 必须同侧。
        sides = {sid: ("train" if sid in scheme["train"] else "validation")
                 for sid in scheme["train"] + scheme["validation"]}
        self.assertEqual(sides["s1"], sides["s1b"])
        # 极小类别 A 只有一个来源组 -> 无法满足两侧覆盖。
        tiny = [u for u in scheme["unsatisfiable"]
                if u["type"] == "class_coverage"]
        self.assertEqual([u["label"] for u in tiny], ["A"])
        # 类别 B 两侧都有覆盖。
        self.assertGreater(scheme["class_coverage"]["B"]["train"], 0)
        self.assertGreater(scheme["class_coverage"]["B"]["validation"], 0)

    def test_isolation_and_identity_counting(self):
        did = self._setup()
        scheme = self.sp.propose_split(
            did, time_bound=1500, target_ratio=0.5, labels=self._labels())
        check = self.sp.validate_scheme(scheme["scheme_id"])
        self.assertTrue(check["isolated"])
        self.assertEqual(check["cross_side_events"], [])
        self.assertEqual(check["cross_side_subjects"], [])
        self.assertEqual(check["cross_side_seals"], [])
        self.assertEqual(check["future_data"], [])
        # 同一事件被 pay/risk 两特征、被 s1/s1b 两样本引用，只计一次：
        # u1:p1,p2 + u2:p1,r1 + u3:p9 = 5 个独立来源事件。
        self.assertEqual(check["distinct_source_events"], 5)

    def test_cross_subject_relation_forces_same_side(self):
        did = self._setup()
        scheme = self.sp.propose_split(
            did, time_bound=1500, target_ratio=0.5, labels=self._labels(),
            relations=[["s1", "s3"]])
        sides = {sid: ("train" if sid in scheme["train"] else "validation")
                 for sid in scheme["train"] + scheme["validation"]}
        self.assertEqual(sides["s1"], sides["s3"])
        self.assertEqual(sides["s1"], sides["s1b"])
        self.assertTrue(self.sp.validate_scheme(scheme["scheme_id"])["isolated"])

    def test_relation_conflict_and_insufficient_samples(self):
        did = self._setup()
        # 关系引用时间界限外的样本 -> 无法满足的范围。
        scheme = self.sp.propose_split(
            did, time_bound=1500, target_ratio=0.5, labels=self._labels(),
            relations=[["s2", "s4"]])
        conflicts = [u for u in scheme["unsatisfiable"]
                     if u["type"] == "relation_conflict"]
        self.assertEqual(conflicts[0]["missing"], ["s4"])
        # 时间界限内历史可见样本不足。
        scheme2 = self.sp.propose_split(
            did, time_bound=50, target_ratio=0.5, labels=self._labels())
        types = {u["type"] for u in scheme2["unsatisfiable"]}
        self.assertIn("insufficient_samples", types)
        self.assertEqual(scheme2["train"], [])
        self.assertEqual(scheme2["validation"], [])

    def test_compare_and_adopt(self):
        did = self._setup()
        s1 = self.sp.propose_split(did, time_bound=1500, target_ratio=0.5,
                                   labels=self._labels(), name="half")
        s2 = self.sp.propose_split(did, time_bound=1500, target_ratio=0.8,
                                   labels=self._labels(), name="eighty")
        compared = self.sp.compare_schemes([s1["scheme_id"], s2["scheme_id"]])
        self.assertEqual(len(compared["schemes"]), 2)
        self.assertLessEqual(compared["schemes"][0]["ratio_gap"], 0.3)
        adopted = self.sp.adopt_scheme(s1["scheme_id"])
        self.assertEqual(adopted["status"], "adopted")
        self.assertEqual(self.sp.get_scheme(s2["scheme_id"])["status"],
                         "proposed")
        # 重新采用另一份：显式切换。
        self.sp.adopt_scheme(s2["scheme_id"])
        self.assertEqual(self.sp.get_scheme(s1["scheme_id"])["status"],
                         "proposed")

    def test_late_data_only_forms_new_scheme(self):
        did = self._setup()
        s1 = self.sp.propose_split(did, time_bound=1500, target_ratio=0.5,
                                   labels=self._labels())
        # 迟到资料补登（对 u3 当时可见的迟到事件）。
        self.svc.ingest("p8", "u3", source="pay", event_time=50,
                        arrival_time=1600, payload={"amount": 1})
        self.svc.materialize("s3", "pay")
        # 旧方案原样保留。
        again = self.sp.get_scheme(s1["scheme_id"])
        self.assertEqual(again["train"], s1["train"])
        self.assertEqual(again["validation"], s1["validation"])
        # 迟到资料只能形成新方案。
        s2 = self.sp.propose_split(did, time_bound=1500, target_ratio=0.5,
                                   labels=self._labels())
        self.assertNotEqual(s1["scheme_id"], s2["scheme_id"])
        self.assertEqual(len(self.sp.list_schemes(did)), 2)

    def test_requires_frozen_dataset(self):
        did = self._setup()
        draft = self.ds.create_dataset("draft", created_at=4000)
        with self.assertRaises(Exception):
            self.sp.propose_split(draft, time_bound=1500, target_ratio=0.5)
        with self.assertRaises(ValueError):
            self.sp.propose_split(did, time_bound=1500, target_ratio=1.5)


if __name__ == "__main__":
    unittest.main()

