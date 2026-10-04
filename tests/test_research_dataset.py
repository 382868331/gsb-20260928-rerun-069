"""固定研究数据集：交叠封存、迟到资料、不同观察时刻、部分缺失、
冻结重开一致性、身份计数、修订隔离与未采用差异保留。
"""

import os
import tempfile
import unittest

from feature_service import FeatureService, ResearchDatasetService


class ResearchDatasetTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "d.db")
        self.svc = FeatureService(self.db)
        self.ds = ResearchDatasetService(self.svc)

    def tearDown(self):
        self.svc.close()
        self.tmp.cleanup()

    def _setup(self):
        # 两个行为来源共享同一业务事件 id（不同来源合法复用）。
        self.svc.register_feature(
            "pay", entity="user", windows=[1000],
            aggregations=["count", "sum"], value_field="amount",
            sources=("pay",), created_at=0,
        )
        self.svc.register_feature(
            "risk", entity="user", windows=[1000],
            aggregations=["count"], value_field="amount",
            sources=("pay", "risk"), created_at=0,
        )
        # u1：as_of=1000，两条支付，其中一条迟到修订。
        self.svc.ingest("p1", "u1", source="pay", event_time=100, arrival_time=100,
                        payload={"amount": 10})
        self.svc.ingest("p2", "u1", source="pay", event_time=200, arrival_time=200,
                        payload={"amount": 20})
        self.svc.register_sample("s1", "user", "u1", as_of=1000)
        self.svc.materialize("s1", "pay")
        self.svc.materialize("s1", "risk")
        # u2：as_of=500 的另一观察时刻（不同样本不同 as_of）。
        self.svc.ingest("p1", "u2", source="pay", event_time=100, arrival_time=100,
                        payload={"amount": 5})
        self.svc.ingest("r1", "u2", source="risk", event_time=150, arrival_time=150,
                        payload={"amount": 9})
        self.svc.register_sample("s2", "user", "u2", as_of=500)
        self.svc.materialize("s2", "pay")
        self.svc.materialize("s2", "risk")
        # u3：只物化 pay，risk 完全没有证据。
        self.svc.ingest("p9", "u3", source="pay", event_time=100, arrival_time=100,
                        payload={"amount": 7})
        self.svc.register_sample("s3", "user", "u3", as_of=1000)
        self.svc.materialize("s3", "pay")

    def test_preview_completeness_reasons(self):
        self._setup()
        # 先把 pay 全量封存（两批交叠），risk 全部留开放。
        self.svc.seal("s1", "pay", 1, feature_keys=["pay_w1000_sum"], sealed_at=1100)
        overlap = self.svc.seal("s1", "pay", 1,
                                feature_keys=["pay_w1000_sum", "pay_w1000_count"],
                                sealed_at=1200)
        self.assertEqual(overlap["confirmed"], ["pay_w1000_count"])
        self.assertEqual(overlap["already_sealed"], ["pay_w1000_sum"])
        for key in ("risk_w1000_count",):
            pass
        self.svc.seal("s2", "pay", 1, sealed_at=600)

        did = self.ds.create_dataset("d1", created_at=1300)
        self.ds.add_feature_view(did, "pay", 1)
        self.ds.add_feature_view(did, "risk", 1)
        self.ds.add_samples(did, ["s1", "s2", "s3"])
        preview = self.ds.preview_dataset(did)

        by_sample = {p["sample_id"]: p for p in preview["samples"]}
        # s2：pay 封存；risk 开放但有证据 -> unconfirmed。
        self.assertFalse(by_sample["s2"]["complete"])
        self.assertIn("unconfirmed", by_sample["s2"]["reasons"])
        # s3：pay 开放(unconfirmed) + risk 从未物化(no_evidence)。
        self.assertEqual(set(by_sample["s3"]["reasons"]), {"unconfirmed", "no_evidence"})
        self.assertEqual(preview["complete"], [])

        # 封存补齐 s1、s2 的 risk；s3 的 risk 仍无证据。
        self.svc.seal("s1", "risk", 1, sealed_at=1300)
        self.svc.seal("s2", "risk", 1, sealed_at=600)
        self.svc.seal("s3", "pay", 1, sealed_at=1100)
        preview = self.ds.preview_dataset(did)
        by_sample = {p["sample_id"]: p for p in preview["samples"]}
        self.assertTrue(by_sample["s1"]["complete"])
        self.assertTrue(by_sample["s2"]["complete"])
        self.assertFalse(by_sample["s3"]["complete"])
        self.assertEqual(by_sample["s3"]["reasons"], ["no_evidence"])
        self.assertEqual(set(preview["complete"]), {"s1", "s2"})

    def test_freeze_lineage_identity_counts_and_future_guard(self):
        self._setup()
        # 迟到修订 p1=10 -> 50，在 s1 as_of=1000 之后才到达。
        self.svc.ingest("p1", "u1", source="pay", event_time=100, arrival_time=5000,
                        payload={"amount": 50})
        # 同时补登一条“当时可见但晚登记”的撤回（arrival=900 <= 1000）。
        self.svc.retract("p2", "u1", source="pay", event_time=200, arrival_time=900)
        for sid in ("s1", "s2"):
            self.svc.seal(sid, "pay", 1, sealed_at=1300 if sid == "s1" else 600)
            self.svc.seal(sid, "risk", 1, sealed_at=1300 if sid == "s1" else 600)
        # s3 的 pay 也封存，但 risk 从未物化、根本没有证据。
        self.svc.seal("s3", "pay", 1, sealed_at=1100)

        did = self.ds.create_dataset("d1", created_at=1400)
        self.ds.add_feature_view(did, "pay", 1)
        self.ds.add_feature_view(did, "risk", 1)
        self.ds.add_samples(did, ["s1", "s2", "s3"])
        self.ds.freeze_dataset(did, frozen_at=1400)

        # 只纳入完整样本 s1/s2；s3 因 risk 无证据被排除。
        self.assertEqual(set(self.ds.included_samples(did)), {"s1", "s2"})
        excluded = {e["sample_id"]: e["reason"] for e in self.ds.excluded_samples(did)}
        self.assertEqual(excluded["s3"], "no_evidence")

        # s1 冻结值：p2 已撤回（当时可见），p1 取封存值 10（迟到修订不穿越）。
        row1 = self.ds.read_row(did, "s1")
        self.assertEqual(row1["pay:1:pay_w1000_sum"], 10)
        self.assertEqual(row1["pay:1:pay_w1000_count"], 1)
        # risk 汇总 pay+risk 来源：u1 无 risk 来源事件，只含 pay 中未撤回者。
        self.assertEqual(row1["risk:1:risk_w1000_count"], 1)

        # 血缘逐格记录来源批次与证据 seq。
        lin = self.ds.lineage(did, "s1")
        sum_cell = lin["pay:1:pay_w1000_sum"]
        self.assertEqual(sum_cell["source_kind"], "sealed")
        self.assertIsNotNone(sum_cell["seal_id"])
        self.assertTrue(sum_cell["records"]) 
        for rec in sum_cell["records"]:
            self.assertLessEqual(rec["arrival_time"], 1000)  # 不使用未来资料

        # 身份计数：两个样本；来源事件按 (entity, source, event_id) 去重，
        # 同一 pay 事件同时进入 pay 与 risk 两个特征不多计。
        cov = self.ds.coverage(did)
        self.assertEqual(cov["sample_count"], 2)
        self.assertEqual(cov["sample_ids"], ["s1", "s2"])
        identities = set(cov["source_events"])
        self.assertEqual(cov["source_event_count"], len(identities))
        self.assertIn(("u1", "pay", "p1"), identities)
        self.assertIn(("u2", "pay", "p1"), identities)  # 不同实体，分别计
        self.assertIn(("u2", "risk", "r1"), identities)

        # 冻结后再来早期资料/修订不改变旧数据集。
        self.svc.ingest("p1", "u1", source="pay", event_time=100, arrival_time=900,
                        payload={"amount": 11})
        self.svc.materialize("s1", "pay")
        self.assertEqual(self.ds.read_row(did, "s1")["pay:1:pay_w1000_sum"], 10)

        # 重开后逐项读取与冻结时一致。
        self.svc.close()
        svc2 = FeatureService(self.db)
        ds2 = ResearchDatasetService(svc2)
        self.assertEqual(ds2.read_row(did, "s1"), row1)
        self.assertEqual(ds2.coverage(did), cov)
        svc2.close()

    def test_revision_isolated_and_pending_retained(self):
        self._setup()
        for sid in ("s1", "s2"):
            self.svc.seal(sid, "pay", 1, sealed_at=1100)
            self.svc.seal(sid, "risk", 1, sealed_at=1100)
        did = self.ds.create_dataset("d1", created_at=1200)
        self.ds.add_feature_view(did, "pay", 1)
        self.ds.add_feature_view(did, "risk", 1)
        self.ds.add_samples(did, ["s1", "s2"])
        self.ds.freeze_dataset(did, frozen_at=1200)
        before = self.ds.read_row(did, "s1")["pay:1:pay_w1000_sum"]

        # 针对历史特征提出并发布修订（新到达的当时可见资料）。
        self.svc.ingest("p1", "u1", source="pay", event_time=100, arrival_time=950,
                        payload={"amount": 12})
        rev = self.svc.propose_revision("s1", "pay", 1, note="late ledger fix")
        self.svc.publish_revision(rev["revision_id"], published_at=1300)

        # 旧数据集不自动采用修订。
        self.assertEqual(self.ds.read_row(did, "s1")["pay:1:pay_w1000_sum"], before)

        proposal = self.ds.propose_update(did)
        rev_diffs = proposal["by_category"]["revision"]
        self.assertTrue(any(d["feature_key"] == "pay_w1000_sum" for d in rev_diffs))
        s1_sum = next(d for d in rev_diffs if d["feature_key"] == "pay_w1000_sum")
        self.assertEqual(s1_sum["old_value"], before)
        self.assertEqual(s1_sum["new_value"], 12 + 20)

        # 只采用 s1 的 sum 修订，其它差异保留为 pending。
        key = ["s1", "pay", 1, "pay_w1000_sum"]
        new_id = self.ds.adopt_update(did, [key], name="d2", created_at=1400,
                                      frozen_at=1400)
        self.assertEqual(
            self.ds.read_row(new_id, "s1")["pay:1:pay_w1000_sum"], 12 + 20)
        lin = self.ds.lineage(new_id, "s1")["pay:1:pay_w1000_sum"]
        self.assertEqual(lin["source_kind"], "revision")
        self.assertEqual(lin["revision_id"], rev["revision_id"])
        # 旧数据集原样。
        self.assertEqual(self.ds.read_row(did, "s1")["pay:1:pay_w1000_sum"], before)
        # 未采用的 count 修订差异保留在新数据集上。
        pending = {(d["sample_id"], d["feature_key"]) for d in self.ds.pending_diffs(new_id)}
        self.assertIn(("s1", "pay_w1000_count"), pending)

    def test_time_limit_then_new_sample_scope(self):
        self._setup()
        # 新特征版本在 s1 观察时刻之后才创建。
        v2 = self.svc.register_feature(
            "pay", entity="user", windows=[1000, 2000],
            aggregations=["count"], value_field="amount",
            sources=("pay",), created_at=2000)
        self.svc.register_sample("s4", "user", "u1", as_of=3000)
        self.svc.materialize("s4", "pay", version=2)
        self.svc.seal("s4", "pay", 2, sealed_at=3100)
        did = self.ds.create_dataset("d1", created_at=3200)
        self.ds.add_feature_view(did, "pay", 2)
        self.ds.add_samples(did, ["s1", "s4"])
        preview = self.ds.preview_dataset(did)
        by_sample = {p["sample_id"]: p for p in preview["samples"]}
        self.assertFalse(by_sample["s1"]["complete"])
        self.assertEqual(by_sample["s1"]["reasons"], ["time_limit"])
        self.assertTrue(by_sample["s4"]["complete"])


if __name__ == "__main__":
    unittest.main()
