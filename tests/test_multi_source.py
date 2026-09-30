"""多来源时间关联：状态快照 + 行为窗口、乱序/冲突/边界、封存重算与血缘。"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from feature_service import FeatureService


class MultiSourceTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = str(Path(self.tmp.name) / "features.db")
        self.svc = FeatureService(self.db)
        self.svc.register_source("risk", "state", created_at=0)
        self.svc.register_source("click", "event", created_at=0)
        self.svc.register_feature(
            "ctx", entity="user", windows=[1000],
            aggregations=["count", "sum"], value_field="amount",
            event_sources=["click"], state_sources=["risk"], created_at=0,
        )

    def tearDown(self):
        self.svc.close()
        self.tmp.cleanup()

    def _spec(self):
        return self.svc._get_spec("ctx")

    def test_latest_state_and_behavior_window(self):
        s = self.svc
        # 风险状态：乱序到达（event_time 早的记录晚到）
        s.ingest("r2", "u1", source="risk", event_time=150, arrival_time=300,
                 payload={"amount": 2})
        s.ingest("r1", "u1", source="risk", event_time=100, arrival_time=400,
                 payload={"amount": 1})
        # 行为流
        s.ingest("c1", "u1", source="click", event_time=50, arrival_time=60,
                 payload={"amount": 7})
        s.ingest("c2", "u1", source="click", event_time=950, arrival_time=960,
                 payload={"amount": 3})
        # 观测点 1000：最近状态取 r2(150)=2（event_time 最大），行为窗 [0,1000)
        got = s.compute_batch("u1", self._spec(), 1000)
        self.assertEqual(got["ctx_w1000_count"], 2)
        self.assertEqual(got["ctx_w1000_sum"], 10)
        self.assertEqual(got["ctx_state_risk"], 2)
        # as_of=120：r1 尚不可见（400 才到），但 r2 的 event_time=150 还没发生
        self.assertIsNone(s.compute_batch("u1", self._spec(), 120)["ctx_state_risk"])
        # 迟到状态不能进入早期样本：as_of=350 时 r1 未到达，最近状态是 r2
        self.assertEqual(s.compute_batch("u1", self._spec(), 350)["ctx_state_risk"], 2)

    def test_boundary_messages_and_identity_conflict(self):
        s = self.svc
        spec = self._spec()
        # 边界时刻相同：event_time == as_of-window 计入，== as_of 不计入
        s.ingest("c_lo", "u1", source="click", event_time=0, arrival_time=0,
                 payload={"amount": 5})
        s.ingest("c_hi", "u1", source="click", event_time=1000, arrival_time=1000,
                 payload={"amount": 9})
        got = s.compute_batch("u1", spec, 1000)
        self.assertEqual(got["ctx_w1000_count"], 1)
        self.assertEqual(got["ctx_w1000_sum"], 5)

        # 重复身份冲突：同一 event_id 两条不同 UPSERT，后到更正覆盖，且受 as_of 约束
        s.ingest("dup", "u1", source="click", event_time=100, arrival_time=100,
                 payload={"amount": 4})
        s.ingest("dup", "u1", source="click", event_time=100, arrival_time=500,
                 payload={"amount": 6})
        self.assertEqual(s.compute_batch("u1", spec, 400)["ctx_w1000_sum"], 9)
        self.assertEqual(s.compute_batch("u1", spec, 1000)["ctx_w1000_sum"], 11)
        # 完全重复投递仍被幂等忽略
        self.assertFalse(
            s.ingest("dup", "u1", source="click", event_time=100, arrival_time=500,
                     payload={"amount": 6})
        )
        # 不同来源的相同 event_id 是不同身份，互不覆盖
        s.ingest("dup", "u1", source="default", event_time=100, arrival_time=100,
                 payload={"amount": 100})
        got = s.compute_batch("u1", spec, 1000)  # ctx 只看 click 来源
        self.assertEqual(got["ctx_w1000_sum"], 11)

    def test_state_same_event_time_tie_by_seq_and_retract(self):
        s = self.svc
        spec = self._spec()
        s.ingest("r", "u1", source="risk", event_time=100, arrival_time=100,
                 payload={"amount": 1})
        s.ingest("r", "u1", source="risk", event_time=100, arrival_time=200,
                 payload={"amount": 9})
        # 同 event_time 的更正：按 seq 后者胜
        self.assertEqual(s.compute_batch("u1", spec, 300)["ctx_state_risk"], 9)
        # 撤回状态：300 到达后无有效状态；as_of=250 仍看到 9
        s.retract("r", "u1", source="risk", event_time=100, arrival_time=300)
        self.assertEqual(s.compute_batch("u1", spec, 250)["ctx_state_risk"], 9)
        self.assertIsNone(s.compute_batch("u1", spec, 400)["ctx_state_risk"])

    def test_incremental_equals_batch_with_evidence(self):
        s = self.svc
        self.test_latest_state_and_behavior_window()
        for as_of in (120, 350, 900, 1000):
            s.projection.catch_up()
            from feature_service.engine import read_log_rows, compute_from_rows
            inc = compute_from_rows(self._spec(), as_of, s.projection.read_rows("u1"))
            bat = compute_from_rows(self._spec(), as_of, read_log_rows(s.conn, "u1"))
            self.assertEqual(
                {k: v.value for k, v in inc.items()},
                {k: v.value for k, v in bat.items()},
            )
            self.assertEqual(
                {k: v.evidence for k, v in inc.items()},
                {k: v.evidence for k, v in bat.items()},
                f"evidence 不一致 as_of={as_of}",
            )

    def test_sealed_keeps_evidence_unsealed_recomputes(self):
        s = self.svc
        s.ingest("r1", "u1", source="risk", event_time=100, arrival_time=100,
                 payload={"amount": 1})
        s.ingest("c1", "u1", source="click", event_time=100, arrival_time=100,
                 payload={"amount": 7})
        s.register_sample("live", "user", "u1", 1000)
        s.register_sample("frozen", "user", "u1", 1000)
        v_live = s.materialize("live", "ctx", sealed=False)
        v_frozen = s.materialize("frozen", "ctx", sealed=True)
        self.assertEqual(v_live["ctx_state_risk"], 1)

        # 来源更正与撤回：账本回填（arrival_time 仍 <= 样本 as_of，
        # 故重算时对该样本可见；而真正迟到的记录对早期样本永不可见）
        s.ingest("r1", "u1", source="risk", event_time=100, arrival_time=900,
                 payload={"amount": 4})
        s.retract("c1", "u1", source="click", event_time=100, arrival_time=950)

        # 未封存：按影响范围重算；已封存：保留原值原证据
        new_live = s.recompute_unsealed("live", "ctx", 1)
        self.assertEqual(new_live["ctx_state_risk"], 4)
        self.assertEqual(new_live["ctx_w1000_count"], 0)
        self.assertIsNone(s.recompute_unsealed("frozen", "ctx", 1))
        self.assertEqual(s.get_features("frozen", "ctx", 1), v_frozen)

        # 封存未封存结果后，后续更正不再影响它
        s.seal("live", "ctx", 1)
        s.ingest("r1", "u1", source="risk", event_time=100, arrival_time=980,
                 payload={"amount": 8})
        self.assertIsNone(s.recompute_unsealed("live", "ctx", 1))
        self.assertEqual(s.get_features("live", "ctx", 1)["ctx_state_risk"], 4)

    def test_explain_after_restart(self):
        s = self.svc
        s.ingest("c1", "u1", source="click", event_time=50, arrival_time=60,
                 payload={"amount": 7})
        s.register_sample("o", "user", "u1", 1000)
        s.materialize("o", "ctx")
        s.close()
        s2 = FeatureService(self.db)
        ex = s2.explain("o", "ctx_w1000_sum")
        self.assertEqual(ex["value"], 7)
        self.assertEqual(ex["evidence_seqs"], [1])
        self.assertEqual(ex["records"][0]["source"], "click")
        self.assertEqual(ex["records"][0]["event_id"], "c1")
        self.assertTrue(ex["sealed"])
        s2.close()

    def test_source_must_match_kind(self):
        with self.assertRaises(ValueError):
            self.svc.register_feature(
                "bad", entity="user", windows=[10], aggregations=["count"],
                event_sources=["risk"], created_at=1,
            )
        with self.assertRaises(KeyError):
            self.svc.ingest("x", "u1", source="unknown", event_time=1, arrival_time=1)


if __name__ == "__main__":
    unittest.main()
