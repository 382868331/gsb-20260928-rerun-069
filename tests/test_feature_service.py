"""特征服务校验：跨窗口、迟到不可穿越、重复/撤回抵消、重启后增量=整批。"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from feature_service import FeatureService, register_aggregation
from feature_service.aggregations import Aggregation


class _StdDev(Aggregation):
    """总体标准差，用于演示“新增计算类型仍受 as_of 约束”。"""

    name = "stddev"
    empty = None

    def compute(self, values):
        mean = sum(values) / len(values)
        return (sum((v - mean) ** 2 for v in values) / len(values)) ** 0.5


class FeatureServiceTest(unittest.TestCase):
    def setUp(self):
        register_aggregation(_StdDev())
        self.tmp = tempfile.TemporaryDirectory()
        self.db = str(Path(self.tmp.name) / "features.db")
        self.svc = FeatureService(self.db)
        self.svc.register_feature(
            "pay", entity="user", windows=[100, 1000],
            aggregations=["count", "sum", "mean"], value_field="amount",
            created_at=0,
        )

    def tearDown(self):
        self.svc.close()
        self.tmp.cleanup()

    def feed_base_events(self):
        s = self.svc
        # event_time 与 arrival_time 相同：按时到达
        s.ingest("e1", "u1", event_time=100, arrival_time=100, payload={"amount": 10})
        s.ingest("e2", "u1", event_time=150, arrival_time=160, payload={"amount": 20})
        s.ingest("e3", "u1", event_time=500, arrival_time=510, payload={"amount": 30})
        # e4 发生在 900，但直到 9000 才到达：对 1000 附近的历史样本不可见
        s.ingest("e4", "u1", event_time=900, arrival_time=9000, payload={"amount": 40})

    def test_cross_window_aggregation(self):
        self.feed_base_events()
        spec = self.svc._get_spec("pay")
        # as_of=200：w100 窗口 [100,200) 含 e1,e2；w1000 窗口 [-800,200) 同样
        got = self.svc.compute_batch("u1", spec, 200)
        self.assertEqual(got["pay_w100_count"], 2)
        self.assertEqual(got["pay_w100_sum"], 30)
        self.assertEqual(got["pay_w100_mean"], 15)
        self.assertEqual(got["pay_w1000_count"], 2)
        # event_time 恰在左边界计入、右边界（=as_of）不计入
        self.assertEqual(self.svc.compute_batch("u1", spec, 150)["pay_w100_count"], 1)
        self.assertEqual(self.svc.compute_batch("u1", spec, 250)["pay_w1000_count"], 2)
        # 510 之后 e3 到达
        self.assertEqual(self.svc.compute_batch("u1", spec, 600)["pay_w1000_count"], 3)
        self.assertEqual(self.svc.compute_batch("u1", spec, 505)["pay_w1000_count"], 2)

    def test_late_arrival_does_not_travel_back(self):
        self.feed_base_events()
        s = self.svc
        s.register_sample("old", "user", "u1", 1000)
        # 在 e4 到达前后，as_of=1000 看到的内容必须一致
        v_before = s.materialize("old", "pay", batch=True)
        self.assertEqual(v_before["pay_w1000_count"], 3)
        self.assertEqual(v_before["pay_w1000_sum"], 60)
        # 即使未来重新打开服务、整批重放，已冻结的训练快照不变
        v_replay = s.materialize("old", "pay")
        self.assertEqual(v_replay, v_before)
        # 而晚些时候、窗口够大的新样本可以看到 e4
        s.register_feature(
            "pay_big", entity="user", windows=[10000],
            aggregations=["count", "sum"], value_field="amount", created_at=1,
        )
        s.register_sample("new", "user", "u1", 9100)
        v_new = s.materialize("new", "pay_big", batch=True)
        self.assertEqual(v_new["pay_big_w10000_count"], 4)
        self.assertEqual(v_new["pay_big_w10000_sum"], 100)

    def test_duplicate_revise_retract(self):
        s = self.svc
        spec = s._get_spec("pay")
        s.ingest("e1", "u1", event_time=100, arrival_time=100, payload={"amount": 10})
        # 完全重复投递：幂等忽略
        self.assertFalse(
            s.ingest("e1", "u1", event_time=100, arrival_time=100, payload={"amount": 10})
        )
        self.assertEqual(s.compute_batch("u1", spec, 200)["pay_w100_count"], 1)

        # 迟到修订：180 到达前 as_of=170 仍看到旧值 10
        s.ingest("e1", "u1", event_time=100, arrival_time=180, payload={"amount": 15},
                 op="UPSERT")
        self.assertEqual(s.compute_batch("u1", spec, 170)["pay_w100_sum"], 10)
        self.assertEqual(s.compute_batch("u1", spec, 200)["pay_w100_sum"], 15)

        # 撤回：190 到达后 e1 被抵消；185 时点撤回尚不可见
        s.retract("e1", "u1", event_time=100, arrival_time=190)
        self.assertEqual(s.compute_batch("u1", spec, 185)["pay_w100_count"], 1)
        self.assertEqual(s.compute_batch("u1", spec, 200)["pay_w100_count"], 0)
        self.assertEqual(s.compute_batch("u1", spec, 200)["pay_w100_sum"], 0)

        # 撤回一个从未见过的事件应安全无效，不产生任何计数
        s.retract("ghost", "u1", event_time=120, arrival_time=195)
        self.assertEqual(s.compute_batch("u1", spec, 200)["pay_w100_count"], 0)

    def test_incremental_equals_batch_after_restart(self):
        self.feed_base_events()
        s = self.svc
        checkpoints = [170, 200, 600, 950, 9050, 10000]
        spec = s._get_spec("pay")

        # 增量路径在运行中持续 catch_up
        for as_of in checkpoints:
            inc = s.compute_incremental("u1", spec, as_of)
            bat = s.compute_batch("u1", spec, as_of)
            self.assertEqual(inc, bat, f"as_of={as_of}")

        # 模拟重启：新进程打开同一个数据库，投影与 checkpoint 已持久化
        s.close()
        s2 = FeatureService(self.db)
        spec2 = s2._get_spec("pay")
        # 重启后只新增两条：一条修订、一条撤回
        s2.ingest("e2", "u1", event_time=150, arrival_time=9100, payload={"amount": 25})
        s2.retract("e3", "u1", event_time=500, arrival_time=9200)
        for as_of in [*checkpoints, 9300]:
            inc = s2.compute_incremental("u1", spec2, as_of)
            bat = s2.compute_batch("u1", spec2, as_of)
            self.assertEqual(inc, bat, f"重启后 as_of={as_of}")

        # 9300 时点：e1=10, e2 修订为25, e3 撤回, e4=40；w10000 视角
        big = s2._get_spec("pay_big") if any(
            n == "pay_big" for n, _ in s2.list_features()
        ) else None
        if big is None:
            ver = s2.register_feature(
                "pay_big", entity="user", windows=[10000],
                aggregations=["count", "sum"], value_field="amount", created_at=2,
            )
            big = s2._get_spec("pay_big", ver)
        got = s2.compute_batch("u1", big, 9300)
        self.assertEqual(got["pay_big_w10000_count"], 3)
        self.assertEqual(got["pay_big_w10000_sum"], 75)

        # 丢掉缓存整批重放，仍与账本直读一致
        s2.rebuild_projection()
        for as_of in [200, 9300]:
            self.assertEqual(
                s2.compute_incremental("u1", spec2, as_of),
                s2.compute_batch("u1", spec2, as_of),
            )
        s2.close()

    def test_versions_are_immutable_and_appended(self):
        self.feed_base_events()
        s = self.svc
        self.assertEqual(
            s.register_feature(
                "pay", entity="user", windows=[100, 1000],
                aggregations=["count", "sum", "mean"], value_field="amount",
                created_at=5,
            ),
            1,
        )
        v2 = s.register_feature(
            "pay", entity="user", windows=[500, 2000],
            aggregations=["count", "sum"], value_field="amount", created_at=6,
        )
        self.assertEqual(v2, 2)
        s.register_sample("old", "user", "u1", 600)
        v1_values = s.materialize("old", "pay", version=1)
        v2_values = s.materialize("old", "pay", version=2)
        self.assertEqual(v1_values["pay_w1000_count"], 3)
        # w500 在 as_of=600 时窗口为 [100,600)，e1/e2/e3 都在窗内
        self.assertEqual(v2_values["pay_w500_count"], 3)
        self.assertEqual(v2_values["pay_w2000_sum"], 60)
        # 新版本发布后，v1 快照原样可读
        self.assertEqual(s.get_features("old", "pay", 1), v1_values)
        with self.assertRaises(KeyError):
            s._get_spec("pay", 9)

    def test_new_aggregation_respects_as_of(self):
        self.feed_base_events()
        s = self.svc
        s.register_feature(
            "pay_d", entity="user", windows=[10000],
            aggregations=["stddev"], value_field="amount", created_at=3,
        )
        spec = s._get_spec("pay_d")
        # as_of=600：只有 10/20/30；e4 9000 才到，不能参与
        early = s.compute_batch("u1", spec, 600)["pay_d_w10000_stddev"]
        self.assertAlmostEqual(early, (200 / 3) ** 0.5, places=9)
        late = s.compute_batch("u1", spec, 9100)["pay_d_w10000_stddev"]
        self.assertNotAlmostEqual(early, late)

    def test_empty_result_evidence_is_explicit(self):
        self.svc.register_sample("empty", "user", "u1", 100)
        self.svc.materialize("empty", "pay")
        evidence = self.svc.get_evidence("empty", "pay", 1, include_records=False)
        self.assertEqual(evidence["pay_w100_count"], {"behavior": [], "state": []})
        self.assertEqual(evidence["pay_w100_sum"], {"behavior": [], "state": []})

    def test_multi_source_time_join_boundaries_and_lineage(self):
        s = self.svc
        version = s.register_feature(
            "profile", entity="user", windows=[100],
            aggregations=["count", "sum"], value_field="amount",
            sources=("click",), state_source="status", state_field="tier",
            created_at=10,
        )
        spec = s._get_spec("profile", version)

        # 两条状态 event_time/arrival_time 完全相同：同一来源同键冲突按账本顺序
        # 取最后一条；事件时间恰为 as_of 的状态可用。
        s.ingest("s1", "u1", source="status", event_time=100, arrival_time=100,
                 payload={"tier": "A"})
        s.ingest("s2", "u1", source="status", event_time=100, arrival_time=100,
                 payload={"tier": "B"})
        s.ingest("click-1", "u1", source="click", event_time=50, arrival_time=50,
                 payload={"amount": 5})
        # event_time=as_of 的行为落在右边界，左闭右开，不计入。
        s.ingest("click-2", "u1", source="click", event_time=100, arrival_time=100,
                 payload={"amount": 7})
        # 不同来源复用相同业务 ID 不互相抵消；purchase 也不在 profile.sources 中。
        s.ingest("click-1", "u1", source="purchase", event_time=50, arrival_time=50,
                 payload={"amount": 999})
        # 状态事件时间更早，但到达晚于观测点，不能参与 as_of=100。
        s.ingest("s0", "u1", source="status", event_time=90, arrival_time=101,
                 payload={"tier": "C"})

        expected = {
            "profile_w100_count": 1,
            "profile_w100_sum": 5,
            "profile_last_state": "B",
        }
        inc = s.explain_incremental("u1", spec, 100)
        bat = s.explain_batch("u1", spec, 100)
        self.assertEqual(inc["values"], expected)
        self.assertEqual(bat["values"], expected)
        self.assertEqual(inc["evidence"], bat["evidence"])

        s.register_sample("old", "user", "u1", 100)
        frozen = s.materialize("old", "profile", version)
        self.assertEqual(frozen, expected)
        before_evidence = s.get_evidence("old", "profile", version, include_records=False)
        self.assertEqual(
            before_evidence["profile_w100_sum"]["behavior"],
            [inc["evidence"]["profile_w100_sum"]["behavior"][0]],
        )
        self.assertEqual(
            before_evidence["profile_last_state"]["state"],
            [inc["evidence"]["profile_last_state"]["state"][0]],
        )

        # 来源更正/撤回在 101 后可见：新样本重算为右边界行为和更早但后到的状态。
        self.assertTrue(
            s.retract("click-1", "u1", source="click", event_time=50, arrival_time=101)
        )
        self.assertTrue(
            s.retract("s2", "u1", source="status", event_time=100, arrival_time=101)
        )
        self.assertTrue(
            s.retract("s1", "u1", source="status", event_time=100, arrival_time=101)
        )
        s.register_sample("new", "user", "u1", 200)
        fresh = s.materialize("new", "profile", version, batch=True)
        self.assertEqual(fresh["profile_w100_count"], 1)
        self.assertEqual(fresh["profile_w100_sum"], 7)
        self.assertEqual(fresh["profile_last_state"], "C")

        # 已冻结样本的值与证据均不被影响范围重算改写；recompute 只返回对照。
        comparison = s.materialize("old", "profile", version, recompute=True)
        self.assertEqual(comparison["frozen"], expected)
        self.assertEqual(s.get_features("old", "profile", version), expected)
        self.assertEqual(
            s.get_evidence("old", "profile", version, include_records=False),
            before_evidence,
        )

        # 重启后仍能从冻结证据还原每个值来自哪些当时可见的原始记录。
        s.close()
        reopened = FeatureService(self.db)
        records = reopened.get_evidence("old", "profile", version)
        self.assertEqual(
            records["profile_w100_sum"]["behavior"][0]["source"], "click"
        )
        self.assertEqual(
            records["profile_w100_sum"]["behavior"][0]["payload"]["amount"], 5
        )
        self.assertEqual(
            records["profile_last_state"]["state"][0]["payload"]["tier"], "B"
        )
        reopened.close()

    def test_open_results_follow_visible_revisions(self):
        s = self.svc
        s.ingest("e1", "u1", event_time=100, arrival_time=100, payload={"amount": 10})
        s.register_sample("s1", "user", "u1", 1000)
        self.assertEqual(s.materialize("s1", "pay")["pay_w1000_sum"], 10)

        # 物化后才写入、但对 as_of=1000 可见的修订：开放结果随之更新
        s.ingest("e1", "u1", event_time=100, arrival_time=500, payload={"amount": 15})
        self.assertEqual(s.get_features("s1", "pay", 1)["pay_w1000_sum"], 15)
        self.assertEqual(s.materialize("s1", "pay")["pay_w1000_sum"], 15)

        # 未来才到达（arrival > as_of）的修订不混入当前结果
        s.ingest("e1", "u1", event_time=100, arrival_time=2000, payload={"amount": 99})
        self.assertEqual(s.get_features("s1", "pay", 1)["pay_w1000_sum"], 15)

    def test_preview_seal_shows_scope_window_and_reasons(self):
        s = self.svc
        s.ingest("e1", "u1", event_time=100, arrival_time=100, payload={"amount": 10})
        s.ingest("e2", "u1", event_time=150, arrival_time=160, payload={"amount": 20})
        s.register_sample("s1", "user", "u1", 1000)
        s.materialize("s1", "pay")

        # 同时发生：迟到资料、窗口边界修订、撤回（都对 as_of=1000 可见）
        s.ingest("e3", "u1", event_time=500, arrival_time=800, payload={"amount": 30})
        s.ingest("e2", "u1", event_time=950, arrival_time=950, payload={"amount": 20})
        s.retract("e1", "u1", event_time=100, arrival_time=900)

        preview = s.preview_seal("s1", "pay", 1)
        self.assertEqual(preview["sealed"], [])
        changes = {c["feature_key"]: c for c in preview["changes"]}
        self.assertEqual(
            set(changes),
            {"pay_w100_count", "pay_w100_sum", "pay_w100_mean",
             "pay_w1000_count", "pay_w1000_sum", "pay_w1000_mean"},
        )
        # 窗口与边界：w100 区间 [900,1000)，e2 修订 150->950 跨入
        w100 = changes["pay_w100_sum"]
        self.assertEqual(w100["window"], 100)
        self.assertEqual(w100["window_range"], [900, 1000])
        self.assertEqual(w100["stored_value"], 0)
        self.assertEqual(w100["current_value"], 20)
        kinds = {r["kind"] for r in w100["reasons"]}
        self.assertIn("revision", kinds)
        self.assertIn("boundary_shift", kinds)
        # w1000：迟到资料 + 撤回 + 修订
        w1000 = changes["pay_w1000_sum"]
        self.assertEqual(w1000["stored_value"], 30)
        self.assertEqual(w1000["current_value"], 50)
        kinds = {r["kind"] for r in w1000["reasons"]}
        self.assertEqual(kinds, {"late_arrival", "retract", "revision"})
        late = [r for r in w1000["reasons"] if r["kind"] == "late_arrival"][0]
        self.assertEqual(late["event_id"], "e3")
        # 均值不受影响（空值均值除外）：mean 也变了，检查 unchanged 只含没有变化的 key
        self.assertNotIn("pay_w1000_sum", preview["unchanged"])

        # 确认封存 w1000 范围后，预览不再把它列为开放变化
        sealed = s.seal("s1", "pay", 1,
                        feature_keys=["pay_w1000_count", "pay_w1000_sum", "pay_w1000_mean"])
        self.assertEqual(sealed["pay_w1000_sum"], 50)
        again = s.preview_seal("s1", "pay", 1)
        self.assertEqual(
            sorted(again["sealed"]),
            ["pay_w1000_count", "pay_w1000_mean", "pay_w1000_sum"],
        )
        self.assertEqual(
            {c["feature_key"] for c in again["changes"]},
            {"pay_w100_count", "pay_w100_mean", "pay_w100_sum"},
        )
        with self.assertRaises(KeyError):
            s.preview_seal("ghost", "pay", 1)

    def test_state_retract_preview_and_seal(self):
        s = self.svc
        version = s.register_feature(
            "profile", entity="user", windows=[100],
            aggregations=["count"], value_field="amount",
            sources=("click",), state_source="status", state_field="tier",
            created_at=20,
        )
        s.ingest("c1", "u1", source="click", event_time=50, arrival_time=50,
                 payload={"amount": 5})
        s.ingest("st1", "u1", source="status", event_time=90, arrival_time=90,
                 payload={"tier": "A"})
        s.register_sample("p1", "user", "u1", 100)
        self.assertEqual(s.materialize("p1", "profile")["profile_last_state"], "A")

        # 状态撤回在 as_of 之前到达：开放结果更新，预览给出 retract 原因
        s.retract("st1", "u1", source="status", event_time=90, arrival_time=95)
        preview = s.preview_seal("p1", "profile", version)
        changes = {c["feature_key"]: c for c in preview["changes"]}
        self.assertIn("profile_last_state", changes)
        state_change = changes["profile_last_state"]
        self.assertIsNone(state_change["window"])
        self.assertEqual(state_change["stored_value"], "A")
        self.assertIsNone(state_change["current_value"])
        self.assertEqual(state_change["reasons"][0]["kind"], "retract")
        self.assertEqual(state_change["reasons"][0]["source"], "status")

        s.seal("p1", "profile", version)
        # 封存后新的可见状态也不再改变历史值
        s.ingest("st2", "u1", source="status", event_time=92, arrival_time=96,
                 payload={"tier": "B"})
        self.assertIsNone(s.get_features("p1", "profile", version)["profile_last_state"])

    def test_seal_survives_restart_and_mixed_changes(self):
        s = self.svc
        s.ingest("e1", "u1", event_time=100, arrival_time=100, payload={"amount": 10})
        s.ingest("e2", "u1", event_time=150, arrival_time=160, payload={"amount": 20})
        s.register_sample("s1", "user", "u1", 1000)
        s.materialize("s1", "pay")
        evidence_before = s.get_evidence("s1", "pay", 1, include_records=False)
        s.seal("s1", "pay", 1,
               feature_keys=["pay_w1000_count", "pay_w1000_sum", "pay_w1000_mean"])

        # 重启接续：封存决定与历史值都在数据库里
        s.close()
        s2 = FeatureService(self.db)
        # 同时到达：迟到资料、撤回、窗口边界修订（可见），以及未来才到的记录
        s2.ingest("e3", "u1", event_time=950, arrival_time=950, payload={"amount": 40})
        s2.retract("e1", "u1", event_time=100, arrival_time=600)
        s2.ingest("e2", "u1", event_time=920, arrival_time=920, payload={"amount": 20})
        s2.ingest("e4", "u1", event_time=960, arrival_time=5000, payload={"amount": 99})

        got = s2.get_features("s1", "pay", 1)
        # 封存范围保持历史原值与历史证据
        self.assertEqual(got["pay_w1000_count"], 2)
        self.assertEqual(got["pay_w1000_sum"], 30)
        self.assertEqual(got["pay_w1000_mean"], 15)
        self.assertEqual(
            s2.get_evidence("s1", "pay", 1, include_records=False)["pay_w1000_sum"],
            evidence_before["pay_w1000_sum"],
        )
        # 开放范围只更新当时可见且仍开放的部分：e3+e2(920) 入窗，e4 未来才到不混入
        self.assertEqual(got["pay_w100_count"], 2)
        self.assertEqual(got["pay_w100_sum"], 60)
        # 重新物化不改写封存 key
        again = s2.materialize("s1", "pay")
        self.assertEqual(again["pay_w1000_sum"], 30)
        self.assertEqual(again["pay_w100_sum"], 60)
        s2.close()


if __name__ == "__main__":
    unittest.main()
