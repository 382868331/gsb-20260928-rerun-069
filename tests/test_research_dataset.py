from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from feature_service import FeatureService


class ResearchDatasetTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = str(Path(self.tmp.name) / "research.db")
        self.svc = FeatureService(self.db)
        self.svc.register_feature(
            "pay", entity="user", windows=[100],
            aggregations=["count", "sum", "mean"], value_field="amount",
            created_at=0,
        )
        self.svc.register_feature(
            "spend", entity="user", windows=[100],
            aggregations=["count", "sum"], value_field="amount",
            created_at=0,
        )
        self.svc.register_feature(
            "pay_copy", entity="user", windows=[100],
            aggregations=["count", "sum"], value_field="amount",
            created_at=0,
        )

    def tearDown(self):
        self.svc.close()
        self.tmp.cleanup()

    def test_preview_freeze_counts_lineage_and_restart_immutability(self):
        s = self.svc
        s.ingest("e1", "u1", event_time=50, arrival_time=50, payload={"amount": 10})
        # e2 的事件时间属于 s1 的窗口，但 150 才到达：as_of=100 不能使用。
        s.ingest("e2", "u1", event_time=90, arrival_time=150, payload={"amount": 20})

        s.register_sample("s1", "user", "u1", 100)
        s.register_sample("s2", "user", "u1", 150)
        s.register_sample("s3", "user", "u3", 200)
        s.materialize("s1", "pay")
        s.seal("s1", "pay", 1,
               feature_keys=["pay_w100_count", "pay_w100_sum"], sealed_at=300)
        s.materialize("s2", "pay")
        s.seal("s2", "pay", 1, sealed_at=300)
        s.materialize("s2", "spend")
        s.seal("s2", "spend", 1, sealed_at=300)
        s.materialize("s2", "pay_copy")
        s.seal("s2", "pay_copy", 1, sealed_at=300)

        selection = [
            {"sample_id": "s1", "feature_name": "pay", "feature_version": 1},
            {"sample_id": "s2", "feature_name": "pay", "feature_version": 1},
            {"sample_id": "s2", "feature_name": "spend", "feature_version": 1},
            {"sample_id": "s2", "feature_name": "pay_copy", "feature_version": 1},
            {"sample_id": "s3", "feature_name": "pay", "feature_version": 1},
        ]
        preview = s.preview_research_dataset(selection)
        self.assertEqual(preview["complete_samples"], ["s2"])
        self.assertEqual(preview["incomplete_samples"], ["s1", "s3"])

        by_item = {
            (i["sample_id"], i["feature_key"]): i for i in preview["items"]
        }
        self.assertEqual(
            by_item[("s1", "pay_w100_mean")]["evidence_state"],
            "time_restricted",
        )
        self.assertEqual(
            by_item[("s1", "pay_w100_mean")]["blocked_records"][0]["event_id"],
            "e2",
        )
        self.assertEqual(
            by_item[("s3", "pay_w100_count")]["evidence_state"], "no_evidence"
        )
        self.assertEqual(preview["sample_coverage"], 1)
        self.assertEqual(preview["source_event_count"], 2)

        with self.assertRaises(ValueError):
            s.freeze_research_dataset("study", selection, frozen_at=400)
        selection_without_missing = [selection[1], selection[2], selection[3]]
        selection_with_s1 = [selection[0], *selection_without_missing]
        frozen = s.freeze_research_dataset(
            "study", selection_without_missing, frozen_at=400
        )
        self.assertEqual(frozen["sample_coverage"], 1)
        self.assertEqual(frozen["source_event_count"], 2)
        self.assertNotIn("s1", {i["sample_id"] for i in frozen["items"]})

        # 冻结后补登的早期资料不能进入旧数据集，即使整行原本还有开放特征。
        s.ingest("e3", "u1", event_time=80, arrival_time=100, payload={"amount": 5})
        s.close()
        reopened = FeatureService(self.db)
        reread = reopened.get_research_dataset("study", 1)
        self.assertEqual(reread, frozen)
        old_pay = next(
            i for i in reread["items"]
            if i["sample_id"] == "s2" and i["feature_key"] == "pay_w100_count"
        )
        self.assertEqual(old_pay["value"], 2)

        # 新数据集才采用开放视角的迟到资料；e2 仍被 s1 的 as_of=100 挡住。
        reopened.materialize("s1", "pay")
        updated = reopened.update_research_dataset(
            "study", selection_with_s1,
            adopt_visible=True, include_unconfirmed=True, frozen_at=500,
        )
        self.assertEqual(updated["version"], 2)
        s1_mean = next(
            i for i in updated["items"]
            if i["sample_id"] == "s1" and i["feature_key"] == "pay_w100_mean"
        )
        self.assertEqual(s1_mean["value"], 7.5)
        self.assertEqual(s1_mean["evidence_state"], "time_restricted")
        self.assertEqual(s1_mean["blocked_records"][0]["event_id"], "e2")
        self.assertEqual(reopened.get_research_dataset("study", 1), frozen)

    def test_revision_only_affects_dataset_that_adopts_it(self):
        s = self.svc
        s.ingest("e1", "u1", event_time=50, arrival_time=50, payload={"amount": 10})
        s.register_sample("s1", "user", "u1", 100)
        s.materialize("s1", "pay")
        s.seal("s1", "pay", 1, sealed_at=200)
        selection = [
            {"sample_id": "s1", "feature_name": "pay", "feature_version": 1}
        ]
        first = s.freeze_research_dataset("study", selection, frozen_at=300)

        s.retract("e1", "u1", event_time=50, arrival_time=90)
        revision = s.propose_revision(
            "s1", "pay", 1, created_at=400, note="撤回 e1"
        )
        s.publish_revision(revision["revision_id"], published_at=410)

        unchanged = s.update_research_dataset("study", selection, frozen_at=500)
        count = next(i for i in unchanged["items"] if i["feature_key"] == "pay_w100_count")
        self.assertEqual(count["value"], 1)
        self.assertEqual(count["perspective"], "stored")
        self.assertTrue(unchanged["pending_differences"])

        adopted = s.update_research_dataset(
            "study", selection, adopt_revisions=True, frozen_at=600
        )
        count = next(i for i in adopted["items"] if i["feature_key"] == "pay_w100_count")
        self.assertEqual(count["value"], 0)
        self.assertEqual(count["perspective"], "published_revision")
        self.assertEqual(
            count["adopted_revision_id"], revision["revision_id"]
        )
        self.assertEqual(s.get_research_dataset("study", 1), first)


if __name__ == "__main__":
    unittest.main()
