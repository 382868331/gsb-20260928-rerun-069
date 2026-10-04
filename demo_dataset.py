"""固定研究数据集端到端演示。

逐样本按自身观察时刻组装多种（已封存/已采用修订）特征视角，
预览完整性 -> 冻结 -> 重开一致 -> 提出/采用更新（未采用差异保留）。
"""

import tempfile
from pathlib import Path

from feature_service import FeatureService, ResearchDatasetService


def main():
    tmp = tempfile.TemporaryDirectory()
    svc = FeatureService(str(Path(tmp.name) / "r.db"))
    ds = ResearchDatasetService(svc)

    svc.register_feature("pay", entity="user", windows=[1000],
                         aggregations=["count", "sum"], value_field="amount",
                         sources=("pay",), created_at=0)
    svc.ingest("p1", "u1", source="pay", event_time=100, arrival_time=100,
               payload={"amount": 10})
    svc.ingest("p2", "u1", source="pay", event_time=200, arrival_time=200,
               payload={"amount": 20})
    svc.register_sample("s1", "user", "u1", as_of=1000)
    svc.materialize("s1", "pay")
    svc.seal("s1", "pay", 1, sealed_at=1100)

    did = ds.create_dataset("study-2026Q1", created_at=1200)
    ds.add_feature_view(did, "pay", 1)
    ds.add_samples(did, ["s1"])
    preview = ds.preview_dataset(did)
    print("预览完整样本:", preview["complete"], "不完整:", preview["incomplete"])
    ds.freeze_dataset(did, frozen_at=1200)
    print("冻结行:", ds.read_row(did, "s1"))
    print("覆盖(身份计数):", ds.coverage(did)["sample_count"],
          ds.coverage(did)["source_event_count"])

    # 迟到的历史修订只形成更新建议，不自动进入旧数据集。
    svc.ingest("p1", "u1", source="pay", event_time=100, arrival_time=950,
               payload={"amount": 12})
    rev = svc.propose_revision("s1", "pay", 1)
    svc.publish_revision(rev["revision_id"], published_at=1300)
    proposal = ds.propose_update(did)
    print("更新建议分类:",
          {k: len(v) for k, v in proposal["by_category"].items()})
    print("旧数据集不变:", ds.read_row(did, "s1")["pay:1:pay_w1000_sum"])

    new_id = ds.adopt_update(
        did, [["s1", "pay", 1, "pay_w1000_sum"]],
        name="study-2026Q1-r1", created_at=1400, frozen_at=1400)
    print("采用后新数据集:", ds.read_row(new_id, "s1")["pay:1:pay_w1000_sum"])
    print("仍未采用的差异:", [(d["sample_id"], d["feature_key"]) for d in ds.pending_diffs(new_id)])
    lin = ds.lineage(new_id, "s1")["pay:1:pay_w1000_sum"]
    print("血缘来源:", lin["source_kind"], "revision_id =", lin["revision_id"])

    svc.close()
    tmp.cleanup()


if __name__ == "__main__":
    main()
