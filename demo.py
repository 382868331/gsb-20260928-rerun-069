"""端到端演示：开放/封存两态 + 封存前预览 + 可见修订按范围更新 + 重启接续。

直接运行：``python demo.py``（在临时目录中创建数据库，结束即清理）。
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from feature_service import FeatureService


def main() -> None:
    tmp = tempfile.TemporaryDirectory()
    db = str(Path(tmp.name) / "demo.db")
    svc = FeatureService(db)

    # 1) 定义：用户支付金额，100ms / 1000ms 两个滚动窗口
    svc.register_feature(
        "pay", entity="user", windows=[100, 1000],
        aggregations=["count", "sum", "mean"], value_field="amount",
        created_at=0,
    )

    # 2) 原始事件：event_time=发生时间，arrival_time=系统知道的时间
    svc.ingest("e1", "u1", event_time=100, arrival_time=100, payload={"amount": 10})
    svc.ingest("e2", "u1", event_time=150, arrival_time=160, payload={"amount": 20})
    svc.ingest("e3", "u1", event_time=500, arrival_time=510, payload={"amount": 30})
    svc.ingest("e4", "u1", event_time=900, arrival_time=9000, payload={"amount": 40})

    # 3) 人工样本：as_of=1000 物化特征（此时 e4 还未到达），结果默认开放
    svc.register_sample("label_001", "user", "u1", as_of=1000)
    saved = svc.materialize("label_001", "pay")
    print("as_of=1000 物化(开放):", saved)
    assert saved["pay_w1000_count"] == 3
    assert saved["pay_w1000_sum"] == 60

    # 4) 完全重复投递被忽略
    svc.ingest("e1", "u1", event_time=100, arrival_time=100, payload={"amount": 10})
    # 迟到修订 e1=10 -> 15、撤回 e3，都在 9000+ 才到达，不混入 as_of=1000 的结果
    svc.ingest("e1", "u1", event_time=100, arrival_time=9100, payload={"amount": 15})
    svc.retract("e3", "u1", event_time=500, arrival_time=9200)

    still = svc.get_features("label_001", "pay", 1)
    print("未来才到达的修订/撤回后:", still)
    assert still == saved, "未来才到达的信息混入了当前结果！"

    # 4.5) 当时可见（arrival_time <= as_of）的撤回后来才补登到账本：
    # 封存前先预览受影响样本、窗口与原因
    svc.retract("e2", "u1", event_time=150, arrival_time=900)
    preview = svc.preview_seal("label_001", "pay", 1)
    affected = {e["feature_key"]: e for e in preview["entries"] if e["changed"]}
    print("封存前预览(受影响范围):")
    for key, entry in sorted(affected.items()):
        print(f"  {key}: {entry['stored']} -> {entry['current']} 原因={entry['reasons']}")
    assert affected["pay_w1000_count"]["current"] == 2
    assert "retract" in affected["pay_w1000_count"]["reasons"]

    # 只确认封存 pay_w1000_sum；其余范围保持开放，继续随可见修订更新
    svc.seal("label_001", "pay", 1, feature_keys=["pay_w1000_sum"], sealed_at=9500)
    svc.ingest("e1", "u1", event_time=100, arrival_time=950, payload={"amount": 12})
    updated = svc.materialize("label_001", "pay")
    print("可见修订后(封存+开放):", updated)
    assert updated["pay_w1000_sum"] == 40    # 封存：封存时点可见结果，原样保留
    assert updated["pay_w1000_count"] == 2   # 开放：e2 撤回已反映
    assert updated["pay_w1000_mean"] == 21   # 开放：e1 修订为 12 后 (12+30)/2

    # 5) 新样本（as_of=9300）按当时已知计算：e1=12, e2/e3 已撤回, e4=40
    svc.register_sample("label_002", "user", "u1", as_of=9300)
    svc.register_feature(
        "pay", entity="user", windows=[100, 1000, 10000],
        aggregations=["count", "sum", "mean"], value_field="amount",
        created_at=1,
    )  # 配置变化 -> v2；v1 的封存范围保持原样
    fresh = svc.materialize("label_002", "pay", version=2, batch=True)
    print("as_of=9300 新样本(v2):", fresh)
    assert fresh["pay_w10000_count"] == 2
    assert fresh["pay_w10000_sum"] == 52
    assert svc.get_features("label_001", "pay", 1) == updated

    # 5.5) 多来源时间关联：历史点击 + 观测前最近有效会员状态
    svc.register_feature(
        "profile", entity="user", windows=[10000],
        aggregations=["count", "sum"], value_field="amount",
        sources=("click",), state_source="membership", state_field="tier",
        created_at=2,
    )
    svc.ingest("c1", "u1", source="click", event_time=800, arrival_time=800,
               payload={"amount": 7})
    svc.ingest("m1", "u1", source="membership", event_time=700, arrival_time=700,
               payload={"tier": "silver"})
    # 事件时间更早但晚到：对 as_of=9300 不可见
    svc.ingest("m0", "u1", source="membership", event_time=600, arrival_time=9400,
               payload={"tier": "gold"})
    svc.register_sample("profile_old", "user", "u1", as_of=9300)
    joined = svc.materialize("profile_old", "profile", version=1)
    print("多来源 as_of=9300 物化:", joined)
    assert joined["profile_w10000_count"] == 1
    assert joined["profile_w10000_sum"] == 7
    assert joined["profile_last_state"] == "silver"

    # 6) 模拟重启：增量投影与整批重放必须一致
    svc.close()
    svc2 = FeatureService(db)
    spec = svc2._get_spec("pay", 2)
    for as_of in (170, 1000, 9050, 9300):
        inc = svc2.compute_incremental("u1", spec, as_of)
        bat = svc2.compute_batch("u1", spec, as_of)
        assert inc == bat, (as_of, inc, bat)
    svc2.rebuild_projection()  # 丢弃缓存整批重放
    assert svc2.compute_incremental("u1", spec, 9300) == svc2.compute_batch("u1", spec, 9300)
    print("重启后增量 == 整批，且缓存重建结果一致 ✓")

    # 7) 重启接续：封存决定与历史值保留，开放范围仍随可见修订更新
    assert svc2.seal_status("label_001", "pay", 1)["pay_w1000_sum"] == "sealed"
    assert svc2.get_features("label_001", "pay", 1)["pay_w1000_sum"] == 40
    print("重启后封存决定与历史值保留 ✓")

    # 8) 部分交叠封存：第二批与第一批范围交叠，交叠 key 保持首次归属
    svc2.seal("label_001", "pay", 1, feature_keys=["pay_w1000_count"], sealed_at=9600)
    overlap = svc2.seal(
        "label_001", "pay", 1,
        feature_keys=["pay_w1000_count", "pay_w1000_mean"], sealed_at=9700,
    )
    assert overlap["confirmed"] == ["pay_w1000_mean"]
    assert overlap["already_sealed"] == ["pay_w1000_count"]
    view = svc2.query_features("label_001", "pay", 1)
    batches = svc2.seal_batches("label_001", "pay", 1)
    by_id = {b["seal_id"]: b for b in batches}
    assert view["pay_w1000_count"]["seal_id"] != view["pay_w1000_mean"]["seal_id"]
    assert not view["pay_w100_count"]["seal_id"]  # 仍开放，可继续更新
    assert view["pay_w100_count"]["updatable"]
    print("部分交叠封存：每批归属清晰，交叠不重复计入 ✓")

    # 9) 修订流程：对已封存结果提出修订 -> 发布独立修订视角
    svc2.ingest("e1", "u1", event_time=100, arrival_time=980, payload={"amount": 99})
    rev = svc2.propose_revision("label_001", "pay", 1, note="e1 再次修订")
    entries = {e["feature_key"]: e for e in rev["entries"]}
    print("修订对照:", {k: (e["old_value"], e["new_value"]) for k, e in entries.items()})
    assert entries["pay_w1000_sum"]["old_value"] == 40
    assert entries["pay_w1000_sum"]["new_value"] == 99 + 30  # e1 修订为 99，e3 仍在窗内
    assert svc2.get_features("label_001", "pay", 1)["pay_w1000_sum"] == 40  # 未采纳不影响
    svc2.publish_revision(rev["revision_id"], published_at=9800)
    svc2.close()

    # 10) 保存重开：原封存、已采纳修订、仍开放范围各自血缘一致
    svc3 = FeatureService(db)
    assert svc3.query_features("label_001", "pay", 1) == view
    got_rev = svc3.get_revision(rev["revision_id"])
    assert got_rev["status"] == "published"
    assert {m["event_id"] for e in got_rev["entries"] for m in e["materials"]} >= {"e1"}
    assert svc3.get_features("label_001", "pay", 1)["pay_w1000_sum"] == 40
    print("重启后：原封存/已采纳修订/开放范围血缘各自一致 ✓")
    svc3.close()
    tmp.cleanup()


if __name__ == "__main__":
    main()
