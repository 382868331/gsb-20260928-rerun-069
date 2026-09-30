"""端到端演示：跨窗口 + 迟到修订不穿越 + 重复/撤回抵消 + 重启后增量=整批。

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

    # 3) 人工样本：as_of=1000 时冻结特征（此时 e4 还未到达）
    svc.register_sample("label_001", "user", "u1", as_of=1000)
    frozen = svc.materialize("label_001", "pay")
    print("as_of=1000 冻结:", frozen)
    assert frozen["pay_w1000_count"] == 3
    assert frozen["pay_w1000_sum"] == 60

    # 4) 完全重复投递被忽略
    svc.ingest("e1", "u1", event_time=100, arrival_time=100, payload={"amount": 10})
    # 迟到修订 e1=10 -> 15、撤回 e3，都发生在 9000+，不能改变旧样本
    svc.ingest("e1", "u1", event_time=100, arrival_time=9100, payload={"amount": 15})
    svc.retract("e3", "u1", event_time=500, arrival_time=9200)

    still = svc.get_features("label_001", "pay", 1)
    print("迟到修订/撤回后旧样本:", still)
    assert still == frozen, "旧训练样本被穿越！"

    # 5) 新样本（as_of=9300）按当时已知重算：e1=15, e2=20, e3已撤回, e4=40
    svc.register_sample("label_002", "user", "u1", as_of=9300)
    svc.register_feature(
        "pay", entity="user", windows=[100, 1000, 10000],
        aggregations=["count", "sum", "mean"], value_field="amount",
        created_at=1,
    )  # 配置变化 -> v2；v1 仍冻结
    fresh = svc.materialize("label_002", "pay", version=2, batch=True)
    print("as_of=9300 新样本(v2):", fresh)
    assert fresh["pay_w10000_count"] == 3
    assert fresh["pay_w10000_sum"] == 75
    assert svc.get_features("label_001", "pay", 1) == frozen

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
    svc2.close()

    # 7) 多来源时间关联：观测前同一实体“最近一次有效状态” + 一段行为历史
    svc = FeatureService(db)  # 再次模拟重启
    svc.register_source("risk", "state", created_at=10)
    svc.register_source("click", "event", created_at=10)
    v3 = svc.register_feature(
        "ctx", entity="user", windows=[1000],
        aggregations=["count", "sum"], value_field="amount",
        event_sources=["click"], state_sources=["risk"], created_at=10,
    )
    # 乱序状态：先到 150 的状态，100 的旧状态更晚才到达
    svc.ingest("r2", "u1", source="risk", event_time=150, arrival_time=300,
               payload={"amount": 2})
    svc.ingest("r1", "u1", source="risk", event_time=100, arrival_time=400,
               payload={"amount": 1})
    svc.ingest("k1", "u1", source="click", event_time=50, arrival_time=60,
               payload={"amount": 7})
    svc.register_sample("obs", "user", "u1", as_of=1000)
    obs = svc.materialize("obs", "ctx", version=v3, sealed=False)
    print("观测前关联（最近状态 + 行为窗口）:", obs)
    assert obs["ctx_state_risk"] == 2      # 最近有效状态 event_time=150
    assert obs["ctx_w1000_count"] == 1

    # 8) 未封存结果随来源更正重算；血缘说明每个值来自哪些记录
    svc.ingest("r2", "u1", source="risk", event_time=150, arrival_time=900,
               payload={"amount": 5})
    fixed = svc.recompute_unsealed("obs", "ctx", v3)
    print("来源更正后未封存结果重算:", fixed)
    assert fixed["ctx_state_risk"] == 5
    why = svc.explain("obs", "ctx_state_risk")
    print("特征值血缘:", why["evidence_seqs"], why["records"])
    svc.seal("obs", "ctx", v3)
    assert svc.recompute_unsealed("obs", "ctx", v3) is None
    assert svc.get_features("label_001", "pay", 1) == frozen
    print("封存后保留原证据，已冻结训练样本不变 ✓")
    tmp.cleanup()


if __name__ == "__main__":
    main()
