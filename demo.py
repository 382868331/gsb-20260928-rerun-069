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
    tmp.cleanup()


if __name__ == "__main__":
    main()
