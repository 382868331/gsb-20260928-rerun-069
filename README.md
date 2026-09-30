# 特征计算服务（多来源时间关联 · 滚动统计 · 双时间）

纯 Python 标准库实现（sqlite3），不依赖任何外部特征平台。支持在一次观测点
关联多个来源：汇总同一实体一段历史行为，并取观测前最近一次有效状态；所有
来源都严格保证“当时知道什么”。

## 数据模型（三层分离，各自保存）

| 层 | SQLite 表 | 说明 |
| --- | --- | --- |
| 特征定义 | feature_definitions / feature_versions | 定义只追加版本；已发布版本不可改，配置变化产生新版本 |
| 原始事件 | event_log | append-only 账本，UPSERT/RETRACT 都是新记录，历史永不删除 |
| 特征版本 | sample_features | 样本按 (特征, 版本) 物化的值：开放结果随可见修订更新，封存结果历史冻结 |
| 特征证据 | sample_feature_evidence | 每个封存值使用的 behavior/state 账本 seq |

运行态缓存 projection_entries / checkpoint 可随时删除并由账本整批重放重建。

## 核心语义

- **双时间**：event_time 决定落在哪个窗口，arrival_time 决定查询时点是否已知。
- **不可穿越**：查询 as_of 只能使用 arrival_time <= as_of 的记录；未来才到达的信息不会混入当前结果，已封存的历史快照永久固定。
- **滚动窗口**：左闭右开 [as_of - window, as_of)。
- **多来源关联**：行为记录按 `sources` 过滤后进入滚动窗口；`state_source` 中取 `event_time <= as_of` 且 `arrival_time <= as_of` 的最近有效状态。
- **最近状态**：先按事件时间选最近；事件时间相同再按到达时间，仍相同按账本 seq（同批边界消息的确定性顺序）选择。
- **同一业务事件**以 (entity, source, event_id) 归约：不同来源可安全复用同一 ID；同来源内取该时点可见记录的最后一条，UPSERT 贡献值，RETRACT 抵消为空。
- **重复投递**（op/event_time/负载完全相同）在写入时幂等忽略。
- **开放与封存**：物化结果默认开放——之后到达且对样本 as_of 可见的迟到资料/撤回/修订（含窗口边界变化）会反映到开放范围；调用 seal 封存的范围保持历史原值和原证据，重启接续保留封存决定。不用永久冻结替代修订流程。
- **封存前预览**：preview_seal 对比已存值与当前可见值，逐项列出受影响样本的特征 key、所属窗口（左闭右开区间）与原因（late_arrival / retract / revision / boundary_shift），确认范围后再 seal。
- **对照核对**：recompute=True 仅返回 frozen/current 对照，不覆盖已存结果。
- **增量 = 整批**：增量投影按 checkpoint(last_seq) 只追加新账本记录（重启后续跑），与直接扫描账本整批重放走同一个纯函数 compute_from_rows。

## 用法

```python
from feature_service import FeatureService

svc = FeatureService("data.db")  # 或 ":memory:"

svc.register_feature("pay", entity="user", windows=[100, 1000],
                     aggregations=["count", "sum", "mean"], value_field="amount",
                     sources=("pay",))

svc.ingest("e1", "u1", source="pay", event_time=100, arrival_time=100,
           payload={"amount": 10})
svc.ingest("e2", "u1", event_time=150, arrival_time=160, payload={"amount": 20})
svc.retract("e1", "u1", source="pay", event_time=100, arrival_time=200)

svc.register_sample("s1", "user", "u1", as_of=200)
svc.materialize("s1", "pay")             # 物化为开放结果，随可见修订更新
svc.preview_seal("s1", "pay")            # 封存前预览：受影响 key / 窗口 / 原因
svc.seal("s1", "pay")                    # 确认封存（可传 feature_keys 只封存部分范围）
svc.get_features("s1", "pay", 1)         # 封存读历史快照，开放读当前可见值
svc.get_evidence("s1", "pay", 1)         # 重启后仍可查看每个值来自哪些原始记录
```

时间统一使用**整数毫秒时间戳**，由调用方显式给出，保证可重放、可测试。

关联行为来源与状态来源：

```python
svc.register_feature(
    "profile", entity="user", windows=[3600],
    aggregations=["count", "sum"], value_field="amount",
    sources=("click", "search"),
    state_source="membership", state_field="tier",
)
svc.ingest("c1", "u1", source="click", event_time=100, arrival_time=100,
           payload={"amount": 3})
svc.ingest("m1", "u1", source="membership", event_time=90, arrival_time=95,
           payload={"tier": "gold"})
# profile_w3600_count / sum 汇总 click+search；profile_last_state 取 membership
```

## 增加计算类型

新聚合只需注册一个 Aggregation，自动继承 as-of 过滤（聚合器拿到的已经是
“当时可见 + 去重抵消后”的值）：

```python
from feature_service import register_aggregation
from feature_service.aggregations import Aggregation

class StdDev(Aggregation):
    name = "stddev"
    empty = None
    def compute(self, values):
        m = sum(values) / len(values)
        return (sum((v - m) ** 2 for v in values) / len(values)) ** 0.5

register_aggregation(StdDev())
```

内置：count、sum、mean、min、max。

## 校验

```bash
python -m unittest discover -s tests -v   # 跨窗口 / 迟到 / 多来源 / 边界 / 撤回 / 开放封存 / 证据重启
python demo.py                            # 端到端演示
```
