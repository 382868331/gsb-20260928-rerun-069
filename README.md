# 特征计算服务（滚动统计 · 双时间 · point-in-time correct）

纯 Python 标准库实现（sqlite3），不依赖任何外部特征平台。第一项能力：
按**事件时间**为人工样本生成滚动统计特征，并严格保证“当时知道什么”。

## 数据模型（三层分离，各自保存）

| 层 | SQLite 表 | 说明 |
| --- | --- | --- |
| 特征定义 | feature_definitions / feature_versions | 定义只追加版本；已发布版本不可改，配置变化产生新版本 |
| 原始事件 | event_log / sources | 多来源 append-only 账本，UPSERT/RETRACT 都是新记录，历史永不删除 |
| 特征版本 | sample_features | 样本按 (特征, 版本) 物化的值、证据血缘（evidence）与封存标记 |

运行态缓存 projection_entries / checkpoint 可随时删除并由账本整批重放重建。

## 核心语义

- **双时间**：event_time 决定落在哪个窗口，arrival_time 决定查询时点是否已知。
- **不可穿越**：查询 as_of 只能使用 arrival_time <= as_of 的记录；迟到事件、迟到修订、迟到撤回对历史样本不可见，已物化的旧训练快照永久固定。
- **滚动窗口**：左闭右开 [as_of - window, as_of)。
- **多来源关联**：特征可同时声明行为来源（event，进滚动窗口）与状态来源（state，取观测前同一实体“最近一次有效状态”，event_time 相同时按写入顺序决胜）。
- **同一业务事件**以 (source, entity, event_id) 归约：取该时点可见记录的最后一条；UPSERT 贡献值，RETRACT 抵消为空。不同来源的相同 id 是不同身份。
- **证据血缘**：每个物化特征值都保存产出它的账本 seq 列表（evidence），重启后可用 ``explain`` 说明该值由哪些可见记录计算而来；逐条接入与整批重放的证据完全一致。
- **封存 / 重算**：``materialize(..., sealed=False)`` 产生未封存结果，来源更正或撤回后调用 ``recompute_unsealed`` 按影响范围重算；``seal`` 后冻结。已封存训练样本保留原值与原证据，迟到更正不可穿越（重算仍只取 arrival_time <= as_of 的记录）。
- **重复投递**（op/event_time/负载完全相同）在写入时幂等忽略。
- **增量 = 整批**：增量投影按 checkpoint(last_seq) 只追加新账本记录（重启后续跑），与直接扫描账本整批重放走同一个纯函数 compute_from_rows。

## 用法

```python
from feature_service import FeatureService

svc = FeatureService("data.db")  # 或 ":memory:"

svc.register_feature("pay", entity="user", windows=[100, 1000],
                     aggregations=["count", "sum", "mean"], value_field="amount")

svc.ingest("e1", "u1", event_time=100, arrival_time=100, payload={"amount": 10})
svc.ingest("e2", "u1", event_time=150, arrival_time=160, payload={"amount": 20})
svc.retract("e1", "u1", event_time=100, arrival_time=200)  # 撤回

svc.register_sample("s1", "user", "u1", as_of=200)
svc.materialize("s1", "pay")             # 冻结
svc.get_features("s1", "pay", 1)         # 训练时读取快照
```

多来源时间关联（状态 + 行为）与血缘：

```python
svc.register_source("risk", "state")       # 状态来源
svc.register_source("click", "event")      # 行为来源
svc.register_feature(
    "ctx", entity="user", windows=[1000],
    aggregations=["count", "sum"], value_field="amount",
    event_sources=["click"], state_sources=["risk"],
)
svc.ingest("r2", "u1", source="risk", event_time=150, arrival_time=300,
           payload={"amount": 2})
svc.ingest("k1", "u1", source="click", event_time=50, arrival_time=60,
           payload={"amount": 7})
svc.register_sample("obs", "user", "u1", as_of=1000)
svc.materialize("obs", "ctx", sealed=False)        # 未封存，可重算
svc.recompute_unsealed("obs", "ctx", 1)            # 来源更正后重算
svc.explain("obs", "ctx_state_risk")               # 值 + evidence 记录
svc.seal("obs", "ctx", 1)                          # 封存冻结
```

时间统一使用**整数毫秒时间戳**，由调用方显式给出，保证可重放、可测试。

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
python -m unittest discover -s tests -v   # 跨窗口 / 迟到 / 重复撤回 / 重启一致性
python demo.py                            # 端到端演示
```
