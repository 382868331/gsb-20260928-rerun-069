# 特征计算服务（多来源时间关联 · 滚动统计 · 双时间）

纯 Python 标准库实现（sqlite3），不依赖任何外部特征平台。支持在一次观测点
关联多个来源：汇总同一实体一段历史行为，并取观测前最近一次有效状态；所有
来源都严格保证“当时知道什么”。

## 数据模型（三层分离，各自保存）

| 层 | SQLite 表 | 说明 |
| --- | --- | --- |
| 特征定义 | feature_definitions / feature_versions | 定义只追加版本；已发布版本不可改，配置变化产生新版本 |
| 原始事件 | event_log | append-only 账本，UPSERT/RETRACT 都是新记录，历史永不删除 |
| 特征版本 | sample_features | 样本按 (特征, 版本) 物化的值，分开放(open)/封存(sealed)两态 |
| 特征证据 | sample_feature_evidence | 每个保存值使用的 behavior/state 账本 seq |

运行态缓存 projection_entries / checkpoint 可随时删除并由账本整批重放重建。

## 核心语义

- **双时间**：event_time 决定落在哪个窗口，arrival_time 决定查询时点是否已知。
- **不可穿越**：查询 as_of 只能使用 arrival_time <= as_of 的记录；未来才到达的信息任何时候都不混入当前结果。
- **滚动窗口**：左闭右开 [as_of - window, as_of)。
- **多来源关联**：行为记录按 `sources` 过滤后进入滚动窗口；`state_source` 中取 `event_time <= as_of` 且 `arrival_time <= as_of` 的最近有效状态。
- **最近状态**：先按事件时间选最近；事件时间相同再按到达时间，仍相同按账本 seq（同批边界消息的确定性顺序）选择。
- **同一业务事件**以 (entity, source, event_id) 归约：不同来源可安全复用同一 ID；同来源内取该时点可见记录的最后一条，UPSERT 贡献值，RETRACT 抵消为空。
- **重复投递**（op/event_time/负载完全相同）在写入时幂等忽略。
- **开放与封存**：物化结果默认开放——之后补登到账本、且当时可见（arrival_time <= as_of）的迟到资料/撤回/修订，会在下次 materialize 时按影响范围只更新仍未封存的 feature_key；封存的 feature_key 保持原值与原证据，永不改写，不能用永久冻结替代修订流程。
- **封存流程**：先 `preview_seal` 预览受影响样本、窗口和原因（late_arrival / retract / revision / window_boundary / new_data），再用 `seal` 确认封存哪些范围；封存值取封存时点当时可见的计算结果。一次封存可以只确认部分样本和范围，剩余部分继续开放；每次 `seal` 调用是一批确认（`seal_batches`），后续批次允许与先前范围交叠——交叠的 key 保持首次确认的值与归属批次，在新批中记为 already_sealed，不重复计入也不遗漏。
- **归属查询**：`query_features` 逐 feature_key 说明当前值归属哪次封存确认（seal_id）、哪些部分仍可更新（updatable），以及每个值使用的账本 seq；同一条来源变动跨越多个范围时，每个范围只计入一次。
- **修订流程**：对已封存结果用 `propose_revision` 提出修订——列出新旧值、新值采用的原始资料与受影响下游（共享同一来源记录的其它已保存结果）；`publish_revision` 发布后成为独立可查的修订视角，`reject_revision` 驳回。未采纳的修订不改变当前已确认视角，也不会提前影响依赖这些特征的研究；已封存的原始值任何情况下都不被改写。
- **重启接续**：封存决定（status / sealed_at）、历史值与证据全部持久化，重启后保留；开放范围仍随可见资料修订。
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
svc.materialize("s1", "pay")             # 物化（结果默认开放）
svc.preview_seal("s1", "pay", 1)         # 封存前预览：受影响窗口、前后值与原因
svc.seal("s1", "pay", 1,
         feature_keys=["pay_w100_sum"])  # 确认封存指定范围；缺省封存全部开放范围
svc.get_features("s1", "pay", 1)         # 读取保存值：开放=最新可见，封存=历史原样
svc.seal_status("s1", "pay", 1)          # 每个 feature_key 的开放/封存状态
svc.query_features("s1", "pay", 1)       # 每个值归属哪次确认、哪些仍可更新、证据 seq
svc.seal_batches("s1", "pay", 1)         # 每次封存批次的确认/交叠明细

rev = svc.propose_revision("s1", "pay", 1)   # 对已封存结果提出修订：新旧值/资料/下游
svc.publish_revision(rev["revision_id"])     # 发布为独立修订视角（原封存值不变）
svc.get_revision(rev["revision_id"])         # 查询已采纳修订的血缘与当时可见资料
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
