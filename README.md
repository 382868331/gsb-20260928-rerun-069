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


## 固定研究数据集（逐样本视角 · 冻结 · 更新分类）

`ResearchDatasetService` 在特征服务之上组装一份**固定研究数据集**：

- **逐样本观察时刻**：每个样本按自己的 `as_of` 取当时可见资料，不统一取最新值。
- **逐值血缘**：每个值标明来自某次封存（`sealed` + `seal_id`）或显式采用的已发布
  修订（`revision` + `revision_id`），并保留各自定义下的证据 seq。
- **缺失区分**：`unconfirmed`（有可见值但范围仍开放、尚未确认）、
  `no_evidence`（根本没有可用证据）、`time_limit`（受观察时刻限制，如特征定义
  在 `as_of` 之后才存在）。
- **预览再冻结**：`preview_dataset` 先列出完整/不完整样本及每格来源，
  `freeze_dataset` 只冻结完整样本（值与血缘复制成快照）。
- **旧集稳定**：冻结后迟到的早期资料、历史特征修订都不自动进入旧数据集。
- **更新分类**：`propose_update` 把差异分为 `sample_scope`（样本范围）、
  `revision`（特征修订，需显式采用）、`visible_selection`（同一封存视角下当时
  可见资料的选择变化，尚无修订可采用，仅保留提示）。`adopt_update` 落到一个新的
  frozen 数据集，**未采用的差异原样保留**在新集的待办中。
- **身份计数**：`coverage` 的样本覆盖按样本身份计数；独立来源证据按来源事件身份
  `(entity, source, event_id)` 计数——同一来源事件合法参与多个特征只计一次，
  一个样本的多个特征也不增加样本数。
- **不使用未来资料**：写入冻结格时校验每个证据 `arrival_time <= as_of`，
  违反即报错，避免某格用了未来资料却把整行标成当时可用。
- **重开一致**：冻结值与血缘全部持久化，重开后 `read_row`/`lineage`/`coverage`
  与冻结时逐项一致。

```python
from feature_service import FeatureService, ResearchDatasetService

svc = FeatureService("data.db")
ds = ResearchDatasetService(svc)

did = ds.create_dataset("study-1", created_at=1200)
ds.add_feature_view(did, "pay", 1)            # 采用特征版本
ds.add_samples(did, ["s1", "s2", "s3"])
preview = ds.preview_dataset(did)            # 完整/不完整样本 + 每格原因
ds.freeze_dataset(did, frozen_at=1200)       # 只冻结完整样本
ds.read_row(did, "s1")                       # 逐项冻结值 {feature:version:key: value}
ds.lineage(did, "s1")                        # 每格 sealed/revision 来源与证据记录
ds.coverage(did)                             # sample_count / source_event_count

proposal = ds.propose_update(did)            # 三类变化，不改写数据
new_id = ds.adopt_update(                    # 只采用选中的 revision/sample_scope
    did, [["s1", "pay", 1, "pay_w1000_sum"]],
    name="study-1-r1", frozen_at=1400)
ds.pending_diffs(new_id)                     # 仍未采用的差异被保留
```

校验：`python -m unittest discover -s tests -v`；演示：`python demo_dataset.py`。

## 按来源泄漏约束划分训练/验证集

`SplitService` 在已冻结数据集之上提出、比较并采用训练/验证划分方案：

- **研究者指定**：时间界限 `time_bound`（只用 as_of 不晚于界限的历史可见
  样本，超出者排除并说明，不用未来资料补齐）、组比例 `target_ratio`、
  必须共同归组的来源关系 `relations`（如跨主体共享同一原事件的样本）。
- **自动归组**：共享原事件（来源事件身份相同）或同一主体的样本放在
  同一侧；在此约束下贪心逼近目标比例，并优先保留类别覆盖（每个类别
  尽量两侧都有）。
- **合法引用不删除**：多个特征引用同一事件按身份只计一次，既不被删除，
  也不被当成独立证据跨侧分配。
- **无法满足即报告**：约束冲突（关系引用界限外样本）、极小类别只有
  一个来源组、历史可见样本不足等，都在方案的 `unsatisfiable` 中给出
  无法满足的范围。
- **方案不可变**：新增迟到资料只能形成新方案；`compare_schemes` 并排
  比较比例偏差/类别覆盖/泄漏核对，`adopt_scheme` 显式采用其中一份。
- **隔离核对**：`validate_scheme` 以跨主体共享事件、同一主体、交叠
  封存（seal_id/证据 seq）、未来资料逐项核对来源隔离及实际类别覆盖。

```python
from feature_service import FeatureService, ResearchDatasetService, SplitService

sp = SplitService(ResearchDatasetService(svc))
scheme = sp.propose_split(did, time_bound=1500, target_ratio=0.7,
                          labels={"s1": "A", "s2": "B"},
                          relations=[["s1", "s3"]])   # 必须同侧的样本
scheme["train"]; scheme["validation"]; scheme["unsatisfiable"]
other = sp.propose_split(did, time_bound=1500, target_ratio=0.5)
sp.compare_schemes([scheme["scheme_id"], other["scheme_id"]])
sp.adopt_scheme(scheme["scheme_id"])                  # 显式采用一份
sp.validate_scheme(scheme["scheme_id"])               # 来源隔离与实际覆盖核对
```

## 校验

```bash
python -m unittest discover -s tests -v   # 跨窗口 / 迟到 / 多来源 / 边界 / 撤回 / 开放封存 / 证据重启
python demo.py                            # 特征服务端到端演示
python demo_dataset.py                    # 固定研究数据集端到端演示
```
