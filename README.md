# 特征计算服务（Feature Computation Service）

纯 Python（标准库 + SQLite）实现，不依赖任何外部特征平台。第一项能力：
按**事件时间**给人工样本生成滚动统计特征，同时严格保证 point-in-time
correctness（“当时知道什么”）。

## 核心约束

1. **查询时点可见性**：查询时间为 `t` 的样本只使用 `arrival_time <= t`
   的记录。记录有两个时间：`event_time`（业务发生时间）与
   `arrival_time`（系统实际收到的时间）。
2. **迟到修订不穿越**：晚到的修订对 `as_of_time` 更早的旧训练样本不可见，
   旧样本一旦计算就不会被未来的数据改写。
3. **重复与撤回抵消**：事件日志是 append-only 的修订链，同一 `event_id`
   取当时可见的最高 `revision`；`retract` 抵消该事件此前所有 `upsert`，
   撤回后再到的高版本 upsert 可以重新生效。相同 `(event_id, revision)`
   的重复投递按业务内容去重（重复包的再次到达不改变“最早知道”时刻），
   内容冲突则报错。
4. **定义、原始事件、特征版本分离保存**：
   - `event_log`：原始事件，只追加；
   - `feature_def`：当前特征定义；
   - `feature_version` / `version_feature`：不可变版本快照，冻结定义列表
     与引擎版本，历史版本的含义永不改变；
   - `feature_value`：按 (版本, 样本) 保存的计算结果。
5. **重启增量 == 整批**：增量处理以到达时间水位（watermark）为游标，
   提交后持久化；重启后从断点继续。只重算落在新覆盖到达区间内的样本，
   最终结果与 `compute_batch` 全量重算逐值相等（测试中强制断言）。

## 模块

- `feature_service/store.py`：SQLite 持久层与表结构；
- `feature_service/engine.py`：可见性解析（修订/撤回抵消）与滚动窗口聚合；
- `feature_service/types.py`：可扩展计算类型注册表（内置
  `count/sum/mean/min/max/std/first/last`，新增类型用
  `register_compute(name, fn)`，类型集随引擎版本固化进特征版本）；
- `feature_service/service.py`：对外 API（摄入、定义、版本、样本、批/增量计算）；
- `feature_service/cli.py`：命令行；
- `demo.py`：跨窗口 + 重复 + 撤回 + 迟到修订 + 重启一致性的端到端演示；
- `tests/`：pytest 用例。

## 使用

```python
from feature_service import FeatureService

svc = FeatureService("features.db")
svc.define("sum_10", {"compute": "sum", "window": 10})
svc.define("cnt_all", {"compute": "count", "window": None})
version = svc.create_version()

svc.ingest("e1", entity_id="A", event_time=2, value=3, arrival_time=3)
svc.add_sample("s1", "A", as_of_time=10, label=1)

svc.compute_batch(version)          # 整批
svc.run_incremental(version)        # 可重启增量（水位持久化）
svc.get_features(version, "s1")
```

命令行：

```bash
python -m feature_service.cli --db f.db types
python -m feature_service.cli --db f.db ingest events.json
python -m feature_service.cli --db f.db define sum_10 '{"compute":"sum","window":10}'
python -m feature_service.cli --db f.db version
python -m feature_service.cli --db f.db sample s1 A 10 --label 1
python -m feature_service.cli --db f.db batch fv_xxx
python -m feature_service.cli --db f.db incremental fv_xxx
```

## 验证

```bash
python demo.py
python -m pytest -q
```
