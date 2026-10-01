"""轻量特征计算服务：按事件时间为人工样本生成滚动统计特征。

核心原则（point-in-time correctness）::

    查询 as_of 时点只能看到 arrival_time <= as_of 的记录；
    物化结果分开放/封存两态：开放结果随当时可见的修订按范围更新，
    封存结果保持原样；未来才到达的信息任何时候都不混入。
"""

from .service import FeatureService
from .aggregations import Aggregation, register_aggregation, get_aggregation

__all__ = [
    "FeatureService",
    "Aggregation",
    "register_aggregation",
    "get_aggregation",
]
