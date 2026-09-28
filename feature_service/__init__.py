"""轻量特征计算服务：按事件时间为人工样本生成滚动统计特征。

核心原则（point-in-time correctness）::

    查询 as_of 时点只能看到 arrival_time <= as_of 的记录；
    迟到事件与修订不会穿越到已经冻结的旧训练样本/旧特征版本。
"""

from .service import FeatureService
from .aggregations import Aggregation, register_aggregation, get_aggregation

__all__ = [
    "FeatureService",
    "Aggregation",
    "register_aggregation",
    "get_aggregation",
]
