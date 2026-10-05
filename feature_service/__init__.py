"""轻量特征计算服务 + 固定研究数据集组装。

point-in-time correctness：查询 as_of 只能看到 arrival_time <= as_of 的记录；
物化结果分开放/封存，封存保持原样；研究数据集逐样本按自身观察时刻组装，
冻结后迟到资料与历史修订不自动进入旧数据集。
"""

from .service import FeatureService
from .dataset import ResearchDatasetService, ResearchDatasetError
from .split import SplitService
from .aggregations import Aggregation, register_aggregation, get_aggregation

__all__ = [
    "FeatureService",
    "ResearchDatasetService",
    "ResearchDatasetError",
    "SplitService",
    "Aggregation",
    "register_aggregation",
    "get_aggregation",
]
