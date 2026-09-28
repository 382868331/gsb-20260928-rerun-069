"""聚合计算类型注册表。

新增计算类型时只需：

1. 实现一个 :class:`Aggregation`；
2. 用 :func:`register_aggregation` 注册。

聚合器只接收已经通过双时间（event_time 落窗 + arrival_time <= as_of）
过滤、并完成去重/抵消后的数值列表，因此“当时知道什么”的约束
由引擎统一保证，新增聚合不会也无法绕过它。
"""

from __future__ import annotations

from typing import Callable, Iterable


class Aggregation:
    name = ""
    empty = None  # 窗口内没有有效记录时的特征值

    def compute(self, values: list[float]) -> object:
        raise NotImplementedError


class _Count(Aggregation):
    name = "count"
    empty = 0

    def compute(self, values: list[float]) -> int:
        return len(values)


class _Sum(Aggregation):
    name = "sum"
    empty = 0

    def compute(self, values: list[float]) -> float:
        return sum(values)


class _Mean(Aggregation):
    name = "mean"
    empty = None

    def compute(self, values: list[float]) -> float:
        return sum(values) / len(values)


class _Min(Aggregation):
    name = "min"
    empty = None

    def compute(self, values: list[float]) -> float:
        return min(values)


class _Max(Aggregation):
    name = "max"
    empty = None

    def compute(self, values: list[float]) -> float:
        return max(values)


_REGISTRY: dict[str, Aggregation] = {}


def register_aggregation(agg: Aggregation) -> None:
    if not getattr(agg, "name", ""):
        raise ValueError("Aggregation.name 不能为空")
    _REGISTRY[agg.name] = agg


def get_aggregation(name: str) -> Aggregation:
    try:
        return _REGISTRY[name]
    except KeyError:
        raise KeyError(f"未知聚合类型: {name!r}，已注册: {sorted(_REGISTRY)}") from None


def registered_names() -> list[str]:
    return sorted(_REGISTRY)


for _agg in (_Count(), _Sum(), _Mean(), _Min(), _Max()):
    register_aggregation(_agg)


def aggregation_from_function(
    name: str, fn: Callable[[list[float]], object], empty: object = None
) -> Aggregation:
    """便捷封装：把普通函数注册成一个聚合类型。"""

    class _FuncAgg(Aggregation):
        pass

    obj = _FuncAgg()
    obj.name = name
    obj.empty = empty
    obj.compute = fn  # type: ignore[method-assign]
    register_aggregation(obj)
    return obj
