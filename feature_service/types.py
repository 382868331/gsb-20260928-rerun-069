"""Pluggable rolling-computation type registry.

A computation turns the visible values inside a window into one number.
Adding a new computation type only requires registering a function here;
the registry itself is part of the engine version pinned into every
feature version, so historical versions keep "what was known then".
"""

from collections.abc import Callable, Sequence
from typing import Any

ComputeFn = Callable[[Sequence[float]], Any]


def _count(values: Sequence[float]) -> int:
    return len(values)


def _sum(values: Sequence[float]) -> float:
    return float(sum(values))


def _mean(values:Sequence[float]) -> Any:
    if not values:
        return None
    return sum(values) / len(values)


def _min(values: Sequence[float]) -> Any:
    return min(values) if values else None


def _max(values: Sequence[float]) -> Any:
    return max(values) if values else None


def _std(values: Sequence[float]) -> Any:
    n = len(values)
    if n == 0:
        return None
    mean = sum(values) / n
    variance = sum((v - mean) ** 2 for v in values) / n
    return variance ** 0.5


def _first(values: Sequence[float]) -> Any:
    return values[0] if values else None


def _last(values: Sequence[float]) -> Any:
    return values[-1] if values else None


_REGISTRY: dict[str, ComputeFn] = {
    "count": _count,
    "sum": _sum,
    "mean": _mean,
    "min": _min,
    "max": _max,
    "std": _std,
    "first": _first,
    "last": _last,
}


def register_compute(name: str, fn: ComputeFn, *, override: bool = False) -> None:
    """Register a new computation type (or override an existing one).

    Overriding does not alter already-created feature versions: their
    snapshot pins ``engine_version`` and the definition list, and recomputation
    is explicit.
    """
    if not name or not isinstance(name, str):
        raise ValueError("compute name must be a non-empty string")
    if name in _REGISTRY and not override:
        raise ValueError(f"compute type {name!r} already registered")
    _REGISTRY[name] = fn


def get_compute(name: str) -> ComputeFn:
    try:
        return _REGISTRY[name]
    except KeyError:
        raise ValueError(f"unknown compute type {name!r}") from None


def compute_types() -> list[str]:
    return sorted(_REGISTRY)
