"""Point-in-time correct event resolution and rolling-window aggregation.

The key rule implemented here:

    A record is visible at query time ``t`` iff ``arrival_time <= t``.
    Among the visible revisions of an ``event_id`` the highest revision
    wins; if that revision is a ``retract`` the event contributes nothing.

Because old training samples query with their fixed ``as_of_time``, a
revision arriving later (late data) is simply not part of their input --
it can never "travel back" and rewrite history.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from .store import EventRecord
from .types import get_compute


@dataclass(frozen=True)
class VisibleEvent:
    event_id: str
    entity_id: Optional[str]
    event_time: Optional[float]
    value: Optional[float]


def resolve_visible(
    events: list[EventRecord], arrival_cutoff: float
) -> dict[str, VisibleEvent]:
    """Collapse an append-only revision log to the state knowable at cutoff.

    Only revisions with ``arrival_time <= arrival_cutoff`` participate,
    which enforces point-in-time correctness.  The latest visible revision
    per ``event_id`` wins (true retraction/offset semantics): a later
    retract cancels every earlier upsert, and a late upsert after a
    retract re-inserts the event.
    """
    latest: dict[str, EventRecord] = {}
    for record in events:
        if record.arrival_time > arrival_cutoff:
            continue
        current = latest.get(record.event_id)
        if current is None or record.revision > current.revision:
            latest[record.event_id] = record
    visible: dict[str, VisibleEvent] = {}
    for event_id, record in latest.items():
        if record.op == "retract":
            continue
        visible[event_id] = VisibleEvent(
            event_id=event_id,
            entity_id=record.entity_id,
            event_time=record.event_time,
            value=record.value,
        )
    return visible


def compute_feature(
    spec: dict[str, Any], visible: dict[str, VisibleEvent], as_of_time: float
) -> Any:
    """Compute one rolling feature for one entity as of ``as_of_time``.

    Spec keys:
      entity     -- entity_id to match (default: the sample's own entity);
      window     -- window length in event-time units, ``null`` = all history;
      value_field-- field carrying the metric (currently top-level ``value``);
      compute    -- registered aggregation name (count/sum/mean/...).
    """
    entity_id = spec.get("entity")
    window = spec.get("window")
    value_field = spec.get("value_field", "value")
    compute_name = spec["compute"]
    fn = get_compute(compute_name)

    start = None if window is None else as_of_time - float(window)

    matched: list[tuple[float, float]] = []
    for event in visible.values():
        if entity_id is not None and event.entity_id != entity_id:
            continue
        if event.event_time is None or event.event_time > as_of_time:
            continue
        if start is not None and event.event_time < start:
            continue
        value = event.value if value_field == "value" else None
        if value is None and compute_name != "count":
            continue
        matched.append((event.event_time, float(value) if value is not None else 0.0))

    matched.sort(key=lambda pair: pair[0])
    values = [value for _, value in matched]
    return fn(values)


def compute_sample_features(
    features: list[tuple[str, dict[str, Any]]],
    events: list[EventRecord],
    entity_id: str,
    as_of_time: float,
) -> dict[str, Any]:
    """Compute every feature of a version for one sample.

    The same immutable visible-state snapshot is shared by all features of
    the sample, guaranteeing internal consistency.
    """
    visible = resolve_visible(events, as_of_time)
    result: dict[str, Any] = {}
    for name, spec in features:
        resolved = dict(spec)
        resolved.setdefault("entity", entity_id)
        result[name] = compute_feature(resolved, visible, as_of_time)
    return result
