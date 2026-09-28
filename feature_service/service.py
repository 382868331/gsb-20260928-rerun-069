"""Public feature-computation service API."""

from __future__ import annotations
import time
import uuid
from collections.abc import Iterable
from typing import Any, Optional

from ._version import ENGINE_VERSION
from .engine import compute_sample_features
from .store import EventRecord, Store
from .types import get_compute


class FeatureService:
    def __init__(self, path: str = ":memory:"):
        self.store = Store(path)

    def close(self) -> None:
        self.store.close()

    def __enter__(self) -> "FeatureService":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- events -----------------------------------------------------------

    def ingest(
        self,
        event_id: str,
        *,
        op: str = "upsert",
        revision: int = 1,
        entity_id: Optional[str] = None,
        event_time: Optional[float] = None,
        value: Optional[float] = None,
        payload: Optional[dict[str, Any]] = None,
        arrival_time: Optional[float] = None,
    ) -> str:
        """Append one raw event revision.

        Returns ``"inserted"`` or ``"duplicate"``.  Exact replays of an
        ``(event_id, revision)`` are duplicates and are silently dropped;
        conflicting content for the same revision raises ``ValueError``.
        """
        status = self.store.insert_event(
            event_id=event_id,
            revision=revision,
            op=op,
            entity_id=entity_id,
            event_time=event_time,
            value=value,
            payload=payload,
            arrival_time=arrival_time,
        )
        self.store.commit()
        return status

    def ingest_many(self, records: Iterable[dict[str, Any]]) -> list[str]:
        statuses = []
        for record in records:
            rec = dict(record)
            event_id = rec.pop("event_id")
            statuses.append(self.ingest(event_id, **rec))
        return statuses

    # -- definitions & versions ------------------------------------------

    def define(self, name: str, spec: dict[str, Any]) -> None:
        if "compute" not in spec:
            raise ValueError("spec requires a 'compute' type")
        get_compute(spec["compute"])
        if "window" not in spec:
            raise ValueError("spec requires a 'window' (use null for all-time)")
        self.store.upsert_definition(name, spec)
        self.store.commit()

    def create_version(
        self,
        names: Optional[list[str]] = None,
        *,
        version: Optional[str] = None,
    ) -> str:
        """Freeze an immutable feature version from current definitions."""
        all_defs = self.store.list_definitions()
        chosen = sorted(names) if names is not None else sorted(all_defs)
        missing = [n for n in chosen if n not in all_defs]
        if missing:
            raise ValueError(f"undefined features: {missing}")
        specs = [(n, all_defs[n]) for n in chosen]
        version = version or f"fv_{uuid.uuid4().hex[:12]}"
        resolved = self.store.create_version(version, specs, ENGINE_VERSION)
        self.store.commit()
        return resolved

    # -- samples ----------------------------------------------------------

    def add_sample(
        self,
        sample_id: str,
        entity_id: str,
        as_of_time: float,
        label: Optional[float] = None,
    ) -> None:
        self.store.upsert_sample(sample_id, entity_id, as_of_time, label)
        self.store.commit()

    # -- computation ------------------------------------------------------

    def _compute_sample(
        self,
        version: str,
        sample: Any,
        events: list[EventRecord],
    ) -> dict[str, Any]:
        features = self.store.version_features(version)
        values = compute_sample_features(
            features, events, sample["entity_id"], float(sample["as_of_time"])
        )
        self.store.replace_values(version, sample["sample_id"], values)
        return values

    def compute_batch(self, version: str) -> dict[str, dict[str, Any]]:
        """Recompute every sample for a version from the full event log."""
        samples = self.store.samples()
        all_events = self.store.events()
        out: dict[str, dict[str, Any]] = {}
        for sample in samples:
            values = self._compute_sample(version, sample, all_events)
            out[sample["sample_id"]] = values
        self.store.commit()
        return out

    def run_incremental(
        self,
        version: str,
        *,
        target_arrival: Optional[float] = None,
    ) -> dict[str, Any]:
        """Advance the restartable incremental cursor and recompute affected
        samples.

        Only events with ``arrival_time`` past the stored cursor are read;
        a sample is recomputed exactly when its ``as_of_time`` is within the
        newly covered arrival range.  Samples already fully in the past
        before this run are touched never again, which is precisely what
        prevents late revisions from rewriting old training samples.
        Restarting resumes from the committed cursor; the final values are
        identical to ``compute_batch`` (asserted in the tests).
        """
        cursor = self.store.get_watermark()
        max_arrival = self.store.max_arrival()
        if max_arrival is None:
            return {"cursor": cursor, "recomputed": [], "new_cursor": cursor}
        if target_arrival is None:
            target_arrival = max_arrival
        target_arrival = min(float(target_arrival), max_arrival)
        if cursor is not None and target_arrival <= cursor:
            return {"cursor": cursor, "recomputed": [], "new_cursor": cursor}

        events = self.store.events(arrival_at_most=target_arrival)
        samples = self.store.samples()
        recomputed: list[str] = []
        for sample in samples:
            as_of = float(sample["as_of_time"])
            if cursor is not None and as_of <= cursor:
                continue
            if as_of > target_arrival:
                continue
            self._compute_sample(version, sample, events)
            recomputed.append(sample["sample_id"])

        self.store.set_watermark(target_arrival)
        self.store.commit()
        return {
            "cursor": cursor,
            "recomputed": recomputed,
            "new_cursor": target_arrival,
        }

    def get_features(self, version: str, sample_id: str) -> dict[str, Any]:
        return self.store.get_values(version, sample_id)

    def all_features(self, version: str) -> dict[str, dict[str, Any]]:
        return self.store.all_values(version)
