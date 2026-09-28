"""End-to-end demonstration of the feature computation service.

Scenario (all times are unix-like event time; ``arrival_time`` says when
the record was actually received):

* events fall in and across two 10-unit windows;
* a duplicate delivery and a retract must cancel correctly;
* a revised event arrives LATE, after the old training sample was built;
* an incremental run is interrupted and resumed in a fresh process;
* batch and incremental results must be identical.
"""

from __future__ import annotations

import json
import os
import tempfile

from feature_service.service import FeatureService


EVENTS = [
    {"event_id": "e1", "entity_id": "A", "event_time": 2, "value": 3,
     "arrival_time": 3},
    {"event_id": "e2", "entity_id": "A", "event_time": 8, "value": 7,
     "arrival_time": 9},
    {"event_id": "e3", "entity_id": "A", "event_time": 12, "value": 5,
     "arrival_time": 13},
    {"event_id": "e1", "entity_id": "A", "event_time": 2, "value": 3,
     "arrival_time": 14},
    {"event_id": "e4", "entity_id": "A", "event_time": 9, "value": 100,
     "arrival_time": 9.5},
    {"event_id": "e4", "op": "retract", "revision": 2, "entity_id": "A",
     "event_time": 9, "value": 100, "arrival_time": 15},
    {"event_id": "e2", "revision": 2, "entity_id": "A", "event_time": 8,
     "value": 70, "arrival_time": 50},
]


def main() -> None:
    db_path = os.path.join(tempfile.mkdtemp(prefix="featdemo_"), "features.db")

    with FeatureService(db_path) as svc:
        svc.define("sum_10", {"compute": "sum", "window": 10})
        svc.define("cnt_10", {"compute": "count", "window": 10})
        svc.define("sum_all", {"compute": "sum", "window": None})
        version = svc.create_version()

        svc.add_sample("old_train", "A", 10, label=1)
        svc.add_sample("new_train", "A", 20, label=0)

        for record in EVENTS:
            rec = dict(record)
            if rec["arrival_time"] <= 16:
                svc.ingest(rec.pop("event_id"), **rec)

        result = svc.run_incremental(version, target_arrival=16)
        print("incremental run #1:", json.dumps(result))
        old_snapshot = svc.get_features(version, "old_train")
        print("old_train @10 (before late data):", json.dumps(old_snapshot))

    with FeatureService(db_path) as svc:
        remaining = [dict(r) for r in EVENTS if r["arrival_time"] > 16]
        svc.ingest_many(remaining)
        result = svc.run_incremental(version)
        print("incremental run #2 after restart:", json.dumps(result))

        incremental = svc.all_features(version)
        print("incremental values:", json.dumps(incremental, sort_keys=True))

        old_after = svc.get_features(version, "old_train")
        assert old_after == old_snapshot, "late revision rewrote old sample!"
        print("old_train unchanged after late arrival: OK")

        batch = svc.compute_batch(version)
        print("batch values:      ", json.dumps(batch, sort_keys=True))
        assert batch == incremental, "batch != incremental"
        print("batch == incremental after restart: OK")
        print("database:", db_path)


if __name__ == "__main__":
    main()
