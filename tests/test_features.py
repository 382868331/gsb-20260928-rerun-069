import pytest

from feature_service.engine import resolve_visible
from feature_service.service import FeatureService
from feature_service.types import register_compute


def _base_service(path=":memory:"):
    svc = FeatureService(path)
    svc.define("sum_10", {"compute": "sum", "window": 10})
    svc.define("cnt_10", {"compute": "count", "window": 10})
    svc.define("sum_all", {"compute": "sum", "window": None})
    svc.define("mean_10", {"compute": "mean", "window": 10})
    version = svc.create_version()
    return svc, version


EVENTS = [
    {"event_id": "e1", "entity_id": "A", "event_time": 2, "value": 3,
     "arrival_time": 3},
    {"event_id": "e2", "entity_id": "A", "event_time": 8, "value": 7,
     "arrival_time": 9},
    {"event_id": "e3", "entity_id": "A", "event_time": 12, "value": 5,
     "arrival_time": 13},
    # duplicate delivery of e1
    {"event_id": "e1", "entity_id": "A", "event_time": 2, "value": 3,
     "arrival_time": 14},
    # insert then retract
    {"event_id": "e4", "entity_id": "A", "event_time": 9, "value": 100,
     "arrival_time": 9},
    {"event_id": "e4", "op": "retract", "revision": 2,
     "entity_id": "A", "event_time": 9, "value": 100,
     "arrival_time": 15},
    # late revision of e2
    {"event_id": "e2", "revision": 2, "entity_id": "A", "event_time": 8,
     "value": 70, "arrival_time": 50},
    # other entity, must not leak into A features
    {"event_id": "b1", "entity_id": "B", "event_time": 5, "value": 999,
     "arrival_time": 6},
    # event arriving before cutoff but with future event_time: excluded
    {"event_id": "future", "entity_id": "A", "event_time": 99, "value": 1,
     "arrival_time": 9},
]


def _ingest(svc, events):
    for record in events:
        rec = dict(record)
        event_id = rec.pop("event_id")
        svc.ingest(event_id, **rec)


def test_duplicate_and_retract_and_late_revision():
    svc, version = _base_service()
    _ingest(svc, EVENTS)
    svc.add_sample("s10", "A", 10)
    svc.add_sample("s20", "A", 20)
    svc.add_sample("s50", "A", 50)
    out = svc.compute_batch(version)

    # @10: e1=3, e2=7 visible; e4 retracted at arrival 15 > 10 -> retract
    # NOT visible, but e4 upsert arrived at 9 <= 10 so it counts (100).
    # The retraction is information only available at t=15.
    assert out["s10"]["sum_10"] == pytest.approx(3 + 7 + 100)
    assert out["s10"]["cnt_10"] == 3

    # @20: retract visible -> e4 gone; late revision of e2 (arrival 50)
    # still invisible, so e2=7; window [10,20] contains e3=5 for sum_10,
    # all-time sum contains e1,e2,e3 = 15
    assert out["s20"]["sum_10"] == pytest.approx(5)
    assert out["s20"]["sum_all"] == pytest.approx(3 + 7 + 5)
    assert out["s20"]["cnt_10"] == 1

    # @50: revised e2=70 is now known
    assert out["s50"]["sum_all"] == pytest.approx(3 + 70 + 5)
    svc.close()


def test_late_data_does_not_rewrite_old_sample_on_restart(tmp_path):
    db = tmp_path / "f.db"
    svc, version = _base_service(str(db))
    early = [r for r in EVENTS if r["arrival_time"] <= 16]
    late = [r for r in EVENTS if r["arrival_time"] > 16]
    _ingest(svc, early)
    svc.add_sample("old", "A", 10)
    svc.add_sample("new", "A", 20)
    svc.run_incremental(version, target_arrival=16)
    old_snapshot = svc.get_features(version, "old")
    svc.close()

    # restart in a fresh process, feed late data
    svc = FeatureService(str(db))
    _ingest(svc, late)
    svc.run_incremental(version)
    assert svc.get_features(version, "old") == old_snapshot
    incremental = svc.all_features(version)

    batch = svc.compute_batch(version)
    assert batch == incremental
    svc.close()


def test_incremental_matches_batch_any_arrival_order(tmp_path):
    db = tmp_path / "f.db"
    svc, version = _base_service(str(db))
    svc.add_sample("s5", "A", 5)
    svc.add_sample("s15", "A", 15)
    svc.add_sample("s60", "A", 60)
    svc.close()

    # ingest in arbitrary arrival chunks, restarting between chunks
    arrivals = sorted({r["arrival_time"] for r in EVENTS})
    events_by_arrival = {}
    for record in EVENTS:
        events_by_arrival.setdefault(record["arrival_time"], []).append(record)
    for i, t in enumerate(arrivals):
        svc = FeatureService(str(db))
        _ingest(svc, events_by_arrival[t])
        svc.run_incremental(version)
        svc.close()

    svc = FeatureService(str(db))
    batch = svc.compute_batch(version)
    incremental = svc.all_features(version)
    assert batch == incremental
    svc.close()


def test_conflicting_revision_rejected():
    svc = FeatureService(":memory:")
    svc.ingest("x", entity_id="A", event_time=1, value=2, arrival_time=1)
    with pytest.raises(ValueError):
        svc.ingest("x", entity_id="A", event_time=1, value=3, arrival_time=2)
    svc.close()


def test_duplicate_ingest_is_idempotent():
    svc = FeatureService(":memory:")
    assert svc.ingest("x", entity_id="A", event_time=1, value=2,
                      arrival_time=1) == "inserted"
    assert svc.ingest("x", entity_id="A", event_time=1, value=2,
                      arrival_time=1) == "duplicate"
    svc.close()


def test_resolve_visible_retract_then_reinsert():
    from feature_service.store import EventRecord

    def rec(event_id, rev, op, arrival):
        return EventRecord(event_id, rev, op, "A", 1.0, 5.0, "{}", arrival)

    events = [
        rec("x", 1, "upsert", 1),
        rec("x", 2, "retract", 2),
        rec("x", 3, "upsert", 3),
    ]
    assert "x" not in resolve_visible(events, 2)
    assert resolve_visible(events, 3)["x"].value == 5.0


def test_versions_are_immutable_and_independent():
    svc = FeatureService(":memory:")
    svc.define("f", {"compute": "sum", "window": 10})
    v1 = svc.create_version()
    # change definition, then create a new version
    svc.define("f", {"compute": "mean", "window": 10})
    v2 = svc.create_version()
    assert v1 != v2
    svc.ingest("e1", entity_id="A", event_time=1, value=4, arrival_time=1)
    svc.ingest("e2", entity_id="A", event_time=2, value=8, arrival_time=2)
    svc.add_sample("s", "A", 5)
    b1 = svc.compute_batch(v1)
    b2 = svc.compute_batch(v2)
    assert b1["s"]["f"] == pytest.approx(12)
    assert b2["s"]["f"] == pytest.approx(6)
    svc.close()


def test_custom_compute_type_registration():
    register_compute("double_sum", lambda vs: 2 * sum(vs), override=True)
    svc = FeatureService(":memory:")
    svc.define("d", {"compute": "double_sum", "window": None})
    version = svc.create_version()
    svc.ingest("e", entity_id="A", event_time=1, value=5, arrival_time=1)
    svc.add_sample("s", "A", 2)
    out = svc.compute_batch(version)
    assert out["s"]["d"] == pytest.approx(10)
    svc.close()
