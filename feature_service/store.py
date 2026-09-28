"""SQLite-backed persistence.

Tables deliberately keep the three concerns apart:

* ``event_log``        -- raw events, append-only, never mutated in place;
* ``feature_def``      -- current feature definitions (metadata only);
* ``feature_version``  -- immutable versioned snapshots of a definition set;
* ``version_feature``  -- the definitions frozen inside each version;
* ``sample``           -- manual labelled samples (entity + as-of time);
* ``feature_value``    -- values computed for (sample, version);
* ``watermark``        -- restartable ingestion cursor.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from dataclasses import dataclass
from typing import Any, Iterable, Optional


SCHEMA = """
CREATE TABLE IF NOT EXISTS event_log (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id      TEXT    NOT NULL,
    revision      INTEGER NOT NULL,
    op            TEXT    NOT NULL CHECK (op IN ('upsert', 'retract')),
    entity_id     TEXT,
    event_time    REAL,
    value         REAL,
    payload       TEXT    NOT NULL DEFAULT '{}',
    arrival_time  REAL    NOT NULL,
    checksum      TEXT    NOT NULL,
    ingested_at   REAL    NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_event_rev
    ON event_log(event_id, revision);
CREATE INDEX IF NOT EXISTS idx_event_arrival ON event_log(arrival_time);
CREATE INDEX IF NOT EXISTS idx_event_entity_time
    ON event_log(entity_id, event_time);

CREATE TABLE IF NOT EXISTS feature_def (
    name        TEXT PRIMARY KEY,
    spec        TEXT NOT NULL,
    created_at  REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS feature_version (
    version        TEXT PRIMARY KEY,
    spec_hash      TEXT NOT NULL,
    engine_version TEXT NOT NULL,
    created_at     REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS version_feature (
    version     TEXT NOT NULL,
    name        TEXT NOT NULL,
    spec        TEXT NOT NULL,
    PRIMARY KEY (version, name),
    FOREIGN KEY (version) REFERENCES feature_version(version)
);

CREATE TABLE IF NOT EXISTS sample (
    sample_id    TEXT PRIMARY KEY,
    entity_id    TEXT NOT NULL,
    as_of_time   REAL NOT NULL,
    label        REAL,
    created_at   REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sample_asof ON sample(as_of_time);

CREATE TABLE IF NOT EXISTS feature_value (
    version      TEXT NOT NULL,
    sample_id    TEXT NOT NULL,
    name         TEXT NOT NULL,
    value        TEXT,
    computed_at  REAL NOT NULL,
    PRIMARY KEY (version, sample_id, name)
);

CREATE TABLE IF NOT EXISTS watermark (
    key   TEXT PRIMARY KEY,
    value REAL NOT NULL
);
"""


def _checksum(data: dict[str, Any]) -> str:
    blob = json.dumps(data, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(blob).hexdigest()


@dataclass(frozen=True)
class EventRecord:
    event_id: str
    revision: int
    op: str
    entity_id: Optional[str]
    event_time: Optional[float]
    value: Optional[float]
    payload: str
    arrival_time: float


class Store:
    def __init__(self, path: str = ":memory:"):
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "Store":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- raw events -------------------------------------------------------

    def insert_event(
        self,
        *,
        event_id: str,
        revision: int,
        op: str,
        entity_id: Optional[str],
        event_time: Optional[float],
        value: Optional[float],
        payload: Optional[dict[str, Any]],
        arrival_time: Optional[float],
    ) -> str:
        if op not in ("upsert", "retract"):
            raise ValueError("op must be 'upsert' or 'retract'")
        if revision < 1:
            raise ValueError("revision must be >= 1")
        payload_json = json.dumps(payload or {}, sort_keys=True)
        now = time.time()
        arr = now if arrival_time is None else float(arrival_time)
        # Business-content fingerprint (excludes arrival_time): a redelivered
        # copy of the same revision may be re-received later, but knowledge
        # dates from its first arrival, so the repeat is ignored wholesale.
        checksum = _checksum(
            {
                "event_id": event_id,
                "revision": revision,
                "op": op,
                "entity_id": entity_id,
                "event_time": event_time,
                "value": value,
                "payload": payload_json,
            }
        )
        existing = self.conn.execute(
            "SELECT checksum FROM event_log WHERE event_id=? AND revision=?",
            (event_id, revision),
        ).fetchone()
        if existing is not None:
            if existing["checksum"] != checksum:
                raise ValueError(
                    f"conflicting revision for event {event_id!r} r{revision}"
                )
            return "duplicate"
        self.conn.execute(
            """INSERT INTO event_log
               (event_id, revision, op, entity_id, event_time, value,
                payload, arrival_time, checksum, ingested_at)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (
                event_id,
                revision,
                op,
                entity_id,
                event_time,
                value,
                payload_json,
                arr,
                checksum,
                now,
            ),
        )
        return "inserted"

    def events(
        self,
        *,
        arrival_after: Optional[float] = None,
        arrival_at_most: Optional[float] = None,
    ) -> list[EventRecord]:
        sql = "SELECT * FROM event_log WHERE 1=1"
        args: list[Any] = []
        if arrival_after is not None:
            sql += " AND arrival_time > ?"
            args.append(arrival_after)
        if arrival_at_most is not None:
            sql += " AND arrival_time <= ?"
            args.append(arrival_at_most)
        sql += " ORDER BY arrival_time, id"
        rows = self.conn.execute(sql, args).fetchall()
        return [
            EventRecord(
                event_id=r["event_id"],
                revision=r["revision"],
                op=r["op"],
                entity_id=r["entity_id"],
                event_time=r["event_time"],
                value=r["value"],
                payload=r["payload"],
                arrival_time=r["arrival_time"],
            )
            for r in rows
        ]

    def max_arrival(self) -> Optional[float]:
        row = self.conn.execute("SELECT MAX(arrival_time) m FROM event_log").fetchone()
        return row["m"]

    # -- definitions ------------------------------------------------------

    def upsert_definition(self, name: str, spec: dict[str, Any]) -> None:
        self.conn.execute(
            """INSERT INTO feature_def(name, spec, created_at) VALUES(?,?,?)
               ON CONFLICT(name) DO UPDATE SET spec=excluded.spec""",
            (name, json.dumps(spec, sort_keys=True), time.time()),
        )

    def get_definition(self, name: str) -> Optional[dict[str, Any]]:
        row = self.conn.execute(
            "SELECT spec FROM feature_def WHERE name=?", (name,)
        ).fetchone()
        return json.loads(row["spec"]) if row else None

    def list_definitions(self) -> dict[str, dict[str, Any]]:
        rows = self.conn.execute("SELECT name, spec FROM feature_def").fetchall()
        return {r["name"]: json.loads(r["spec"]) for r in rows}

    # -- versions ---------------------------------------------------------

    def create_version(
        self, version: str, specs: list[tuple[str, dict[str, Any]]], engine_version: str
    ) -> str:
        spec_blob = json.dumps(
            sorted((n, s) for n, s in specs), sort_keys=True
        ).encode()
        spec_hash = hashlib.sha256(spec_blob).hexdigest()
        found = self.conn.execute(
            "SELECT version FROM feature_version WHERE spec_hash=? AND engine_version=?",
            (spec_hash, engine_version),
        ).fetchone()
        if found:
            return found["version"]
        self.conn.execute(
            "INSERT INTO feature_version(version, spec_hash, engine_version, created_at)"
            " VALUES(?,?,?,?)",
            (version, spec_hash, engine_version, time.time()),
        )
        self.conn.executemany(
            "INSERT INTO version_feature(version, name, spec) VALUES(?,?,?)",
            [(version, n, json.dumps(s, sort_keys=True)) for n, s in specs],
        )
        return version

    def version_features(self, version: str) -> list[tuple[str, dict[str, Any]]]:
        rows = self.conn.execute(
            "SELECT name, spec FROM version_feature WHERE version=? ORDER BY name",
            (version,),
        ).fetchall()
        if not rows:
            raise ValueError(f"unknown version {version!r}")
        return [(r["name"], json.loads(r["spec"])) for r in rows]

    def latest_version(self) -> Optional[str]:
        row = self.conn.execute(
            "SELECT version FROM feature_version ORDER BY created_at DESC, rowid DESC LIMIT 1"
        ).fetchone()
        return row["version"] if row else None

    def list_versions(self) -> list[str]:
        return [
            r["version"]
            for r in self.conn.execute(
                "SELECT version FROM feature_version ORDER BY created_at"
            ).fetchall()
        ]

    # -- samples ----------------------------------------------------------

    def upsert_sample(
        self,
        sample_id: str,
        entity_id: str,
        as_of_time: float,
        label: Optional[float] = None,
    ) -> None:
        self.conn.execute(
            """INSERT INTO sample(sample_id, entity_id, as_of_time, label, created_at)
               VALUES(?,?,?,?,?)
               ON CONFLICT(sample_id) DO UPDATE SET
                 entity_id=excluded.entity_id,
                 as_of_time=excluded.as_of_time,
                 label=excluded.label""",
            (sample_id, entity_id, float(as_of_time), label, time.time()),
        )

    def samples(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM sample ORDER BY as_of_time, sample_id"
        ).fetchall()

    # -- values -----------------------------------------------------------

    def replace_values(
        self,
        version: str,
        sample_id: str,
        values: dict[str, Any],
    ) -> None:
        now = time.time()
        self.conn.execute(
            "DELETE FROM feature_value WHERE version=? AND sample_id=?",
            (version, sample_id),
        )
        self.conn.executemany(
            """INSERT INTO feature_value(version, sample_id, name, value, computed_at)
               VALUES(?,?,?,?,?)""",
            [
                (version, sample_id, name, json.dumps(v, allow_nan=False), now)
                for name, v in values.items()
            ],
        )

    def get_values(self, version: str, sample_id: str) -> dict[str, Any]:
        rows = self.conn.execute(
            "SELECT name, value FROM feature_value WHERE version=? AND sample_id=?",
            (version, sample_id),
        ).fetchall()
        return {r["name"]: json.loads(r["value"]) for r in rows}

    def all_values(self, version: str) -> dict[str, dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT sample_id, name, value FROM feature_value WHERE version=?",
            (version,),
        ).fetchall()
        out: dict[str, dict[str, Any]] = {}
        for r in rows:
            out.setdefault(r["sample_id"], {})[r["name"]] = json.loads(r["value"])
        return out

    # -- watermark --------------------------------------------------------

    def get_watermark(self, key: str = "incremental") -> Optional[float]:
        row = self.conn.execute(
            "SELECT value FROM watermark WHERE key=?", (key,)
        ).fetchone()
        return row["value"] if row else None

    def set_watermark(self, value: float, key: str = "incremental") -> None:
        self.conn.execute(
            """INSERT INTO watermark(key, value) VALUES(?,?)
               ON CONFLICT(key) DO UPDATE SET value=excluded.value""",
            (key, float(value)),
        )

    def commit(self) -> None:
        self.conn.commit()
