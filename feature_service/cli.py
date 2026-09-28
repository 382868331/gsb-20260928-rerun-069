"""Minimal command-line interface (no external feature platform needed)."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from .service import FeatureService
from .types import compute_types


def _load(path: str | None) -> Any:
    if path is None:
        return json.load(sys.stdin)
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="feature-service")
    parser.add_argument("--db", default="features.db", help="SQLite path")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("ingest", help="append event revisions from JSON file")
    p.add_argument("file", nargs="?")

    p = sub.add_parser("define", help="upsert a feature definition")
    p.add_argument("name")
    p.add_argument("spec", help="JSON spec, e.g. '{\"compute\":\"sum\",\"window\":10}'")

    p = sub.add_parser("version", help="freeze a feature version")
    p.add_argument("--features", nargs="*")

    p = sub.add_parser("sample", help="add a manual sample")
    p.add_argument("sample_id")
    p.add_argument("entity_id")
    p.add_argument("as_of_time", type=float)
    p.add_argument("--label", type=float)

    p = sub.add_parser("batch", help="compute all samples for a version")
    p.add_argument("version")

    p = sub.add_parser("incremental", help="advance incremental cursor")
    p.add_argument("version")
    p.add_argument("--target", type=float)

    p = sub.add_parser("get", help="read computed features")
    p.add_argument("version")
    p.add_argument("sample_id")

    sub.add_parser("types", help="list registered compute types")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.cmd == "types":
        print(json.dumps(compute_types()))
        return 0

    with FeatureService(args.db) as svc:
        if args.cmd == "ingest":
            records = _load(args.file)
            statuses = svc.ingest_many(records)
            print(json.dumps(statuses))
        elif args.cmd == "define":
            svc.define(args.name, json.loads(args.spec))
            print("ok")
        elif args.cmd == "version":
            print(svc.create_version(args.features))
        elif args.cmd == "sample":
            svc.add_sample(args.sample_id, args.entity_id, args.as_of_time, args.label)
            print("ok")
        elif args.cmd == "batch":
            print(json.dumps(svc.compute_batch(args.version), sort_keys=True))
        elif args.cmd == "incremental":
            print(json.dumps(svc.run_incremental(args.version, target_arrival=args.target)))
        elif args.cmd == "get":
            print(json.dumps(svc.get_features(args.version, args.sample_id), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
