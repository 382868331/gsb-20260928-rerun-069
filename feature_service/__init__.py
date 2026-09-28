"""Point-in-time correct rolling-feature computation service.

The service stores three things separately and immutably:

1. **raw events** -- an append-only log of upsert / retract revisions with
   both ``event_time`` (business time) and ``arrival_time`` (when the record
   was actually known);
2. **definitions** -- the declared feature definitions;
3. **feature versions** -- immutable snapshots of a set of definitions plus
   the computation-engine version.

Computed values are always attached to a feature version and a manual sample,
so late revisions can never retroactively change an old training sample: a
 query at time ``t`` only ever sees records with ``arrival_time <= t``.
"""

from .service import FeatureService
from .types import register_compute, compute_types
from ._version import ENGINE_VERSION

__all__ = [
    "FeatureService",
    "ENGINE_VERSION",
    "register_compute",
    "compute_types",
]
