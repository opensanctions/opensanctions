# Investigating Dataset Runs via the Data Archive

How to investigate past versions of a dataset using the data.opensanctions.org
archive — why an entity count changed, when an entity appeared or disappeared,
whether runs failed, and what a past run actually produced.

First read the module docstring of `zavod/zavod/archive/__init__.py` — it
contains the full, authoritative documentation of the archive layout: the
`/artifacts/` and `/datasets/` prefixes, the root `versions.json` file and its
`last_successful` field, how version walking works, and the `result` field
marking a run as success or failure. Everything below assumes that layout.
Version IDs are opaque strings; their one guaranteed property is that they
sort chronologically.

## Accessing files

Prefer `gsutil` against the production bucket — unlike HTTPS it can list
directories, which is often the fastest way to see what a run produced:

```
gsutil ls gs://data.opensanctions.org/artifacts/{dataset}/
gsutil ls gs://data.opensanctions.org/artifacts/{dataset}/{version}/
gsutil cat gs://data.opensanctions.org/artifacts/{dataset}/{version}/index.json
```

Only fall back to HTTPS (same paths on `https://data.opensanctions.org`, no
auth needed) if gsutil fails, e.g. for lack of credentials.

## Walking versions

`zavod.archive.iter_dataset_versions()` requires a configured archive backend
(GCS credentials or a local mirror), so for read-only investigation use plain
HTTPS with the `followthemoney` version model instead (verified to work
standalone in the workspace venv):

```python
from typing import Iterator
from urllib.error import HTTPError
from urllib.request import urlopen

from followthemoney.dataset import Version, VersionHistory

ARCHIVE = "https://data.opensanctions.org"


def iter_versions(dataset: str) -> Iterator[Version]:
    """Yield a dataset's versions newest-first, hopping through the
    versions.json snapshots in the version artifact directories."""
    url = f"{ARCHIVE}/artifacts/{dataset}/versions.json"
    seen: set[str] = set()
    while True:
        try:
            history = VersionHistory.from_json(urlopen(url).read().decode())
        except HTTPError:
            return
        new = [v for v in history.items[::-1] if v.id not in seen]
        if not new:
            return  # genesis reached
        yield from new
        seen.update(v.id for v in new)
        url = f"{ARCHIVE}/artifacts/{dataset}/{history.items[0].id}/versions.json"
```

Each yielded `Version` has `.id` and `.dt` (a `datetime`), so you can stop
walking once you're past the time window you care about.

## What to look at, cheap to detailed

1. `index.json` of past versions — `entity_count`, `target_count`, and
   `result` (`"success"`/`"failure"`). Fetch it for a range of versions to
   locate *when* a count changed or runs started failing.
2. `statistics.json` — more detail on *where* a change happened: entity
   counts by schema and by country, target counts, `last_change`.
3. `issues.json` — warnings/errors emitted during that run; usually the
   fastest way to diagnose a `"failure"` version.
4. `entities.delta.json` — *what* actually changed, entity by entity. Larger,
   but definitive. `delta.json` in the same directory indexes recent versions
   that have delta files.

Example `entities.delta.json` lines (line-based JSON; `ADD`/`MOD` carry the
full entity, `DEL` only the ID):

```json
{"op": "ADD", "entity": {"id": "NK-EXTo6dyj9d94bQbSMGLmS3", "caption": "Fly Baghdad", "schema": "LegalEntity", "datasets": ["us_ofac_sdn"], "properties": {"name": ["Fly Baghdad"], "topics": ["sanction"]}, "target": true}}
{"op": "DEL", "entity": {"id": "ofac-1ea2aa5bcbe8a0ee96f1335a86573d2c23674f95"}}
```

Archive files are often large (deltas and `statistics.json` especially) —
don't `curl` them into context. Write a small Python script that streams and
aggregates, e.g.:

```python
import json
from collections import Counter
from urllib.request import urlopen

url = f"{ARCHIVE}/artifacts/us_ofac_sdn/{version_id}/entities.delta.json"
ops: Counter[tuple[str, str]] = Counter()
for line in urlopen(url):
    row = json.loads(line)
    ops[(row["op"], row["entity"].get("schema", "?"))] += 1
print(ops.most_common(20))
```
