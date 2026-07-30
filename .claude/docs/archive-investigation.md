# Investigating Dataset Runs via the Data Archive

How to investigate past versions of a dataset using the data.opensanctions.org
archive — why an entity count changed, when an entity appeared or disappeared,
whether runs failed, and what a past run actually produced.

**This requires Google Cloud credentials.** Pass `--use-gcs` to the commands
below by default: the public site refuses some objects that do exist (old runs
of since-removed datasets answer 403, not 404), and directory listings are only
possible against the bucket. Without credentials you can still read any archive
path you know the name of over plain HTTPS, but deep history will come back
`unreadable`.

First read the module docstring of `zavod/zavod/archive/__init__.py` — it
contains the full, authoritative documentation of the archive layout: the
`/artifacts/` and `/datasets/` prefixes, the root `versions.json` file and its
`last_successful` field, how version walking works, and the `result` field
marking a run as success or failure. Everything below assumes that layout.
Version IDs are opaque strings; their one guaranteed property is that they sort
chronologically.

## Walking the run history

```bash
python -m contrib.maintenance.versions <dataset_name> -n 30 --use-gcs
```

Prints one row per archived run, newest first: version ID, export timestamp,
success/failure, entity and target counts, and the run's `index.json` URL. Add
`--schema Person --schema Company` for per-schema entity counts (read from each
run's `statistics.json`, so blank for failed runs), and `--start <version>` to
resume the walk further back than the last row printed — the tool hops the
per-run `versions.json` snapshots, so history goes back to the dataset's first
run.

## Listing what a run produced

HTTPS can't list directories, so use `gcloud storage` (not `gsutil` — it's
deprecated) against the production bucket:

```bash
gcloud storage ls gs://data.opensanctions.org/artifacts/{dataset}/{version}/
gcloud storage cat gs://data.opensanctions.org/artifacts/{dataset}/{version}/index.json
```

Every archive path is also readable over plain HTTPS without auth, at
`https://data.opensanctions.org/<same path>` — use that when you know the file
name and don't have credentials.

## Digging into a single run, cheap to detailed

1. `index.json` — `entity_count`, `target_count`, `result`. The version table
   above is just this file across many runs; read it directly for the rest of
   the run metadata (resources, `last_change`, `entry_point`).
2. `statistics.json` — where a change happened: entity counts by schema and by
   country, target counts, sanctions programs.
3. `issues.json` / `issues.log` — warnings and errors from that run; usually
   the fastest way to diagnose a `"failure"` version. For the latest run,
   `python -m contrib.maintenance.aggregate_issues <dataset_name>` groups them
   by message pattern.
4. `entities.delta.json` — *what* actually changed, entity by entity. Larger,
   but definitive. `delta.json` in the same directory indexes recent versions
   that have delta files.

Example `entities.delta.json` lines (line-based JSON; `ADD`/`MOD` carry the
full entity, `DEL` only the ID):

```json
{"op": "ADD", "entity": {"id": "NK-EXTo6dyj9d94bQbSMGLmS3", "caption": "Fly Baghdad", "schema": "LegalEntity", "datasets": ["us_ofac_sdn"], "properties": {"name": ["Fly Baghdad"], "topics": ["sanction"]}, "target": true}}
{"op": "DEL", "entity": {"id": "ofac-1ea2aa5bcbe8a0ee96f1335a86573d2c23674f95"}}
```

Archive files are often large (deltas and `statistics.json` especially) — don't
`curl` them into context. Write a small Python script that streams and
aggregates, e.g.:

```python
import json
from collections import Counter
from urllib.request import urlopen

url = f"https://data.opensanctions.org/artifacts/us_ofac_sdn/{version_id}/entities.delta.json"
ops: Counter[tuple[str, str]] = Counter()
for line in urlopen(url):
    row = json.loads(line)
    ops[(row["op"], row["entity"].get("schema", "?"))] += 1
print(ops.most_common(20))
```
