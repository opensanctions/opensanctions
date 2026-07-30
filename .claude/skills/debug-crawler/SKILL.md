---
name: debug-crawler
description: Investigate a failing crawler and propose a fix, starting from a dataset name or an issues.json artifact URL. Covers pulling the diagnostic report, inspecting source data via Zyte, and common failure patterns including sources that are blocked, geo-blocked, 403/429-throttled, or behind a JavaScript challenge or anti-bot protection.
argument-hint: "<dataset name or issues.json URL>"
allowed-tools: Read, Edit, Glob, Grep, Bash, WebFetch
---

# Debug a Failing Crawler

The user has provided a dataset name or issues.json artifact URL: $ARGUMENTS
(In an artifact URL, the dataset name is the path segment after `/artifacts/`.)

Fix the failing crawler. Do not refactor or standardise it.
`.claude/docs/crawler-guide.md` is the hub for how crawlers are normally written, and
links the relevant `zavod/docs` best-practice guides.

## Step 1: Get the diagnostic report

```bash
python -m contrib.maintenance.diagnose <dataset_name>
```

Read the crawler's `.yml` and `crawler.py` from the paths the report resolves. Note
the **row data** on each issue — for source-value issues the keys are slugified
column names, values are cell contents.

The report only covers the latest run and the last successful one. For an overview of
what the crawler produced over time — when counts moved, since when runs have been
failing, when a schema appeared — walk the archived history:

```bash
python -m contrib.maintenance.versions <dataset_name> -n 30
```

`.claude/docs/archive-investigation.md` covers digging into individual past runs from
there.

## Step 2: Inspect the current source data

The source has likely changed. Use `OPENSANCTIONS_ZYTE_API_KEY` (already set in the
environment) to fetch via Zyte when direct access times out or is blocked:

```python
python3 -c "
import requests, os
from base64 import b64decode

ZYTE_API_KEY = os.environ['OPENSANCTIONS_ZYTE_API_KEY']
url = '<the Source data URL from the diagnostic report>'

resp = requests.post(
    'https://api.zyte.com/v1/extract',
    auth=(ZYTE_API_KEY, ''),
    json={'url': url, 'httpResponseBody': True, 'httpResponseHeaders': True},
    timeout=60
)
resp.raise_for_status()
content = b64decode(resp.json()['httpResponseBody'])
# then parse content as appropriate for the source format
"
```

If the fix is to move the crawler onto Zyte (the source is now blocked, geo-blocked,
throttled, or behind a JavaScript challenge), see
`zavod/docs/best_practices/http_operations.md` for choosing the right helper
(`fetch_html` for browser rendering, `fetch_text` / `fetch_json` / `fetch_resource`
otherwise) and set `ci_test: false` on the dataset.

## Step 3: Diagnose

Compare what the source actually contains against what the crawler expects.

### Common failures

| Symptom | Cause | Fix |
|---|---|---|
| Expected field/column not found | Source renamed or restructured columns | Update the crawler to match the new structure |
| First page parses fine, later pages fail | Per-page header handling no longer matches source | Adjust header-reading logic to match current source |
| 403 / empty response from Zyte | Source geo-restricts content | Add `'geolocation': 'US'` (or the relevant country code) to the Zyte request, and the matching `geolocation=` to the crawler's `fetch_resource` / `fetch_html` call |
| Assertion on entity count fails | Source grew or shrank | Verify the count is real — the report's assertion table shows the drift vs the last successful run; check the linked delta.json for what changed. Update `assertions:` bounds if changes can be explained by e.g. sanctions expiring, but never widen the envelope to fit a collapsed count (that's a broken crawl, not drift). |
| Unexpected keys in `audit_data` | New columns added to source | Pop and handle (or explicitly ignore) the new fields |

## Step 4: Fix and verify

```bash
zavod crawl datasets/<path>/<dataset_name>.yml
```

Check `data/datasets/<dataset_name>/issues.log` for remaining warnings. Then export
and confirm the delta is plausible:

```bash
zavod export datasets/<path>/<dataset_name>.yml
```

A healthy run shows:
- No errors in the crawl log
- Delta (added/deleted/modified) consistent with elapsed time since the last run
- Entity counts within the `assertions:` bounds in the `.yml`
