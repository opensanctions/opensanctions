---
name: debug-crawler
description: Investigate a failing crawler and propose a fix, starting from a dataset name or an issues.json artifact URL. Covers pulling the diagnostic report, inspecting source data via Zyte, and common failure patterns including sources that are blocked, geo-blocked, 403/429-throttled, or behind a JavaScript challenge or anti-bot protection.
argument-hint: "<dataset name or issues.json URL>"
allowed-tools: Read, Edit, Glob, Grep, Bash, WebFetch
---

# Debug a Failing Crawler

The user has provided a dataset name or issues.json artifact URL: $ARGUMENTS
(In an artifact URL, the dataset name is the path segment after `/artifacts/`.)

Read `zavod/docs` as needed to understand how crawlers are normally written — the goal
here is to fix the failing crawler in accordance with existing practices, not to
refactor or standardise it.

## Step 1: Get the diagnostic report

```bash
python -m contrib.maintenance.diagnose <dataset_name>
```

The report gives you the run verdict (failed since when, for how many runs), the
dataset's resolved `.yml` and crawler paths, artifact links for the latest and last
successful runs, the issues themselves (inlined, or grouped by pattern with the full
issues.json linked), an assertions-vs-last-good-statistics drift table, and recent
commits touching the dataset. Read the crawler's `.yml` and `crawler.py` from the
paths it resolves, and note the **row data** on each issue — for source-value issues
the keys are slugified column names, values are cell contents.

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

Add `'geolocation': 'US'` (or the relevant country code) to the Zyte request when
the source geo-restricts access — and add the matching `geolocation=` argument to
the `fetch_resource` / `fetch_html` call in the crawler.

If the fix is to move the crawler onto Zyte (the source is now blocked, geo-blocked,
throttled, or behind a JavaScript challenge), see
`zavod/docs/best_practices/http_operations.md` for choosing the right helper
(`fetch_html` for browser rendering, `fetch_text` / `fetch_json` / `fetch_resource`
otherwise) and remember to set `ci_test: false` on the dataset.

## Step 3: Diagnose

Compare what the source actually contains against what the crawler expects.

### Common failures

| Symptom | Cause | Fix |
|---|---|---|
| Expected field/column not found | Source renamed or restructured columns | Update the crawler to match the new structure |
| First page parses fine, later pages fail | Per-page header handling no longer matches source | Adjust header-reading logic to match current source |
| 403 / empty response from Zyte | Source geo-restricts content | Add `geolocation=` to the fetch call |
| Assertion on entity count fails | Source grew or shrank | Verify the count is real — the report's assertion table shows the drift vs the last successful run; check the linked delta.json for what changed. Update `assertions:` bounds if changes can be explained by e.g. sanctions expiring, but never widen the envelope to fit a collapsed count (that's a broken crawl, not drift). |
| Unexpected keys in `audit_data` | New columns added to source | Pop and handle (or explicitly ignore) the new fields |

## Step 4: Fix and verify

After making code changes, delete the cached source file so the fresh copy is fetched:

```bash
rm -f data/datasets/<dataset_name>/source.*

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
