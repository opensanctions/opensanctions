---
name: debug-crawler
description: Investigate a failing crawler from an issues.json artifact URL and propose a fix. Covers fetching error details, inspecting source data via Zyte, and common failure patterns including sources that are blocked, geo-blocked, 403/429-throttled, or behind a JavaScript challenge or anti-bot protection.
argument-hint: "<issues.json URL>"
allowed-tools: Read, Edit, Glob, Grep, Bash, WebFetch
---

# Debug a Failing Crawler

The user has provided an issues.json artifact URL: $ARGUMENTS

Read `zavod/docs` as needed to understand how crawlers are normally written — the goal
here is to fix the failing crawler in accordance with existing practices, not to
refactor or standardise it.

## Step 1: Fetch the issues

Fetch the issues.json URL to understand the error:

```
WebFetch <issues.json URL>
prompt: "Show all issues, especially errors and warnings. Include full message text and any data fields."
```

Note the:
- Dataset name (e.g. `us_ne_med_exclusions`)
- Error message and traceback
- The **row data** if an assertion failed — the keys are slugified column names, values are cell contents

## Step 2: Find the crawler

```bash
# Glob datasets/**/<dataset_name>.yml
```

Read the crawler's `.yml` and `crawler.py`.

## Step 3: Inspect the current source data

The source has likely changed. Use `OPENSANCTIONS_ZYTE_API_KEY` (already set in the
environment) to fetch via Zyte when direct access times out or is blocked:

```python
python3 -c "
import requests, os
from base64 import b64decode

ZYTE_API_KEY = os.environ['OPENSANCTIONS_ZYTE_API_KEY']
url = '<data_url from .yml>'

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

## Step 4: Diagnose

Compare what the source actually contains against what the crawler expects.

### Common failures

| Symptom | Cause | Fix |
|---|---|---|
| Expected field/column not found | Source renamed or restructured columns | Update the crawler to match the new structure |
| First page parses fine, later pages fail | Per-page header handling no longer matches source | Adjust header-reading logic to match current source |
| 403 / empty response from Zyte | Source geo-restricts content | Add `geolocation=` to the fetch call |
| Assertion on entity count fails | Source grew or shrank | Verify the count is real (see recent historical index.json and statistics.json files), see what changed in recent entities.delta.json files, then update `assertions:` bounds if changes can be explained by e.g. sanctions expiring. |
| Unexpected keys in `audit_data` | New columns added to source | Pop and handle (or explicitly ignore) the new fields |

## Step 5: Fix and verify

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
