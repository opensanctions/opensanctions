# Crawler Authoring Guide

For detailed API documentation, see `zavod/docs/`. This guide covers the patterns and
rules for writing crawlers — the "what to do", not exhaustive API reference.

## File layout

```
datasets/<cc>/<dataset>/
  <cc>_<dataset>.yml     # metadata
  crawler.py             # must define crawl(context: Context) -> None
```

Only create `.yml` and `crawler.py`. Do not modify collection files (`datasets/_collections/`),
CI configs, or any other existing files.

## The YAML metadata file

Full field reference: `zavod/docs/metadata.md`

```yaml
title: Full Human-Readable Title
prefix: xx-short        # unique, ≤ 10 chars, used for entity IDs
entry_point: crawler.py
coverage:
  frequency: daily      # daily | weekly | monthly | never
  schedule: "0 */2 * * *"  # cron, only for daily
  start: 2024-01-01
load_statements: true   # always true for production datasets
ci_test: false          # true only for fast, small datasets

summary: >-
  One sentence description.

description: |
  Multi-paragraph description.

url: https://...

publisher:
  name: Full Publisher Name
  acronym: ABBR
  description: >
    Who they are.
  url: https://...
  country: us
  official: true

data:
  url: https://...      # most direct URL to data
  format: XML           # XML | JSON | HTML | CSV | XLS | XLSX
  lang: eng             # ISO 639-3

dates:
  formats: ["%d/%m/%Y", "%Y-%m-%d"]
  months:
    "january": "janvier"

tags:
  - list.sanction       # or list.pep — see type-specific skills

assertions:
  min:
    schema_entities:
      Person: 1000
  max:
    schema_entities:
      Person: 5000
```

Key rules:
- Always set `load_statements: true`.
- Set assertions to ~80% min / ~150% max of expected counts. Checked by
  `zavod validate`, not by `zavod crawl`. See the "Data assertions" section of
  `zavod/docs/metadata.md` for the metrics and comparison semantics.
- Write `summary` and `description` to be **time-agnostic** — describe the source's
  purpose and update cadence, not the current snapshot (e.g. "Members of parliament,
  updated after each election" not "Members since the 2023 elections").

## The crawler module

```python
from zavod import Context
from zavod import helpers as h

def crawl(context: Context) -> None:
    ...
```

`context.data_url` is from `data.url` in the YAML.

**Strict interpretation.** Every source value within the crawler's scope is handled, explicitly ignored, or raises a signal — crash or warn on unexpected data, never silently emit ambiguous output. See `zavod/docs/best_practices/strict_interpretation.md`.

### Fetching data

See also: `zavod/docs/best_practices/caching.md`

```python
doc = context.fetch_html(context.data_url, absolute_links=True)
data = context.fetch_json(url, params={"page": 1}, cache_days=7)

from rigour.mime.types import XML
path = context.fetch_resource("source.xml", url)
context.export_resource(path, XML, title=context.SOURCE_TITLE)
doc = context.parse_resource_xml(path)
```

`cache_days=1` for index pages, `cache_days=7` for detail pages.

### JS-rendered pages and anti-bot protection

If the source is a JS-rendered SPA, look for API endpoints in the page source or JS
bundles first — a clean JSON endpoint is almost always simpler than rendering. If the
source requires JS rendering or anti-bot protection that prevents `context.fetch_*` from
working, route the request through the Zyte API (`zavod.extract.zyte_api`). See
`zavod/docs/best_practices/http_operations.md` for which helper to use (`fetch_html` for
browser rendering, `fetch_text` / `fetch_json` / `fetch_resource` otherwise) and how to
handle geo-blocking. Crawlers that use Zyte set `ci_test: false`, since the API key isn't
available in CI.

### Creating and emitting entities

```python
entity = context.make("Person")
entity.id = context.make_slug(row["id"])  # or make_slug("person", row["id"]) if the source reuses IDs across entity types
entity.add("name", row["name"])
h.apply_date(entity, "birthDate", row["dob"])
context.emit(entity)
```

- `entity.add()` skips `None` and empty strings — never guard with `if value:`.
- See `zavod/docs/best_practices/entity_id.md` for the ID design rules; the two hard rules to remember:
    - **Never put PII into `make_slug`.**
    - **Never change entity IDs** of existing crawlers without team coordination.
- **Never emit** an entity without setting `.id`.

### Audit and fail fast

See also: `zavod/docs/best_practices/patterns.md`

Use `dict.pop()` to consume fields, then `context.audit_data()` at the end.
**Do not pop fields just to discard them** — pass unused fields in `ignore`:

```python
# Good
context.audit_data(record, ignore=["photo_url", "internal_id"])

# Bad — hides fields from audit
record.pop("photo_url", None)
context.audit_data(record)
```

## FTM Schemata

Discover available schemata and properties with the `ftm ref` command group (see the project
`CLAUDE.md`) — e.g. `ftm ref schema Person`, `ftm ref prop Person:nationality`. This is
the authoritative, always-current view of the model; don't rely on remembered property lists.
Captured output is always JSON, so pipe to `jq` to extract fields.

A few usage notes that the model itself doesn't capture:

- **dates**: always set via `h.apply_date()` — never assign raw strings.
- **names**: fingerprinted for dedup, so add all known forms including aliases.
- **topics**: values like `sanction`, `role.pep`, `role.rca` drive downstream logic.

## Helpers (`from zavod import helpers as h`)

Full reference: `zavod/docs/best_practices/patterns.md`, `zavod/docs/best_practices/dates_meta.md`

### Names

```python
h.apply_name(entity, first_name=first, last_name=last, lang="eng", alias=False)
full = h.make_name(full=row.get("fullname"), first_name=first, last_name=last)
```

Use `h.apply_name()` for Person entities. For name-only: `entity.add("name", ...)`.

### Dates

```python
h.apply_date(entity, "birthDate", raw_string)
h.apply_dates(entity, "birthDate", [date1, date2])
```

**Never** add raw date strings directly. For dates in free text, extract with regex first.

### Addresses

```python
addr = h.make_address(context, street=..., city=..., country=...)
h.copy_address(entity, addr)
```

See `zavod/docs/best_practices/addresses.md` for the full pattern and the choice between `copy_address` (default) and `apply_address` (legacy).

### HTML/XML parsing

See also: `zavod/docs/best_practices/xpath_and_html.md`

```python
from lxml import etree, html

el = h.xpath_element(doc, ".//table[@id='data']")
text = h.xpath_string(el, "./td[2]/text()")
rows = h.parse_html_table(table_el)
h.remove_namespace(doc)
```

### Text and spreadsheets

```python
parts = h.multi_split(text, [";", ",", " / "])
clean = h.remove_bracketed(text)

from openpyxl import load_workbook
wb = load_workbook(path, read_only=True)
for row in h.parse_xlsx_sheet(context, wb["Sheet1"]):
    name = row.pop("full_name")
```

## Lookups

Full reference: `zavod/docs/best_practices/datapatch_lookups.md` — the YAML structure, matching modes, result fields, and the property-name → type-lookup mapping.

```yaml
lookups:
  type.country:
    lowercase: true
    options:
      - match: "united states"
        value: us
      - match: "unknown"
        value: null

  type.gender:
    normalize: true
    options:
      - match: [m, male]
        value: male
```

**`type.*` lookups are applied automatically** by `entity.add()`. Only call
`context.lookup_value()` explicitly when you need the result before adding (to branch
or build an ID), or for non-`type.*` lookups (e.g. `sanction.program`).

**Strategy**: Start without lookups. Run the crawler, note warnings, then add entries.

## Common patterns

### RCAs (Relatives and Close Associates)

```python
rca = context.make("Person")
rca.id = context.make_id(person.id, role, name)
rca.add("name", name)
rca.add("topics", "role.rca")
link = context.make("Family")
link.id = context.make_id(person.id, "family", name)
link.add("person", person)
link.add("relative", rca)
link.add("relationship", role)
```

### Paginated JSON APIs

```python
from itertools import count

for page in count(1):
    data = context.fetch_json(url, params={"page": page, "size": 50}, cache_days=1)
    for record in data.get("records", []):
        crawl_record(context, record)
    if page >= data["_metadata"]["totalPages"]:
        break
```

### Non-Latin script names

`data.lang` sets the default language tag. Only specify `lang=` when the value differs:

```python
entity.add("name", arabic_name)                    # uses data.lang default
entity.add("name", latin_name, lang="eng")         # override
```

## Code style

- Full type annotations. Use `str | None` not `Optional[str]`.
- Use `lxml`, never BeautifulSoup or stdlib xml.
- Use `context.fetch_*`, never `requests`.
- Use `rigour.mime.types` constants, never string literals.
- Keep `crawl()` thin; delegate to per-record helpers.

## Running and testing

See also: `zavod/docs/usage.md`

```bash
zavod crawl datasets/xx/foo/xx_foo.yml
# Output: data/datasets/xx_foo/
```

For a dataset already deployed, `python -m contrib.maintenance.diagnose <name>` prints
its production runtime state: run verdict, artifact links, current issues, assertion
drift.

Check `issues.log` for errors, then spot-check with qsv:

```bash
qsv count data/datasets/xx_foo/statements.pack
qsv frequency -s prop --limit 30 data/datasets/xx_foo/statements.pack
qsv search -s prop "^Person:id$" data/datasets/xx_foo/statements.pack | qsv count
```

## Before merging

See `zavod/docs/best_practices/merge_checklist.md` for the review criteria the team applies to new crawlers, and `zavod/docs/best_practices/priorities.md` for the Essential / Should / Could / Won't framing that governs what attributes a crawler should extract.
