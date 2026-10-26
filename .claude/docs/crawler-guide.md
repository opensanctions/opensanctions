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
  frequency: daily      # house defaults per dataset type: see metadata.md
  start: 2024-01-01
load_statements: true

summary: >-
  See "Summary" in zavod/docs/metadata.md.

description: |
  See "Description" in zavod/docs/metadata.md.

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
- Assertions: see "Data assertions" in `zavod/docs/metadata.md`.
- `title`, `summary` and `description` conventions live in `zavod/docs/metadata.md`.

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
data = context.fetch_json(detail_url, cache_days=7)

from rigour.mime.types import XML
path = context.fetch_resource("source.xml", url)
context.export_resource(path, XML, title=context.SOURCE_TITLE)
doc = context.parse_resource_xml(path)
```

### JS-rendered pages and anti-bot protection

Before reaching for rendering or Zyte, do the recon in
`zavod/docs/best_practices/source_recon.md`. If the source still requires JS rendering or
anti-bot protection that prevents `context.fetch_*` from working, route the request
through the Zyte API (`zavod.extract.zyte_api`). See
`zavod/docs/best_practices/http_operations.md` for which helper to use (`fetch_html` for
browser rendering, `fetch_text` / `fetch_json` / `fetch_resource` otherwise) and how to
handle geo-blocking.

### Creating and emitting entities

```python
entity = context.make("Person")
entity.id = context.make_slug(row["id"])  # or make_slug("person", row["id"]) if the source reuses IDs across entity types
entity.add("name", row["name"])
h.apply_date(entity, "birthDate", row["dob"])
context.emit(entity)
```

- Entity IDs require careful planning; see `zavod/docs/best_practices/entity_id.md`.
- **Never emit** an entity without setting `.id`.

### Audit and fail fast

See `zavod/docs/best_practices/strict_interpretation.md`.

## Helpers (`from zavod import helpers as h`)

Full reference: `zavod/docs/best_practices/patterns.md`, `zavod/docs/best_practices/dates_meta.md`

### Names

```python
h.apply_name(entity, first_name=first, last_name=last, lang="eng", alias=False)
full = h.make_name(full=row.get("fullname"), first_name=first, last_name=last)
```

To strip honorifics and post-nominals (`Hon.`, `Dr.`, `, MP`) from source names, see `zavod/docs/best_practices/name_titles.md`.

### Dates

```python
h.apply_date(entity, "birthDate", raw_string)
h.apply_dates(entity, "birthDate", [date1, date2])
```

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

from python_calamine import CalamineWorkbook
wb = CalamineWorkbook.from_path(path)
for row in h.parse_xlsx_sheet(context, wb.get_sheet_by_name("Sheet1")):
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

## Common patterns

### RCAs (Relatives and Close Associates)

See the relatives and close associates section of `zavod/docs/peps/index.md`.

### Paginated JSON APIs

```python
from itertools import count

for page in count(1):
    data = context.fetch_json(url, params={"page": page, "size": 50})
    for record in data["records"]:
        crawl_record(context, record)
    if page >= data["_metadata"]["totalPages"]:
        break
```

## Running and testing

See also: `zavod/docs/usage.md`

```bash
zavod crawl datasets/xx/foo/xx_foo.yml
zavod export datasets/xx/foo/xx_foo.yml          # runs the dataset validators and assertions
contrib/lint_dataset.sh datasets/xx/foo/xx_foo.yml  # ruff + mypy + pre-commit as CI runs them
```

Check the run's issues, then spot-check its statements with qsv:

```bash
P=$(ls data/datasets/xx_foo/_artifacts/*/statements.pack | tail -1)
qsv count "$P"
qsv frequency -s prop --limit 30 "$P"
qsv search -s prop "^Person:id$" "$P" | qsv count
```

## Before merging

See `zavod/docs/best_practices/merge_checklist.md` for the review criteria the team applies to new crawlers, and `zavod/docs/best_practices/priorities.md` for the Essential / Should / Could / Won't framing that governs what attributes a crawler should extract.

A PR that adds a new crawler MUST carry the `new-source` GitHub label, and its title takes the house `[dataset_name]` prefix:

```bash
gh pr create --label new-source --title "[xx_foo] Add ... crawler"
```

Apply the label to an existing PR with `gh pr edit <number> --add-label new-source`.
