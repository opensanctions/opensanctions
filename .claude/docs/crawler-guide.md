# Crawler Authoring Guide

This directory contains dataset crawlers that ingest structured/semi-structured government data
into FollowTheMoney (FTM) entities. Each dataset lives in a subdirectory like
`datasets/<country_code>/<dataset_name>/` and consists of a YAML metadata file and a Python
crawler. The dataset `name` is derived from the YAML file's stem (e.g. `us_ofac_sdn`).

## File layout

```
datasets/us/ofac_sdn/
  us_ofac_sdn.yml        # metadata, schedule, lookups, assertions
  crawler.py             # entry point must define crawl(context: Context) -> None
```

When creating a new crawler, only create the `.yml` and `crawler.py` files inside the
dataset directory. Do not modify collection files (`datasets/_collections/`), CI configs,
or any other existing files in the repository.

## The YAML metadata file

```yaml
title: Full Human-Readable Title
prefix: xx-short        # 2-3 char country code + dataset abbreviation; used for entity IDs
entry_point: crawler.py
disabled: false         # omit when false
coverage:
  frequency: daily      # daily | weekly | monthly | never
  schedule: "0 */2 * * *"  # cron, only for daily
  start: 2024-01-01
load_statements: true   # always true for production datasets
ci_test: false          # set true only for fast, small datasets to run in CI

summary: >-             # one-line, shown in search results
  One sentence description.

description: |          # multi-paragraph, markdown OK
  Detailed description of the source and what it contains.

url: https://...        # canonical homepage for the dataset

publisher:
  name: Full Publisher Name
  acronym: ABBR         # optional
  description: >
    Who they are and why their data matters.
  url: https://...
  country: us           # ISO 2-letter
  official: true        # true if the publisher is the issuing authority

data:
  url: https://...      # direct download or landing page URL
  format: XML           # XML | JSON | HTML | CSV | XLS | XLSX
  lang: eng             # ISO 639-3 language code of source data

dates:
  formats: ["%d/%m/%Y", "%Y-%m-%d"]   # strptime formats for this source
  months:                              # translate non-English month names to English
    "january": "janvier"              # key = English name, value = foreign form(s)
    "february": "février"
    "march": ["mars", "marz"]         # list when multiple spellings exist

tags:
  - list.sanction       # or list.pep — see type-specific skills

assertions:             # bounds checked by `zavod validate`
  min:
    schema_entities:
      Person: 1000
      Organization: 200
    country_entities:
      us: 100
  max:
    schema_entities:
      Person: 5000
```

Key rules:
- `prefix` must be unique across all datasets and ≤ 10 chars.
- Set `ci_test: true` only if the crawler is fast and fetches a small amount of data.
- Always set `load_statements: true` for production datasets.
- `data.url` should be the most direct URL to the actual data file if possible.
- Set assertions to ~80% min / ~150% max of expected counts initially. Tighten after
  the crawler has been running in production for a few weeks. Assertions are checked by
  `zavod validate datasets/xx/foo/xx_foo.yml`, not by `zavod crawl`.
- Write `summary` and `description` to be **time-agnostic and self-renewing** — describe
  what the source publishes and its update cadence, not the current snapshot. The crawler
  re-runs on a schedule, so descriptions tied to a specific year, election, or sanctions
  episode will become stale.

  ```
  Bad:  "Members of parliament since the 2023 elections."
  Good: "Members of parliament. Data updated after each communal election (every 6 years)."

  Bad:  "Sanctions imposed in response to the 2022 invasion of Ukraine."
  Good: "Sanctions maintained by X authority targeting Russian individuals and entities."
  ```

## The crawler module

Every crawler must define:

```python
from zavod import Context
from zavod import helpers as h

def crawl(context: Context) -> None:
    ...
```

`context.data_url` is the URL from `data.url` in the YAML. Use `context.SOURCE_TITLE` as
the title when exporting resources.

**Fail loudly.** When a crawler encounters unexpected data it cannot interpret with
confidence, it must crash or emit a warning — never silently emit ambiguous output.
Use `assert`, `raise ValueError(...)`, or `context.log.warning(...)` for known-benign
edge cases. This principle applies everywhere: ID generation, field parsing, lookups,
and entity construction.

### Fetching data

```python
# HTML page (returns lxml element tree)
doc = context.fetch_html(context.data_url, absolute_links=True)

# JSON API
data = context.fetch_json(url, params={"page": 1}, cache_days=7)

# Download a file and cache it locally
from rigour.mime.types import XML, ZIP, CSV, XLSX
path = context.fetch_resource("source.xml", url)
context.export_resource(path, XML, title=context.SOURCE_TITLE)
doc = context.parse_resource_xml(path)

# Plain text
text = context.fetch_text(url, cache_days=1)
```

`cache_days` controls how long responses are cached. Use `cache_days=1` for index pages,
`cache_days=7` for individual record pages that change rarely.

### JS-rendered pages and anti-bot protection

If the source is a JS-rendered page (React, Next.js, etc.), first look for API endpoints
in the page source or JS bundle chunks before attempting HTML parsing. Many SPAs have
public API endpoints that return structured JSON.

If the source requires JavaScript rendering or has anti-bot protection that prevents
`context.fetch_*` from working, **stop and warn the user**. These sources require the
Zyte API (`zavod.extract.zyte_api`), which needs manual setup and testing. Do not
attempt to implement Zyte-based crawlers autonomously.

### Creating and emitting entities

```python
entity = context.make("Person")            # schema name (see FTM Schemata below)
entity.id = context.make_slug("person", row["id"])   # prefix-based slug ID
# OR
entity.id = context.make_id(first_name, last_name, birth_date)  # hash-based ID

entity.add("name", row["name"])
h.apply_date(entity, "birthDate", row["dob"])  # never add dates with .add() directly
context.emit(entity)
```

`entity.add()` silently skips `None` and empty strings — never guard it with `if value:`.

**ID generation:**
- `context.make_slug(*parts)` — stable, human-readable IDs using the dataset prefix.
  Use when the source provides a stable unique identifier (e.g. `gb-fcdo-123456`).
- `context.make_id(*parts)` — SHA1-hashed ID with dataset prefix.
  Use when composing an ID from multiple fields. More stable against source ID changes.

**Never put PII into `make_slug`** — person names, dates of birth, passport numbers, and
similar personal data must not appear in entity IDs. IDs are logged, indexed, and exposed in
APIs. Use the source's own opaque identifier (a row number, a registry code, an internal ID)
in slugs. When no such identifier exists, use `make_id` which hashes its inputs.

```python
# Good — source-assigned opaque ID in slug
entity.id = context.make_slug("person", row["id"])

# Bad — name and DOB are PII; they also change, breaking downstream links
entity.id = context.make_slug(first_name, last_name, birth_date)  # NEVER
```

**Do not change entity IDs of existing crawlers** without explicit agreement. IDs are the
stable key used by downstream enrichment, deduplication, and consumers. Changing them
silently breaks those links. If a source changes its own identifiers and a migration is
unavoidable, document it clearly in the PR and coordinate with the team.

**Never** emit an entity without setting `.id`.

### Audit and fail fast

```python
context.audit_data(row, ignore=["irrelevant_field"])
# Logs a warning for any key in `row` that wasn't popped/consumed.

assert condition, "Descriptive message"
# Prefer crashing over silently emitting wrong data.
```

Use `dict.pop()` to consume fields as you read them, then call `context.audit_data()` at
the end to catch unhandled fields — this surfaces new fields when the source changes.

**Do not pop fields just to discard them.** If a field is not used, pass it in the `ignore`
list instead:

```python
# Good — unused fields are documented in one place
context.audit_data(record, ignore=["photo_url", "internal_id"])

# Bad — silently discarding fields hides them from audit
record.pop("photo_url", None)
record.pop("internal_id", None)
context.audit_data(record)
```

## FTM Schemata

Documentation: https://followthemoney.tech/explorer/schemata/

### Core schemata

**Person** — a natural person
```
name, firstName, lastName, middleName, patronymic, fatherName, motherName
title, nameSuffix, alias, weakAlias
birthDate, birthPlace, birthCountry, deathDate
nationality, citizenship, passportNumber, idNumber
gender, position, education, political, religion
phone, email, website, address
notes, summary, description
innCode, taxNumber, registrationNumber
```

**Organization** — a non-person legal entity
```
name, alias, weakAlias
incorporationDate, dissolutionDate, jurisdiction, legalForm
registrationNumber, taxNumber, vatCode, innCode, ogrnCode, leiCode
email, phone, website, address
notes, summary, description
```

**LegalEntity** — base schema for both Person and Organization; use when type is unknown.

**Address** — a location
```
full, street, street2, city, postalCode, region, country, poBox
```

**Family** — a family relationship between two persons
```
person, relative, relationship (e.g. "spouse", "father", "mother")
```

**Ownership** — ownership link between two entities
```
owner (LegalEntity), asset (LegalEntity)
startDate, endDate, percentage, status
```

### Property types

See https://followthemoney.tech/explorer/types/ for the full list. Key behaviours:
- **country**: Accepts ISO 2-letter codes and many country name variants. Use the `type.country`
  lookup for unusual values from the source.
- **date**: Normalised to ISO prefixes (YYYY, YYYY-MM, YYYY-MM-DD). Always use `h.apply_date()`.
- **name**: Will be fingerprinted for deduplication. Add all known name forms including aliases.
- **topics**: Add values like `sanction`, `role.pep`, `role.rca` to drive downstream logic.

## Helpers (`from zavod import helpers as h`)

### Names

```python
# Build a full name from parts (returns None if all parts are None)
full = h.make_name(
    full=row.get("fullname"),     # preferred over generating from parts
    first_name=row.get("first"),
    last_name=row.get("last"),
    patronymic=row.get("patronymic"),
)

# Apply name parts to a Person entity (also sets firstName/lastName etc.)
h.apply_name(
    entity,
    first_name=first,
    last_name=last,
    patronymic=patronymic,
    title=title,
    lang="eng",     # optional BCP47 lang tag
    alias=False,    # True to add as alias instead of primary name
)
```

Use `h.apply_name()` for Person entities. For name-only situations use `entity.add("name", ...)`.

### Dates

```python
# Parse and add a date to an entity (uses dataset's dates.formats + dates.months)
h.apply_date(entity, "birthDate", raw_string)
h.apply_dates(entity, "birthDate", [date1, date2])

# Low-level parse — returns a PrefixDate object
parsed = h.parse_formats(text, ["%d/%m/%Y", "%Y-%m-%d"])
if parsed.text:
    entity.add("birthDate", parsed.text)
```

`h.apply_date()` automatically applies the `dates.months` translation from the dataset YAML
before parsing, so non-English month names are handled transparently.

**Never** add raw date strings directly to entities. Always go through `h.apply_date()`.

**Dates embedded in free-text fields**: when the date is buried in a prose paragraph
(e.g. `"Born 8 July 1955 in Ljubljana."`), use a regex to extract just the date portion
first, then pass the extracted substring to `h.apply_date()`. Do not pass the full
paragraph — `parse_formats` requires the whole string to match the format.

```python
import re

# Extract the date substring — the surrounding prose is discarded
DOB_RE = re.compile(r"Born\s+(.+?\d{4})", re.IGNORECASE)

def parse_birth_date(text: str | None) -> str | None:
    if not text:
        return None
    m = DOB_RE.search(text)
    return m.group(1) if m else None   # e.g. "8 July 1955"

# Later, in crawl_person:
h.apply_date(person, "birthDate", parse_birth_date(raw_bio))
# dates.months in the YAML handles non-English month names transparently
```

### Addresses

```python
# Create an Address entity and link it to a LegalEntity
addr = h.make_address(
    context,
    street=row.get("address1"),
    street2=row.get("address2"),
    city=row.get("city"),
    postal_code=postcode,
    po_box=pobox,
    region=row.get("state"),
    country=row.get("country"),
)
h.copy_address(entity, addr)   # emits addr and adds address prop to entity

# Or for simple cases where you don't need the Address entity:
h.apply_address(entity, ...)   # sets entity.address as formatted string only

# Split a postal code field that may contain a PO box
postcode, pobox = h.postcode_pobox(raw_postal_code)
```

### HTML/XML parsing

```python
from lxml import etree, html

# XPath helpers
links = h.xpath_strings(doc, ".//a[@class='download']/@href")
el = h.xpath_element(doc, ".//table[@id='sanctions']")
text = h.xpath_string(el, "./td[2]/text()")

# Parse HTML table into list of dicts
rows = h.parse_html_table(table_el)   # header row auto-detected
for row in rows:
    name = h.cells_to_str(row).get("Name")

# Strip XML namespaces (simplifies XPath)
h.remove_namespace(doc)

# Extract text with whitespace normalisation
text = h.element_text(el, squash=True)
```

### Text utilities

```python
# Split on multiple separators
parts = h.multi_split(text, [";", ",", " / "])

# Remove bracketed content: "Smith (a.k.a. Jones)" -> "Smith"
clean = h.remove_bracketed(text)

# Check if value is empty/null-like
if h.is_empty(value):
    continue
```

### Spreadsheets (XLS/XLSX)

```python
from openpyxl import load_workbook

wb = load_workbook(path, read_only=True)
for row in h.parse_xlsx_sheet(context, wb["Sheet1"]):
    # row is a dict of slugified header -> cell value
    # Headers are slugified: "Full Name" -> "full_name", "Date of Birth" -> "date_of_birth"
    # datetime cells are converted to date strings automatically
    name = row.pop("full_name")
```

When the source uses a placeholder like `"n/a"` for empty fields, pre-clean the whole row
at the start of the record handler rather than wrapping every field individually:

```python
def crawl_row(context: Context, row: dict) -> None:
    row = {k: (None if isinstance(v, str) and v.strip().lower() == "n/a" else v)
           for k, v in row.items()}
    ...
```

## Lookups

Lookups let the YAML file override how specific source values are mapped to entity properties.
This keeps data decisions in config rather than code.

```yaml
lookups:
  type.country:           # name is referenced in code as context.lookup("type.country", val)
    lowercase: true       # normalise match values to lowercase before comparing
    options:
      - match: "united states"
        value: us
      - match:
          - "great britain"
          - "england"
        value: gb
      - match: "unknown"
        value: null       # explicitly null: skip/discard this value

  type.gender:
    normalize: true       # strip accents, lowercase before matching
    options:
      - match: [m, male, "1"]
        value: male
      - match: [f, female, "2"]
        value: female

  # Multi-value lookup: one match -> multiple values
  type.name:
    options:
      - match: "Smith, John"
        values:           # use `values` (plural) for lists
          - "John Smith"
          - "J. Smith"
      - match: "."        # garbage input
        value: null
```

### Automatic application via `entity.add()`

Each FTM property has a **type** (e.g. `country`, `gender`, `name`) that may be shared
across many properties. For example, `jurisdiction`, `nationality`, `citizenship`, and
`birthCountry` all have type `country`. A lookup named `type.country` therefore rewrites
values for *all* of those properties, not just one named `country`.

**`type.*` lookups are applied automatically inside every `entity.add()` call** — you do
not need to call `context.lookup_value()` first. Before FTM normalises a value,
`zavod` checks whether the dataset has a `type.<typename>` lookup for that property's type
and rewrites the raw value if there is a match. So a `type.gender` lookup with
`Z -> female` means you can write:

```python
entity.add("gender", raw_value)   # "Z" is transparently rewritten to "female"
```

Only call `context.lookup_value()` / `context.lookup()` explicitly when:
- you need the looked-up value **before** adding it to an entity (e.g. to branch on it,
  build an ID, or skip a record), or
- the lookup is keyed by something other than an FTM type name (e.g. `sanction.program`,
  or any other custom key — these are never applied automatically).

```python
# Needed: branching on the result to skip an entity
schema = context.lookup_value("type.entity", raw_type)
if schema is None:
    return
entity = context.make(schema)

# Not needed: type.gender is applied automatically by entity.add()
entity.add("gender", raw_gender)   # rewritten via type.gender lookup if defined
```

When a value is not matched and no `warn_unmatched` argument is set, the lookup silently
returns `None` (the `entity.add()` call will then pass the raw value to FTM's own
normaliser). Add `warn_unmatched=True` to surface gaps in your lookup table.

**Strategy**: Start without lookups. Run the crawler, note warnings about unmatched values,
then add lookup entries to handle each case. This is the preferred way to document data
cleaning decisions.

## Common patterns

### Relatives and Close Associates (RCAs)

```python
def emit_rca(context: Context, person: Entity, name: str | None, role: str) -> None:
    if name is None or not name.strip():
        return
    rca = context.make("Person")
    rca.id = context.make_id(person.id, role, name)
    rca.add("name", name)
    rca.add("topics", "role.rca")
    context.emit(rca)

    link = context.make("Family")
    link.id = context.make_id(person.id, "family", name)
    link.add("person", person)
    link.add("relative", rca)
    link.add("relationship", role)
    context.emit(link)
```

### Ownership relationships

```python
owner = context.make("Organization")
owner.id = context.make_slug("named", owner_name)
owner.add("name", owner_name)
context.emit(owner)

ownership = context.make("Ownership")
ownership.id = context.make_id(owner.id, "owns", entity.id)
ownership.add("owner", owner)
ownership.add("asset", entity)
context.emit(ownership)
```

### Paginated JSON APIs

```python
from itertools import count

for page in count(1):
    data = context.fetch_json(url, params={"page": page, "size": 50}, cache_days=1)
    records = data.get("records", [])
    for record in records:
        crawl_record(context, record)
    if page >= data["_metadata"]["totalPages"]:
        break
```

### ZIP archives containing XML

```python
from zipfile import ZipFile
from rigour.mime.types import ZIP

path = context.fetch_resource("source.zip", url)
context.export_resource(path, ZIP, title=context.SOURCE_TITLE)
with ZipFile(path) as zf:
    for name in zf.namelist():
        if name.lower().endswith(".xml"):
            with zf.open(name) as fh:
                doc = etree.parse(fh)
                # process doc...
```

### Non-Latin script names

`data.lang` in the YAML sets the default language tag for all `entity.add()` calls in that
crawler. Only specify `lang=` explicitly when the value differs from the source language —
for example when adding a Latin-script alias to an Arabic-script crawler:

```python
# data.lang: ara — no lang= needed for Arabic values (it's the default)
entity.add("name", arabic_name)

# Override only when the value is in a different script/language
entity.add("name", latin_name, lang="eng")
h.apply_name(entity, arabic_name, lang="ara", alias=True)
```

### Entity type dispatch

```python
def get_schema(type_str: str) -> str:
    match type_str.lower():
        case "individual":
            return "Person"
        case "entity" | "company":
            return "Organization"
        case "vessel" | "ship":
            return "Vessel"
        case _:
            raise ValueError(f"Unknown entity type: {type_str!r}")
```

## Code style and quality

- Full type annotations on all functions. Use built-in container types (`list`, `dict`, `set`,
  `tuple`) not `typing.List` etc. Use `str | None` instead of `Optional[str]` —
  `typing.Optional` is deprecated in favour of the `X | None` union syntax (PEP 604).
- Use `lxml` for HTML/XML. Never use BeautifulSoup or `xml.etree.ElementTree`.
- Use `context.fetch_*` methods. Never use `requests` directly.
- Use `rigour.mime.types` constants for MIME types, never string literals.
- Keep the `crawl()` function thin; delegate to per-record helper functions.
- A crawler should be runnable multiple times: `zavod crawl datasets/xx/foo/xx_foo.yml`.
  Cached resources avoid repeated downloads.

## Running and testing

```bash
# Run a crawler (from repo root, with venv active)
zavod crawl datasets/xx/foo/xx_foo.yml

# Output written to data/datasets/xx_foo/
# Check issues:
python3 -c "import json; d=json.load(open('data/datasets/xx_foo/issues.json')); print(d['issues'])"
```

After a crawl, check:
1. Spot-check the data using `statements.pack` (see below).
2. `issues.json` — any errors or warnings?
3. All fields from the source are either used or explicitly ignored in `audit_data`.

### Analysing output with `statements.pack`

`zavod crawl` writes `data/datasets/<name>/statements.pack`, a CSV file (despite the
`.pack` extension) containing every statement emitted. Each row is one property value:

| column | meaning |
|---|---|
| `entity_id` | entity ID (e.g. `si-dzrs-mdb-p020`) |
| `prop` | schema-qualified property (e.g. `Person:birthDate`) |
| `value` | normalised value |
| `original_value` | raw value before normalisation (e.g. `"8. julija 1955"`) |
| `dataset` | dataset name |
| `first_seen` / `last_seen` | ISO timestamps from this run |

The installed `qsv` tool can query it directly. Run each check as a **separate** command
(shell state does not persist between tool calls, so shell variable assignments like
`PACK=...` must be in the same call as the command that uses them — it is cleaner to
inline the path each time):

```bash
# Total statement count
qsv count data/datasets/xx_foo/statements.pack

# All props emitted and how often (replace --limit as needed)
qsv frequency -s prop --limit 30 data/datasets/xx_foo/statements.pack

# Count of a specific entity type (e.g. all Person entities)
qsv search -s prop "^Person:id$" data/datasets/xx_foo/statements.pack | qsv count

# Distribution of a property's values (e.g. gender, status, topics)
qsv search -s prop "^Person:gender$" data/datasets/xx_foo/statements.pack | qsv frequency -s value

# Check all birthDates are valid ISO (no unparsed strings slipping through)
qsv search -s prop "^Person:birthDate$" data/datasets/xx_foo/statements.pack \
  | qsv search -v -s value "^[0-9]{4}(-[0-9]{2}(-[0-9]{2})?)?$" \
  | qsv select entity_id,value

# Verify original_value is preserved (dates, names)
qsv search -s prop "^Person:birthDate$" data/datasets/xx_foo/statements.pack | qsv select value,original_value | qsv behead | head -10

# Find entities missing an expected prop (e.g. birthDate)
qsv search -s prop "^Person:id$"        data/datasets/xx_foo/statements.pack | qsv select entity_id | qsv behead | sort > /tmp/all.txt && qsv search -s prop "^Person:birthDate$" data/datasets/xx_foo/statements.pack | qsv select entity_id | qsv behead | sort > /tmp/with_dob.txt && comm -23 /tmp/all.txt /tmp/with_dob.txt
```
