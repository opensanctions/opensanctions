---
name: crawler-cleanup
description: Rewrite messy or AI-generated crawler code into clean, production-ready style that follows the zavod best practices. Use when the user asks to clean up, refactor, tidy, or "make production-ready" a crawler, or to bring code in line with best practices.
argument-hint: "[crawler.py path]"
allowed-tools: Read, Edit, Write, Glob, Grep, Bash
---

# Crawler cleanup

Refactor an existing crawler so it follows the OpenSanctions/zavod best practices. Typical
input is a first-draft or AI-generated crawler that works but is verbose, brittle, or
non-idiomatic. Target path: $ARGUMENTS

**The authoritative rules live in `zavod/docs/best_practices/`. This skill is the
_procedure_ for applying them plus before→after recipes.** When a rule is unclear or a
case isn't covered here, read the linked doc — do not invent a convention.

## Golden rule: refactor, don't rewrite behaviour

The output data must not change unless a change is a genuine correctness fix. This is a
**style and robustness** pass: restructure code, tighten selectors, move logic into
helpers, add guards — but keep the set of entities and property values the crawler emits
the same. If you believe a value is actually wrong, call it out to the user separately;
don't silently alter what's emitted.

Sanctioned/legal names are especially sensitive: never introduce LLM-based or heuristic
name "cleaning". See `zavod/docs/extract/names.md`.

## Read first

The best-practices docs, each governing one area below:

- `best_practices/patterns.md` — crawler structure, naming, helpers, constants, logging, assertions, pagination
- `best_practices/xpath_and_html.md` — typed HTML helpers, selector quality, failing loudly
- `best_practices/entity_id.md` — `make_id` vs `make_slug`, which fields, `key=`
- `best_practices/datapatch_lookups.md` — replacing inline conditionals with YAML lookups
- `best_practices/dates_meta.md` — `apply_date` and dataset-level date formats
- `best_practices/addresses.md` — `make_address` + `copy_address`
- `best_practices/http_operations.md` — `context.fetch_*`, headers, Zyte
- `best_practices/caching.md` — what not to cache
- `best_practices/priorities.md` — which properties are worth the effort
- `best_practices/merge_checklist.md` — the pre-merge review checklist

Related skills: for pure type-annotation errors use `/typechecker-fixes`; this skill covers
structure and idiom, and defers the mypy details to it.

## Workflow

1. **Read the crawler and its `.yml`.** Note the data source shape (HTML/CSV/XLSX/JSON/API),
   existing `lookups:`, `dates:`, and `http:` sections.
2. **Apply the patterns below**, most impactful first: structure → selectors/parsing →
   IDs → dates/addresses → lookups → HTTP/caching → logging/assertions → style nits.
3. **Verify** (see the section at the end): `mypy --strict`, `ruff`, and a `zavod crawl`
   run with a clean `issues.log`.
4. **Summarise** what changed and flag anything you deliberately left alone.

---

## Patterns

Each pattern cites the doc that owns it. Read that doc when a case here is ambiguous.

### 1. Split into `crawl` + per-record functions — `patterns.md`

The entrypoint `crawl(context)` fetches data, turns it into an iterable of dicts/rows, and
loops — delegating each record to a `crawl_row` / `crawl_item` / `crawl_person` function
that unpacks, cleans, builds entities, and emits. Name functions that lead to emitted
entities `crawl_<thing>`; name pure extractors specifically (`parse_row`), never
`process_data`.

```python
# Before: one 150-line crawl() with nested branching
def crawl(context):
    for row in rows:
        if row["type"] == "vessel":
            ... 40 lines ...
        elif row["type"] == "person":
            ... 40 lines ...

# After
def crawl_vessel(context: Context, row: dict[str, str]) -> None: ...
def crawl_person(context: Context, row: dict[str, str]) -> None: ...

def crawl(context: Context) -> None:
    for row in fetch_rows(context):
        crawl_row(context, row)
```

Prefer the parsing helpers that yield dicts over hand-rolled loops: `h.parse_html_table`,
`h.parse_xlsx_sheet`, `h.parse_pdf_table`, `csv.DictReader`.

### 2. Helpers at module level, not closures — `patterns.md`

Move nested functions out to module level and pass what they need as arguments. `context`
comes first.

### 3. Return early to flatten nesting — `patterns.md`

Invert `if A: <body>` into `if not A: return` so the main path is un-indented.

```python
name = row.pop("name")
listing_date = row.pop("listing_date")
if not (name and listing_date):
    return
entity = context.make("LegalEntity")
entity.id = context.make_id(name, listing_date)
```

### 4. Inline single-use literals; earn every constant — `patterns.md`

A `TOPICS = [...]`, `COLUMNS = [...]`, or header list referenced once reads better inlined
at the call site. Declare a module constant only for: **precompiled regexes**, **genuine
reuse** (2+ sites), or **an enumeration that is the data** (e.g. a `POSITIONS` mapping).

```python
# Keep as a constant — compiled once, correctness/perf:
REGEX_DOB = re.compile(r"\b(\d{4})\b")
# Inline instead of hoisting a one-off:
person.add("topics", "sanction")
```

### 5. Capture all fields, then `audit_data` the remainder — `patterns.md`

Read source records into a dict, `pop()` each field as you use it, and end each record with
`context.audit_data(row, ignore=[...])` so newly-appearing fields raise a warning instead
of being dropped silently.

Descriptive fields with **no FTM equivalent** (hair colour, internal codes) go into the
`ignore` list — do **not** pack them into `notes`/`description` as `f"Hair: {x}"` strings.

```python
context.audit_data(row, ignore=["hair_colour", "internal_ref"])
```

### 6. Typed, semantic, loud XPath — `xpath_and_html.md`

Replace raw `.xpath()` (returns `Any`) with the typed helpers, and make selections fail
loudly when they don't match.

```python
# Before
rows = table.xpath("./tbody/tr")            # Any; empty selection = silent no-op
title = doc.xpath(".//h1/text()")[0]

# After
rows = h.xpath_elements(table, "./tbody/tr", expect_exactly=12)
title = h.element_text(h.xpath_element(doc, ".//h1"))
```

- One expected match → `h.xpath_element` (raises on 0 or many). Known count → `expect_exactly=`.
  Open-ended but non-empty → `assert len(items) > 0, items`.
- Selectors: specific but semantic. Prefer `.//div[@id="content"]//table` over positional
  `div[3]`; prefer `contains(@class, 'abc')` over `@class='abc'` (watch for `abc-footer`).
- `.findall()` is fine for trivial relative selectors; reach for `xpath_*` when you need
  predicates/axes/`contains`.
- Use `h.assert_dom_hash` (with `text_only=True`) to get warned when hand-parsed page
  regions drift.

### 7. Entity IDs: right fields, raw values, `make_id` by default — `entity_id.md`

- Default to `context.make_id(...)`. Use `context.make_slug(...)` **only** for a single,
  clean, authority-defined identifier — never for names/DOB/ID numbers, never multi-field.
- Feed **raw source values**: no trimming, casing, parsing, or substitution (that would
  mutate every previously-computed ID).
- Pick the fewest stable, mandatory fields that disambiguate.
- Don't set `.id` on entities from `h.make_sanction`/`h.make_address`/`h.make_position` —
  they derive it. Pass `key=` when one entity has several of the same sub-entity.

```python
# Before
entity.id = context.make_slug(name.lower().strip(), dob)   # PII + cleaned + multi-part
# After
entity.id = context.make_id(name, dob)                     # raw values, hashed
```

### 8. Dates via `apply_date` + dataset formats — `dates_meta.md`

Replace manual `strptime`/`datetime` parsing with `h.apply_date` / `h.apply_dates`, and
declare formats (and non-English months) in the `.yml` `dates:` section. This emits
warnings on bad dates and preserves `original_value`.

```python
# Before
entity.add("birthDate", datetime.strptime(v, "%d.%m.%Y").date().isoformat())
# After  (yml: dates.formats: ['%d.%m.%Y'])
h.apply_date(entity, "birthDate", v)
```

Never make a date more precise than the source (`datapatch_lookups.md`).

### 9. Addresses via `make_address` + `copy_address` — `addresses.md`

Pass structured parts to `h.make_address` and inline them with `h.copy_address`; don't
pre-format the `full` string in the crawler. Don't emit standalone `Address` entities by
default. For country-only data, `entity.add("country", value)` directly.

```python
address = h.make_address(context, street=row.pop("street"), city=row.pop("city"),
                         postal_code=row.pop("zip"), country=row.pop("country"))
h.copy_address(entity, address)
```

### 10. Lookups instead of inline conditionals — `datapatch_lookups.md`

Move recurring value fixes/mappings into the YAML `lookups:` section: date typos, header
translations, source-type → FTM schema, relationship → FTM relation, program names. `type.*`
lookups apply automatically; named lookups are called via `context.lookup_value` /
`context.lookup`.

```python
# Before
schema = "Person" if t in ("individual", "person") else "Organization"
# After (yml lookup type.entity), applied where you dispatch schema
schema = context.lookup_value("type.entity", t)
```

### 11. HTTP through `context.fetch_*` — `http_operations.md`

Use `context.fetch_text/html/json` (caching, retries, status checks) instead of raw
`requests`. For bot-blocking set a browser-like `http.user_agent` in the `.yml`, then
richer headers; for geo/network blocks or JS challenges route through `zavod.extract.zyte_api`
(sets `ci_test: false`). Use `doc.make_links_absolute(url)` rather than `urljoin`.

### 12. Caching: don't cache index or paginated pages — `caching.md`

Index/listing pages: no cache (or ≤1 day) so new entities surface fast. Never cache
paginated pages — added items shift entries between pages and cause disappear/reappear
bugs. Detail pages of slow-moving sources may cache longer.

### 13. Pagination that fails loudly, never spins — `patterns.md`

Model the site's own controls (next-URL, max page number). Avoid `while True`; if
unavoidable, add a page counter with an `assert pages < N`. Prefer a `KeyError` on
structural change over silent under-collection.

```python
next_url: str | None = context.data_url
while next_url:
    data = context.fetch_json(next_url)
    next_url = data["links"]["next"]   # KeyError on change; None ends the loop
    for item in data["data"]:
        crawl_row(context, item)
```

### 14. Logging levels & assertions — `patterns.md`

- `warning` for things the team should act on (unmapped entity type, unexpected shape) —
  these surface on the Issues page. Not for known-permanent 404s.
- `info` for progress on large crawls; `debug` for dev detail.
- Add assertions for invariants (a Person has a name, a required field is present) so
  breakage fails at runtime rather than emitting ambiguous data.

```python
context.log.warning("Unhandled entity type", type=entity_type)
assert position_name is not None, entity.id
```

### 15. Text hygiene & original language — `patterns.md`

- Capture free text in its source language; pass a 3-letter code via `lang=` when known:
  `sanction.add("reason", text, lang="rus")`. Don't translate arbitrary text.
- Clean stray whitespace/space chars with `normality.squash_spaces` / `.replace("\xa0", " ")`,
  not ad-hoc slicing.

### 16. Imports & typing — `patterns.md` + `/typechecker-fixes`

Order imports stdlib → third-party → `zavod` (`ruff check --fix --select I <file>`). Fully
type the crawler; use builtin generics (`dict`, `list`) and `X | None`. For the mypy-strict
details defer to the `/typechecker-fixes` skill.

---

## Verification

Run from the repo root after refactoring:

```bash
ruff check --fix --select I <crawler.py>          # import order
cd zavod && mypy --strict <path/to/crawler.py>    # strict types (mypy-datasets hook enforces this)
cd /Users/vestloporvats/projects/opensanctions && zavod crawl <path/to/dataset.yml>
```

Then confirm:

- `data/datasets/<name>/issues.log` is empty of new warnings/errors (transient network
  errors excepted).
- Entity/property output is unchanged vs. before the refactor (diff statement counts or
  spot-check a few entities) — this pass must not alter emitted data.
- The crawler still satisfies its `assertions:` in the `.yml`.

Finally walk the `merge_checklist.md` items relevant to what you touched, and tell the user
which best-practice areas you changed and which you intentionally left as-is.
