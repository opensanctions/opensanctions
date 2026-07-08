# Strict interpretation

A crawler that meets source data it does not understand must crash or warn, rather than silently absorb the change.

OpenSanctions processes risk and compliance data. A new field appears in the source, a category of entity is renamed, a column heading changes: when a crawler quietly absorbs a shift it did not anticipate, the result can understate or overstate the risk associated with an entity, or fail to reproduce the information needed to identify it. It is always better for a crawler to generate maintenance work for the team than to silently ignore a change in the source data that it does not understand.

The requirement: every source value within the crawler's scope is either handled, explicitly ignored, or raises a signal. How a crawler meets it is not dogma; the practices below are the proven mechanisms.

## Two decisions: what is in scope, and how to enforce it

### Scope follows the source's purpose

- On **sanctions lists and watchlists**, the whole record is in scope by default. Records are compact, and any field can bear on the risk statement or on identifying the entity. A new field is a signal worth a warning.
- On **content-rich sources** such as parliamentary websites, the source routinely publishes facts the crawler does not capture: committee memberships, recent speeches, constituency office hours. Scope is a deliberate decision, guided by the [data priorities](priorities.md). Strictness applies to the fields inside that scope. There is no obligation to account for facts the crawler never set out to capture, and no warning is owed when out-of-scope content changes.
- **Always in scope**, on every source type: categorical fields that determine interpretation. See [complete coverage](#key-categorical-fields-need-complete-coverage) below.

### Mechanism follows the data format

- **Structured records** — JSON objects, XML elements, CSV and spreadsheet rows, and HTML tables via [`h.parse_html_table`][zavod.helpers.parse_html_table] — give you a bounded record to exhaust: parse destructively and audit the remainder.
- **Free-form HTML** gives you no record to audit; "accounting for every value on the page" is meaningless there. Strictness lives in the selectors instead.

The two axes often correlate: sanctions data tends to arrive structured, while PEP data is often crawled from HTML. Don't let that blur them. An HTML sanctions table parsed with `parse_html_table` still gets popped and audited row by row, and a content-rich JSON API still gets audited, with the out-of-scope fields listed in `ignore=`.

## Pick the loudness by whether the crawl can continue

- **Crash** when continuing would emit wrong or ambiguous data, or when the broken assumption is structural, meaning the whole parse is invalid if it's false. An unguarded `row.pop(key)`, an `assert` with a message, or `required: true` on a lookup are all fine ways to crash.
- **Warn and skip the record** with `context.log.warning` when one record is unmappable but the rest of the crawl is still valid. Warnings surface on the [Issues](https://www.opensanctions.org/issues/) page, where a daily rotation fixes them; generating that work is the point.

A skip is an alarm state, not an accepted outcome: dropping a record that the source publishes is itself a strict-interpretation failure. A warning lives for a day or a few, never permanently: whoever sees it resolves it by adding the missing mapping — the guard remains to catch the next unknown value.

See [logging and crawler feedback](patterns.md#logging-and-crawler-feedback) for what belongs at warning vs. info level.

## Structured records: pop, then audit the remainder

Read each record into a `dict` and remove each field with `.pop()` as you map it. Use the one-argument form for mandatory fields: `row.pop("name")` raises a `KeyError` the day the source drops or renames the field, instead of adding `None` to the entity.

Close each record with [`context.audit_data()`][zavod.context.Context.audit_data], which warns about fields still left in the `dict`. A field the source introduces tomorrow lands in the remainder and produces a warning instead of being dropped.

Fields that are out of scope or have [no FollowTheMoney equivalent](patterns.md#fields-with-no-followthemoney-equivalent) go into the `ignore=[...]` list. That list is an explicit allowance and doubles as the documented scope decision; a genuinely new field still trips the alarm:

```python
context.audit_data(row, ignore=["hair_colour", "committee_memberships"])
```

## Free-form HTML: strictness lives in the selectors

Extract the in-scope fields with selections that fail when the page no longer matches expectations, and leave the rest of the page alone:

- [`h.xpath_element`][zavod.helpers.xpath_element] raises on zero or multiple matches; `expect_exactly=` pins a known count; open-ended selections get an `assert len(items) > 0, items`.
- [`h.assert_dom_hash`][zavod.helpers.assert_dom_hash] warns when a hand-parsed page region drifts.

See [XPath and HTML](xpath_and_html.md) for the full toolkit and selector-quality guidance.

## Assert the invariants of a valid parse

Assert what must be true for the parse to be valid, so breakage fails at the offending line rather than emitting ambiguous data downstream:

```python
assert position_name is not None, entity.id
```

See [data assertions](patterns.md#data-assertions). Dataset-level [assertions in the metadata](../metadata.md#data-assertions) are the outermost net: they catch a crawl that silently under-collects.

## Key categorical fields need complete coverage

When a source field determines interpretation — an entity or subject type, a name type, a relationship category — enumerate every known value in a [lookup](datapatch_lookups.md) and treat an unknown value as a signal. The rule holds on every source type, however narrow the scope otherwise is: never fall through to a default. A bare `else` that lumps unknown categories into `LegalEntity` is exactly the silent risk misstatement that strict interpretation exists to prevent.

In increasing loudness:

- `context.lookup_value(...)` with an explicit `None` check (or `warn_unmatched=True`) that warns and skips the record. `eu_fsf` maps `subject_type` this way: `person` → `Person`, `enterprise` → `Organization`, and anything else warns and skips.
- `required: true` on the lookup, so a miss raises a `LookupException` and halts the crawl.

`type.*` lookups are the exception: they clean individual property values. An unmatched value flows on to normal property validation.

Column headings are categorical input too. Some non-English tabular crawlers map each native language column heading to a stable slug through a `columns` lookup, so the crawler code reads in English. When the source renames a column, the expected key goes missing and the destructive-parsing net catches it.

## Anti-patterns

- `except: pass`, or any broad `except` that swallows parse errors.
- `.get(key, default)` on a mandatory field.
- A bare `else` or fallback schema for unknown categorical values.
- Looping over a possibly-empty selection with no count guard, so the crawler silently emits nothing.
- Packing out-of-scope facts into `notes` or `description` strings to "handle" them. Out of scope means left alone, not smuggled into free text.
