---
name: crawler-constants-to-yml
description: Move hardcoded lookup/config constants (gender maps, header dicts, value translations, column-label maps, date formats) out of a crawler and into the dataset .yml — as datapatch lookups wherever possible, otherwise http / config / dates metadata. Use when asked to move, hoist, or "put in the yml" a crawler constant such as GENDERS or HEADERS.
argument-hint: "[dataset .yml or crawler.py path]"
allowed-tools: Read, Edit, Bash, Grep, Glob
---

# Move crawler constants into the dataset YAML

Hoist a module-level constant out of a crawler and into its `.yml`. The guiding rule:
**whatever the framework can handle from the YAML should live in the YAML** — a lookup,
`http`, `config`, or `dates` block — leaving the crawler code free of one-off maps and
conditionals. Target: $ARGUMENTS (the `.yml`, or the sibling `crawler.py`).

Read the relevant `zavod/docs` before editing — do not re-derive the mechanics here:

- Lookups (type vs named, matching, config keys): `zavod/docs/best_practices/datapatch_lookups.md`
- Headers / bot-blocking / `http.user_agent`: `zavod/docs/best_practices/http_operations.md`
- Date formats and month-name translation: `zavod/docs/best_practices/dates_meta.md`

## 1. Classify the constant → pick its YAML home

| Constant in code | Belongs in | Mechanism |
|---|---|---|
| Map of source value → clean value for a **typed property** (gender, country, …) | `lookups: type.<type>` | Auto-applied on `entity.add(prop, raw)`. Pass the raw value; delete the dict. |
| Map/translation of a value for a **non-type** property, or a categorical label | `lookups: <name>` | `context.lookup_value("<name>", raw, warn_unmatched=True)` |
| Request `User-Agent` | `http.user_agent` | Applied to the session; drop `headers=` if it held only the UA. |
| Static structural/config dict (column labels, field maps used for validation) | `config:` | Read via `context.dataset.config["key"]` |
| Date formats / non-English month names | `dates.formats`, `dates.months` | Consumed by `h.apply_date` |

Type-property names map to their `type.<type>` per the table in `datapatch_lookups.md`
(e.g. `gender → type.gender`, `citizenship/country → type.country`, `email → type.email`).

If a constant genuinely cannot move (see gotchas), leave it in code — do not force it.

## 2. Apply the change

**Type lookups (the common `GENDERS` case).** Add the lookup and feed the raw value in:

```yaml
lookups:
  type.gender:
    options:
      - match: Masculino   # exact source values, one per real value
        value: male
      - match: Femenino
        value: female
```
```python
person.add("gender", record["sexo"])   # was: GENDERS.get(record["sexo"])
```
Delete the dict, its `.get(...)`, and any `or ""`/None-guard it needed. Match the source
value *exactly* — vocabularies differ per source (`Femenino` vs `Feminino`, `Hombre`/`Mujer`
vs `Masculino`/`Femenino`); never copy a sibling dataset's lookup blind.

**Named lookups.** For a value that isn't a typed property (e.g. a seat/membership type),
use an explicit lookup and **store the result** — if the old code fetched the value only to
validate it, recording the mapped value is a strict improvement:
```python
seat_type = context.lookup_value("membership_type", raw, warn_unmatched=True)
occupancy.add("summary", seat_type)
```

**Headers.** Move only the `User-Agent` to `http.user_agent`. For any *other* header, first
prove whether the server actually needs it (§3) — drop inert ones, keep genuinely-required
ones inline (the `http` block supports only `user_agent`). A format-selecting `Accept` header
can sometimes be replaced by a URL suffix/param in `data.url` (e.g. `…/atual.json`),
removing the header entirely.

## 3. Verify — always run the crawler

```bash
ruff check <crawler.py> && mypy --strict <crawler.py>
zavod crawl <dataset.yml>
# issues.log must be clean (or only carry warnings you expect):
python3 -c "import json;[print(json.loads(l)['level'],json.loads(l).get('message')) for l in open('data/datasets/<name>/issues.log')]" | sort | uniq -c
# confirm the value still lands, at the expected coverage:
grep -a ',<Schema>:<prop>,' data/datasets/<name>/statements.pack | awk -F, '{print $3}' | sort | uniq -c
```

When probing which headers a server requires, drop them one at a time against the live
endpoint and keep only those whose removal changes the status/response.

## Gotchas

- **A type lookup changes failure behavior — that's the point.** The old `DICT.get(x)`
  returned `None` and *silently dropped* unmapped values; the lookup instead lets unmapped
  values reach the type cleaner, which warns. After the change, confirm coverage in the
  statement count and that no new `Rejected property value` warnings appear.
- **Lookups normalize/order-independently; don't put order-significant data in one.** Keep
  ordered structural lists (e.g. left-to-right column keys) in code; move only the
  order-independent label map to `config:`. Making correctness depend on YAML key order is a
  trap for the next editor.
- **`config:` is for static config, not value cleaning.** Reach for a `lookups:` block when
  you're normalizing property values; use `config:` for parsing/structure knobs.
- **Not every header fits the YAML.** `Accept`, `Content-Type`, `Referer`, etc. have no
  `http` field — required ones stay as an inline `headers=` dict on the fetch call.
- **Don't preserve cargo-cult headers.** If a comment claims the server "needs" a header set
  but §3 shows a header is inert, drop it rather than moving it.
