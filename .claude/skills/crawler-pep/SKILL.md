---
name: crawler-pep
description: Scaffold a new PEP (Politically Exposed Persons) crawler from a source URL or GitHub issue
---

# New PEP Crawler

Create a new PEP crawler. The user will provide a target path, source data URL,
and/or a GitHub issue URL: $ARGUMENTS

If given a GitHub issue URL, fetch it first to extract the data source URL and any
context about the dataset.

**Read upfront**:

1. `.claude/docs/crawler-guide.md` — shared crawler patterns (YAML, fetching, entities, helpers, lookups)

**Consult on demand** (open only when you actually need the section — don't pre-load):

- `.claude/skills/crawler-pep/examples.md` — full code examples (Patterns A/B/C, subnational variant, occupancy date edge cases, associates). Open when you're stuck on a pattern or want a worked example.
- `zavod/docs/peps.md` — depth on Position naming, `categorise()`, Occupancy duration rules, and which person/PEP properties to capture (its "Properties to capture" section). Open if you need more than the summary in this skill.
- `zavod/docs/metadata.md` — full YAML field reference. Open if you're using a field not covered by the template in `crawler-guide.md`.
- `zavod/docs/extract/names.md` — open only if you're doing LLM-assisted or reviewed name cleaning.

**Prefer section reads over full reads.** All of these docs are well-headered — use `Grep` to find the symbol/topic you need (`make_occupancy`, `apply_date`, `coverage.start`, etc.) and `Read` with `offset`/`limit` instead of reading the whole file.

**Ground the crawler in the files listed above — they are the only source you need.**
They are the curated, current best practice, and `examples.md` is the maintained version
of "show me a crawler like this one." The wider crawler codebase is large and old, so many
crawlers have drifted from current practice — which is exactly why the docs, not the
corpus, are authoritative here.

## Step 1: Understand the source

In addition to the general checks (fields, date formats, language, record count):

- Is there a Wikidata ID for the position(s)? (See `zavod/docs/peps.md`; skip QIDs for per-municipality / per-region positions.)
- What are the position types (parliament, cabinet, judiciary, etc.)?
- Current members only, or historical terms too?
- Are start/end dates provided?
- **Term-bounded data?** Note any *structural* freshness signal (a new page URL, file name, or term id per term) so the crawler fails loudly when a new term lands. Record-count ranges are not a freshness signal — they belong in the `assertions` block, not the crawler.
- **Does the position legally require citizenship?** Don't assume from position type — national parliaments usually do (UK is an exception), but sub-national elected positions (mayors, councils) often don't. Spawn a subagent (`Agent` with `WebSearch`/`WebFetch`) to find the **legal document** (electoral law, constitution, official government guidance) that stipulates the citizenship requirement for this specific position. In a code comment next to the `person.add("citizenship", ...)` call — or, if citizenship is not required, next to the omission — include the URL to that legal document.

## Step 2: YAML metadata — PEP-specific parts

Full field reference: `zavod/docs/metadata.md`. PEP-specific additions:

```yaml
tags:
  - list.pep

assertions:
  min:
    schema_entities:
      Person: 100        # ~80% of expected count
      Position: 1
    country_entities:
      cc: 50
  max:
    schema_entities:
      Person: 1000       # ~150% of expected count
```

- Include `Position` counts in assertions when the crawler creates multiple position types.
- `frequency` matches source update cadence (daily/weekly/monthly). PEP crawlers do not have to be monthly.
- Lookups rarely go past `type.*` for PEP crawlers. (Non-English role labels are handled by `translate_name=True` in `make_position`; a `position` translation lookup is only worth it when the source has very few distinct labels.)

## Step 3: Write the crawler module

### Required imports

```python
from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise
```

### Person properties

Capture properties by priority — don't chase every field. For people, capture when
available: name(s), date/place/country of birth, citizenship/nationality, and ID
numbers. Don't extract private addresses or phone numbers. Full PEP property ladder
(Must/Could/Won't) and the generic framing: `zavod/docs/peps.md` → "Properties to
capture".

### Position naming

Build position names with `h.make_position`. Rules:

- **Always pass `lang=`** (ISO 639-3, e.g. `lang="eng"`, `lang="fra"`) declaring the
  language the position name is in. Two cases:
    - **Crawler-supplied names** (the standard case — e.g. a parliament crawler where
      the name is always `Member of the ... Parliament`): write the name in English
      and pass `lang="eng"`. Use the standard English term for the role; keep
      native-language terminology only for proper nouns of specific institutions
      (e.g. `Landtag of Mecklenburg-Vorpommern`).
    - **Source-supplied names** (role labels read from the data): pass them through
      as-is with the source language as `lang=` and `translate_name=True` —
      `make_position` translates the name to English via LLM and keys the entity ID
      on the untranslated original, so the ID stays stable. Only when the source has
      very few distinct labels, a `position` YAML lookup translating them to English
      (then `lang="eng"`) is fine instead — see the subnational variant in
      `examples.md`.
- Include the role, the organisational body where relevant, and the geographic jurisdiction. For members of national parliaments, include `citizenship` (except UK Parliament).
- Avoid: legislative term, an elected official's constituency, or the country for sub-national representatives.
- `wikidata_id` becomes the position's entity ID, so never pass the same QID to multiple distinct positions — they'd collapse into one entity. Per-municipality/region positions usually omit `wikidata_id` (per-locality QIDs rarely exist on Wikidata) and rely on `subnational_area=...` to disambiguate; pass a QID only when each subnational position has its own unique Wikidata entry.

Depth on edge cases: `zavod/docs/peps.md` → "Selecting a position name".

### Position categorisation

Full reference: `zavod/docs/peps.md`. `categorise()` is a stateful DB operation; `is_pep`/`topics` only matter on first insertion — subsequent crawls return DB values (including UI edits).

**`default_is_pep` calling patterns:**

| `default_is_pep` arg | When to use |
|---|---|
| `True` | Source definitionally contains PEPs (parliament, cabinet, judges) |
| `None` | Mixed dataset, or per-locality positions where the UI decides PEP status |

Pass the returned `categorisation` to `make_occupancy()`.

### Critical rules (in addition to `zavod/docs/peps.md`)

- Set ALL person props (birthDate, deathDate, etc.) BEFORE calling `make_occupancy()` — it reads them to determine PEP status.
- `make_occupancy()` returns `None` if the occupancy doesn't meet PEP criteria. Only emit persons with at least one valid occupancy.
- Emit the person AFTER `make_occupancy` — it mutates `person.topics`.
- For judicial crawlers, also `person.add("topics", "role.judge")`.
- **Term-bounded sources** (fixed mandates, per-term archives): fail in `crawl()` when the source's *structural* signature changes (new page URL, file name, term id). Don't hardcode record-count bands and `raise` — count sanity is the `assertions` block's job. A continuously-updated roster (a parliament refilled by by-elections) is not term-bounded.

### `no_end_implies_current`

- `True` (default): no end date → still in office. Use for live official rosters.
- `False`: no end date → unknown. Use for declarations, point-in-time snapshots, historical data.

### Name cleaning

LLM-assisted (`h.clean_names()`) and reviewed-name (`h.apply_reviewed_names()`) helpers are both acceptable for PEP data — full reference: `zavod/docs/extract/names.md`. (Unlike sanctions, where LLM cleaning is forbidden.)

## Step 4: Validate

Run `zavod crawl <path>` then `zavod validate <path>`.

Spot-check the crawl output with qsv against `data/datasets/<dataset>/statements.pack`.
The `prop` column is `Schema:property`, so entity type is recoverable; within one
dataset's pack `entity_id` matches the ids that `Occupancy:holder`/`post` reference (this
is pre-resolution crawl output). Each integrity check below should print nothing:

```bash
P=data/datasets/<dataset>/statements.pack

# Entity counts — sanity-check against the assertions block
for s in Person Position Occupancy; do
  echo "$s: $(qsv search -s prop "^${s}:id\$" "$P" | qsv behead | wc -l)"
done

# 1. Occupancy.post referencing a Position that wasn't emitted
comm -23 \
  <(qsv search -s prop '^Occupancy:post$' "$P" | qsv select value     | qsv behead | sort -u) \
  <(qsv search -s prop '^Position:'       "$P" | qsv select entity_id | qsv behead | sort -u)

# 2. Occupancy.holder referencing a Person that wasn't emitted
comm -23 \
  <(qsv search -s prop '^Occupancy:holder$' "$P" | qsv select value     | qsv behead | sort -u) \
  <(qsv search -s prop '^Person:'           "$P" | qsv select entity_id | qsv behead | sort -u)

# 3. role.pep Person that never holds an Occupancy
comm -23 \
  <(qsv search -s prop '^Person:topics$' "$P" | qsv search -s value '^role\.pep$' | qsv select entity_id | qsv behead | sort -u) \
  <(qsv search -s prop '^Occupancy:holder$' "$P" | qsv select value | qsv behead | sort -u)

# 4. PEP Person with no country/citizenship/nationality (make_occupancy no longer
#    back-fills country from the position, so this must be set explicitly)
comm -23 \
  <(qsv search -s prop '^Person:topics$' "$P" | qsv search -s value '^role\.pep$' | qsv select entity_id | qsv behead | sort -u) \
  <(qsv search -s prop '^Person:(citizenship|country|nationality)$' "$P" | qsv select entity_id | qsv behead | sort -u)
```
