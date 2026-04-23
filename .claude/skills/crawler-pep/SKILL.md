---
name: crawler-pep
description: Scaffold a new PEP (Politically Exposed Persons) crawler from a source URL or GitHub issue
allowed-tools: Read, Edit, Write, Glob, Grep, Bash, WebFetch, WebSearch, Agent
---

# New PEP Crawler

Create a new PEP crawler. The user will provide a target path, source data URL,
and/or a GitHub issue URL: $ARGUMENTS

If given a GitHub issue URL, fetch it first to extract the data source URL and any
context about the dataset before proceeding.

**Before writing any code, read these files — they contain everything you need:**
1. `.claude/docs/crawler-guide.md` — shared crawler patterns (YAML template, fetching data, entity creation, helpers, lookups, FTM schemata, qsv analysis)
2. `.claude/skills/crawler-pep/examples.md` — full PEP code examples (Patterns A/B/C, current-only, ambiguous dates, associates)

**Do NOT search the repository for similar crawlers or patterns.** The guide and examples
above are the authoritative reference. Do not read `datasets/CLAUDE.md` or other crawler
source files for patterns — use only the files listed above.

## Step 1: Understand the source

Before writing any code, inspect the data source. In addition to the general checks
(fields, date formats, language, record count), PEP sources need:

- Check whether a Wikidata ID exists for the position(s) being crawled
- Identify the position types (parliament, cabinet, judiciary, etc.)
- Determine if the source lists current members only, or includes historical terms
- Check whether term start/end dates are provided

## Step 2: YAML metadata — PEP-specific parts

Use the generic YAML template from the crawler guide. PEP-specific additions:

```yaml
tags:
  - list.pep

assertions:
  min:
    schema_entities:
      Person: 100        # ~80% of expected count
      Position: 1        # at least 1 position
    country_entities:
      cc: 50
  max:
    schema_entities:
      Person: 1000       # ~150% of expected count
```

- `frequency` should match how often the source updates: `daily` for actively maintained
  lists (with a `schedule:` cron), `weekly` for slower sources, `monthly` for rarely changing
  data. There is no fixed rule that PEP crawlers must be monthly.
- Omit `schedule:` for weekly/monthly frequency.
- Include Position counts in assertions when the crawler creates multiple position types.
- PEP crawlers rarely need custom lookups beyond the standard `type.*` patterns
  (`type.country`, `type.gender`, etc.).

## Step 3: Write the crawler module

### Required imports

```python
from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise
```

### Position categorisation

`categorise()` is a **stateful database operation**, not a pure classifier. It persists
position metadata (`is_pep`, `topics`) to a shared PostgreSQL `position` table, which the
UI can then edit. This is the bridge between crawlers and the position review UI.

**How it works:**
1. First crawl: `categorise()` inserts the position into the database with the `is_pep`
   and `topics` values from the crawler.
2. UI review: Staff can edit `is_pep` and `topics` for any position via the positions UI.
3. Subsequent crawls: `categorise()` returns the **database values** (including any UI
   edits), ignoring whatever the crawler passes. The crawler's values only matter on
   first insertion.

**Three `is_pep` calling patterns:**

| Pattern | When to use | Example datasets |
|---|---|---|
| `is_pep=True` | Source definitionally contains PEPs (parliament, cabinet, judges) | fr_assemblee, ie_parliament, ky_judicial |
| `is_pep=None` | Mixed dataset — some positions are PEP, some aren't. Defers to UI. | fr_hatvp_declarations, cz_pep_declarations |
| `is_pep=False` | Explicitly not PEP (rare in PEP crawlers) | — |

**The `categorisation` result must be passed to `make_occupancy()`.**

### Critical rules

- Set ALL person props (birthDate, deathDate, etc.) BEFORE calling `make_occupancy()` —
  it reads them from the entity to determine PEP status.
- `make_occupancy()` returns `None` if the occupancy doesn't meet PEP criteria (e.g. ended
  too long ago). Only emit persons who have at least one valid occupancy.
- Emit the person AFTER `make_occupancy` — it mutates `person.topics`.
- Always pass `propagate_country=True` so the person inherits the position's country.
- Always call `categorise()` after creating a position and pass the result to `make_occupancy()`.
- For judicial crawlers, also add `person.add("topics", "role.judge")`.
- Do not use `wikidata_id` for per-municipality or per-region positions — it collapses all
  instances into one entity. Only use `wikidata_id` for nationally unique positions
  (e.g. "Member of Parliament", not "Mayor of X").

### `no_end_implies_current`

Controls how missing end dates are interpreted:
- `True` (default): "No end date means still in office." Use for live official rosters.
- `False`: "No end date means unknown." Use for declarations, point-in-time snapshots,
  historical data where absence of an end date doesn't prove current status.

### Position topics

Position topics (`gov.national`, `gov.legislative`, `gov.executive`, `gov.judicial`,
`gov.state`, `gov.muni`, etc.) are managed through the UI review system, not set in
crawler code. The `categorise()` call persists positions to the database; staff then
assign topics via the positions UI. Topics affect how long someone remains a PEP after
leaving office (e.g. `gov.national` = 20 years, `gov.national` + `gov.head` = no
expiration, default = 5 years).

### Name cleaning strategy for PEP data

PEP crawlers may use `h.clean_names()` (LLM-assisted) or the stateful name review
system (`h.apply_reviewed_names()` / `h.review_names()`) to normalise messy name data.
This is acceptable for PEP data — unlike sanctions, where LLM cleaning is forbidden.

## Step 4: PEP-specific validation checks

After running `zavod crawl`, use these PEP-specific qsv checks (see the crawler guide
for general qsv patterns):

```bash
# Position count
qsv search -s prop "^Position:id$" data/datasets/cc_dataset/statements.pack | qsv count

# Occupancy status distribution (expect mix of current/ended)
qsv search -s prop "^Occupancy:status$" data/datasets/cc_dataset/statements.pack | qsv frequency -s value

# Referential integrity: every Occupancy:holder must be a known Person
qsv search -s prop "^Occupancy:holder$" data/datasets/cc_dataset/statements.pack | qsv select value | qsv behead | sort > /tmp/holders.txt && qsv search -s prop "^Person:id$" data/datasets/cc_dataset/statements.pack | qsv select entity_id | qsv behead | sort > /tmp/persons.txt && comm -23 /tmp/holders.txt /tmp/persons.txt

# Country distribution (verify propagate_country is working)
qsv search -s prop "^Person:country$" data/datasets/cc_dataset/statements.pack | qsv frequency -s value
```

Then run `zavod validate datasets/cc/dataset/cc_dataset.yml`.

## FTM schemata reference (PEP-specific)

See the crawler guide for Person, Organization, Address, Family, and other shared schemata.

### Position
```
name, country, topics (e.g. ["gov.national", "gov.legislative"])
wikidataId, subnationalArea, organization
inceptionDate, dissolutionDate, numberOfSeats
```

### Occupancy (links Person to Position)
```
holder (Person), post (Position)
startDate, endDate, status (current|ended|unknown)
```

### Associate (for staff/collaborators)
```
person, associate, relationship
```
