---
name: crawler-sanctions
description: Scaffold a new sanctions list crawler from a source URL or GitHub issue
allowed-tools: Read, Edit, Write, Glob, Grep, Bash, WebFetch, WebSearch, Agent
---

# New Sanctions Crawler

Create a new sanctions list crawler. The user will provide a target path, source data URL,
and/or a GitHub issue URL: $ARGUMENTS

If given a GitHub issue URL, fetch it first to extract the data source URL and any
context about the dataset before proceeding.

**Before writing any code, read these files — they contain everything you need:**
1. `.claude/docs/crawler-guide.md` — shared crawler patterns (YAML template, fetching data, entity creation, helpers, lookups, FTM schemata, qsv analysis)
2. `.claude/skills/crawler-sanctions/examples.md` — full sanctions code examples

**Do NOT search the repository for similar crawlers or patterns.** The guide and examples
above are the authoritative reference. Do not read `datasets/CLAUDE.md` or other crawler
source files for patterns — use only the files listed above.

## Step 1: Understand the source

Before writing any code, inspect the data source. In addition to the general checks
(fields, date formats, language, record count), sanctions sources need:

- Identify entity types present: persons, organizations, vessels, aircraft
- Identify how sanctions programs are labeled in the source
- Check if the source provides unique opaque IDs per entry (for slug-based IDs)
- Check if relationships between entities are encoded (ownership, family, associates)
- Identify the data structure: flat list vs nested XML vs paginated API

## Step 2: YAML metadata — sanctions-specific parts

Use the generic YAML template from the crawler guide. Sanctions-specific additions:

```yaml
tags:
  - list.sanction
  - issuer.west          # optional

assertions:
  min:
    schema_entities:
      Person: 1000       # ~80% of expected count
      Organization: 200
      Sanction: 1000
    country_entities:
      cc: 100
  max:
    schema_entities:
      Person: 5000       # ~150% of expected count
      Organization: 1000
```

- Sanctions lists typically use `frequency: daily` with a cron `schedule:`.
- Assert Sanction entity counts alongside Person/Organization counts.

### Sanctions-specific lookups

The most important sanctions lookup maps source program names to OpenSanctions keys:

```yaml
lookups:
  # Entity type dispatch (when source uses custom type labels)
  type.entity:
    lowercase: true
    options:
      - match: [individual, person]
        value: Person
      - match: [entity, company, organization]
        value: Organization
      - match: [vessel, ship]
        value: Vessel

  # Map source program names to OpenSanctions program keys
  sanction.program:
    options:
      - match: "Executive Order 13224"
        value: US-EO13224

  # Date edge cases common in sanctions data
  type.date:
    options:
      - match: "1972-08-10 or 1972-08-11"
        values: ["1972-08-10", "1972-08-11"]
      - match: "1975-19-25"       # typo
        value: "1975"
```

`type.*` lookups are applied automatically by `entity.add()`. The `sanction.program`
lookup must be called explicitly via `h.lookup_sanction_program_key()`.

## Step 3: Write the crawler module

### Sanction entity creation

`h.make_sanction()` automatically sets `country`, `authority`, and `sourceUrl` from
dataset metadata. The key parameters:

```python
sanction = h.make_sanction(
    context,
    entity,                                    # the sanctioned entity (required)
    key=entry_id,                              # disambiguator when entity has multiple sanctions
    program_name=program,                      # human-readable program name
    source_program_key=program,                # raw value from source (preserved as original_value)
    program_key=h.lookup_sanction_program_key(  # OpenSanctions program key from yaml lookup
        context, program
    ),
    start_date=listing_date,                   # optional: when sanction began
    end_date=end_date,                         # optional: when sanction ended
)
```

- **`key`**: Use when an entity appears on multiple sanctions lists/programs. The sanction
  ID is `make_id("Sanction", entity.id, key)`, so `key` disambiguates multiple sanctions
  per entity.
- **`program_key`**: Always go through `h.lookup_sanction_program_key()` which reads the
  `sanction.program` yaml lookup. Add entries to the lookup as you encounter new program
  names.
- **`source_program_key`**: The raw program string from the source, preserved as
  `original_value` on the programId property for auditability.
- **Always** also set `entity.add("topics", "sanction")` on the sanctioned entity.

For simple datasets with a single known program, you can skip the lookup:

```python
sanction = h.make_sanction(context, entity, program_key="US-DOS-CU-PAL")
```

#### Checking if a sanction is active

```python
if h.is_active(sanction):
    entity.add("topics", "sanction")
# Only mark as sanctioned if the sanction is currently active
```

### Name handling in sanctions crawlers

Sanctioned names are legal designations. Unlike PEP crawlers, **do not use LLM-based
name cleaning**. Any normalisation must be human-reviewed via the stateful name review
system, or handled with explicit lookup entries.

### Relationships between sanctioned entities

See the crawler guide for the generic Family and Ownership patterns.
See [examples.md](examples.md) for UnknownLink (sanctions-specific untyped relationships).

### De-listing and modification tracking

When the source tracks modifications and de-listings, use `sanction.add("endDate", ...)`
for de-listings and `sanction.add("modifiedAt", ...)` for amendments. See
[examples.md](examples.md) for the full pattern.

### LLM extraction from free-text fields

For sources with unstructured "remarks" fields containing structured data, use GPT
extraction with the stateful review system (`run_typed_text_prompt` + `review_extraction`).
Requires `ci_test: false`. See [examples.md](examples.md) for the full pattern.

## Step 4: Sanctions-specific validation checks

After running `zavod crawl`, use these sanctions-specific qsv checks (see the crawler
guide for general qsv patterns):

```bash
# Entity counts by schema
qsv search -s prop "^Person:id$" data/datasets/cc_dataset/statements.pack | qsv count
qsv search -s prop "^Organization:id$" data/datasets/cc_dataset/statements.pack | qsv count
qsv search -s prop "^Sanction:id$" data/datasets/cc_dataset/statements.pack | qsv count

# Sanction program distribution
qsv search -s prop "^Sanction:program$" data/datasets/cc_dataset/statements.pack | qsv frequency -s value

# Every Sanction:entity must point to a real entity
qsv search -s prop "^Sanction:entity$" data/datasets/cc_dataset/statements.pack | qsv select value | qsv behead | sort > /tmp/sanction_targets.txt && qsv search -s prop ":id$" data/datasets/cc_dataset/statements.pack | qsv select entity_id | qsv behead | sort -u > /tmp/all_entities.txt && comm -23 /tmp/sanction_targets.txt /tmp/all_entities.txt

# Check all entities have topics=sanction
qsv search -s prop ":id$" data/datasets/cc_dataset/statements.pack | qsv select entity_id | qsv behead | sort -u > /tmp/all_ids.txt && qsv search -s prop ":topics$" data/datasets/cc_dataset/statements.pack | qsv search -s value "^sanction$" | qsv select entity_id | qsv behead | sort -u > /tmp/sanctioned.txt && comm -23 /tmp/all_ids.txt /tmp/sanctioned.txt
```

Then run `zavod validate datasets/cc/dataset/cc_dataset.yml`.

## FTM schemata reference (sanctions-specific)

See the crawler guide for Person, Organization, LegalEntity, Address, Family, Ownership,
and other shared schemata.

### Vessel
```
name, flag, imoNumber, mmsi, callSign
type, tonnage, buildDate
alias, previousName, topics (sanction)
```

### Airplane
```
name, serialNumber, registrationNumber
model, type
alias, topics (sanction)
```

### Sanction
```
entity (required -- the sanctioned entity)
authority, authorityId
program, programId, programUrl
unscId (UN Security Council ID)
startDate, endDate, listingDate, modifiedAt
reason, provisions, status, country
sourceUrl, summary
```

### Identification (passport / ID document entity)
```
holder, number, type, country, authority
startDate, endDate, summary
```

### UnknownLink
```
subject, object, role
```
