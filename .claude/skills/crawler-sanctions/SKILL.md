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

**Read upfront.** These are the rules; this skill is the procedure for applying them:

1. `zavod/docs/sanctions/index.md`
2. `zavod/docs/sanctions/examples.md`
3. `.claude/docs/crawler-guide.md`

**Do NOT search the repository for similar crawlers or patterns.** Ground the crawler in
these files, not in other crawlers.

## Step 1: Understand the source

Do the recon in `zavod/docs/best_practices/source_recon.md` before writing any code. For a
sanctions source, also identify:

- the entity types present: persons, organizations, vessels, aircraft
- how sanctions programs are labeled in the source
- whether the source provides unique opaque IDs per entry
- whether relationships between entities are encoded (ownership, family, associates)

## Step 2: Write the YAML and crawler

Follow the pages above and `zavod/docs/metadata.md`.

## Step 3: Validate

After running `zavod crawl`, run these checks:

```bash
P=$(ls data/datasets/cc_dataset/_artifacts/*/statements.pack | tail -1)

# Entity counts by schema
qsv search -s prop "^Person:id$" "$P" | qsv count
qsv search -s prop "^Organization:id$" "$P" | qsv count
qsv search -s prop "^Sanction:id$" "$P" | qsv count

# Sanction program distribution
qsv search -s prop "^Sanction:program$" "$P" | qsv frequency -s value

# Every Sanction:entity must point to a real entity
qsv search -s prop "^Sanction:entity$" "$P" | qsv select value | qsv behead | sort -u > /tmp/sanction_targets.txt && qsv search -s prop ":id$" "$P" | qsv select entity_id | qsv behead | sort -u > /tmp/all_entities.txt && comm -23 /tmp/sanction_targets.txt /tmp/all_entities.txt

# Designated entities without topics=sanction (expect only expired designations)
qsv search -s prop ":topics$" "$P" | qsv search -s value "^sanction$" | qsv select entity_id | qsv behead | sort -u > /tmp/sanctioned.txt && comm -23 /tmp/sanction_targets.txt /tmp/sanctioned.txt
```

Then run `zavod export datasets/cc/dataset/cc_dataset.yml`, which checks the dataset
validators and assertions.
