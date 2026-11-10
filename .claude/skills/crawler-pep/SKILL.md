---
name: crawler-pep
description: Scaffold a new PEP (Politically Exposed Persons) crawler — members of a parliament, legislature, senate, chamber of deputies, cabinet, judiciary, or an asset-declaration register — from a source URL or GitHub issue. Creates the dataset .yml plus a crawler emitting Person, Position and Occupancy entities via make_position/categorise/make_occupancy. Use when asked to add, write or scaffold a PEP or members-of-parliament crawler.
argument-hint: "[target path | source URL | GitHub issue URL]"
allowed-tools: Read, Edit, Write, Glob, Grep, Bash, WebFetch, WebSearch, Agent
---

# New PEP Crawler

Create a new PEP crawler. The user will provide a target path, source data URL,
and/or a GitHub issue URL: $ARGUMENTS

If given a GitHub issue URL, fetch it first to extract the data source URL and any
context about the dataset.

**Read upfront.** These are the rules; this skill is the procedure for applying them:

1. `zavod/docs/peps/index.md` — the PEP model, properties, position naming, categorisation,
   occupancy dates and status, historical terms.
2. `.claude/docs/crawler-guide.md` — shared crawler patterns, YAML template, lookups.
3. `zavod/docs/peps/examples.md` → **"Reference crawler"** — a complete, reviewed crawler.
   Yours should look like it: the shortest code that still handles every source field
   explicitly. Each helper, constant, guard or extra request you add beyond it needs a
   reason a reviewer can see — first drafts fail review far more often from added
   machinery than from missing it.

Open the rest of `examples.md` when your source differs from the reference (mixed
datasets, several positions, multi-term, subnational). Ground the crawler in these files,
not in other crawlers: the codebase is old and many have drifted from current practice.

## Step 1: Understand the source

Do the recon in `zavod/docs/best_practices/source_recon.md` before writing any code. For a
PEP source, also:

- **Find the terms the source exposes** and whether dates are per person or per term.
  New terms: `zavod/docs/peps/index.md` → "Noticing a new term".
- **Citizenship:** spawn a subagent (`WebSearch`/`WebFetch`) to find the legal document
  (constitution, electoral law) that requires citizenship for this specific position.
- **Count the records by role and by term**, and compare with the seats the body
  actually has. Could a record's dates be stale (e.g. a re-elected member still carrying
  their previous term)?

**Checkpoint: show the user a short recon note and wait before writing code** —
endpoints and pagination; every field with its decision; the terms exposed; counts by
role and term against the seats; anything the source contradicts itself on, with the
simplest options. A wrong assumption corrected here costs one message; found in review
it costs a rewrite. In an automated run with no user, put the note at the top of your
final report and proceed.

## Step 2: Write the YAML and crawler

- Tags: `zavod/docs/metadata.md`. For `title`, `description` and `coverage.frequency`
  apply `/legislature-metadata` (legislatures), and `/dataset-metadata` for the rest.
- Constants (gender maps, headers, date formats, column labels) belong in the YAML,
  not the crawler — use `/crawler-constants-to-yml` if you've written one in code.

## Step 3: Validate

**A crawler that has not completed a successful `zavod crawl` is not deliverable.** If
the source can't be fetched, stop and report the blocker with the evidence from your
recon note — don't ship a parser validated against an archived copy.

Crawl, export and lint as in `.claude/docs/crawler-guide.md` → "Running and testing". The
run's issues must be clean.

**Check that the status is true, not just well-formed.** A wrong `current`/`ended`
status is the costliest PEP defect, and no validator or assertion catches it:

```bash
python .claude/skills/crawler-pep/scripts/pep_summary.py <dataset_name>
```

The `current` column should come close to each body's seats (for a multi-term source,
the sitting term). A bigger gap means the dates don't mean what the crawler assumes —
find which records are affected and why, from the source itself. Don't reconstruct
status from other endpoints (votes, rosters): if you think a workaround is needed, bring
the evidence and options to the user instead of building it.

Then run the integrity checks in `.claude/skills/crawler-pep/validation.md` (each
should print nothing), and review your diff against
`zavod/docs/best_practices/merge_checklist.md`. Include the `pep_summary.py` table in
your final report.
