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

**Read upfront.** These are the rules; this skill is the procedure for applying them,
and repeats only the few rules the docs don't cover yet:

1. `zavod/docs/peps.md` — the PEP model, properties, position naming, categorisation,
   occupancy dates and status, historical terms.
2. `.claude/docs/crawler-guide.md` — shared crawler patterns, YAML template, lookups.
3. `.claude/skills/crawler-pep/examples.md` → **"Reference crawler"** — a complete,
   reviewed crawler. Yours should look like it: the shortest code that still handles
   every source field explicitly. Each helper, constant, guard or extra request you
   add beyond it needs a reason a reviewer can see — first drafts fail review far more
   often from added machinery than from missing it.

Open the rest of `examples.md` when your source differs from the reference (mixed
datasets, several positions, multi-term, subnational). Ground the crawler
in these files, not in other crawlers: the codebase is old and many have drifted from
current practice.

## Step 1: Understand the source

Do this before writing any code. A wrong endpoint or a misread date produces a crawler
that looks finished and is worthless.

- **Find the underlying JSON/XML endpoint before parsing HTML** — page source, network
  calls, JS bundles. Parliament sites very often have an API behind the rendered page.
- **Prove the source is blocked before reaching for Zyte.** In order: a browser
  `http.user_agent`; a language cookie or `Accept-Language`; a format suffix (`.json`)
  or `Accept:` header. Zyte costs you `ci_test: false`.
- **Establish the pagination contract from the response** — the per-page cap, the
  total or `next` link. An API that silently caps page size truncates without error.
- **Enumerate every field** the source returns and decide each one: emit, or ignore.
- **Find the terms the source exposes** — a term switcher, an `ElectionId` parameter,
  a `/legislaturas` endpoint — and whether dates are per person or per term.
- **Wikidata QIDs:** check on Wikidata that the item is `instance of (P31): position`
  and `applies to jurisdiction (P1001)` matches the country; a plausible label is not
  enough. Never pass one QID to two positions — it becomes the entity ID, and they'd
  collapse into one.
- **Citizenship:** spawn a subagent (`WebSearch`/`WebFetch`) to find the legal document
  (constitution, electoral law) that requires citizenship for this specific position.
  Cite its URL in a comment next to `person.add("citizenship", ...)`, or next to its
  omission if not required.
- **Term-bounded source** (fixed mandates, per-term pages)? Note a *structural*
  signature (page URL, file name, term id) and fail in `crawl()` when it changes, so a
  new term can't go unnoticed.
- **Check the records against reality.** Count them by role and by term, and compare
  with the seats the body actually has. Could a record's dates be stale (e.g. a
  re-elected member still carrying their previous term)?

**Checkpoint: show the user a short recon note and wait before writing code** —
endpoints and pagination; every field with its decision; the terms exposed; counts by
role and term against the seats; anything the source contradicts itself on, with the
simplest options. A wrong assumption corrected here costs one message; found in review
it costs a rewrite. In an automated run with no user, put the note at the top of your
final report and proceed.

## Step 2: Write the YAML and crawler

- Tag `list.pep`. For `title`, `description` and `coverage.frequency` apply
  `/legislature-metadata` (legislatures), and `/dataset-metadata` for the rest.
- Base `assertions` bands on the entity counts the crawl actually emitted, not on the
  seat count: a multi-term crawl holds several cohorts and grows every election.
- Constants (gender maps, headers, date formats, column labels) belong in the YAML,
  not the crawler — use `/crawler-constants-to-yml` if you've written one in code.
- Pass `topics=` to `make_position` for positions the crawler names itself
  (`["gov.national", "gov.legislative"]`, …); omit it for positions read from the
  source, where the review system decides.
- Set every person property `make_occupancy` reads (`birthDate`, `deathDate`) before
  calling it, and emit the person after it — it adds `role.pep` to the person.
- For judicial positions, also add `role.judge` to the person's topics.
- Honorifics: `zavod/docs/best_practices/name_titles.md`. LLM-assisted or reviewed name
  cleaning is acceptable for PEP data (unlike sanctions): `zavod/docs/extract/names.md`.

## Step 3: Validate

**A crawler that has not completed a successful `zavod crawl` is not deliverable.** If
the source can't be fetched, stop and report the blocker with the evidence from your
recon note — don't ship a parser validated against an archived copy.

```bash
zavod crawl <path>               # then read the run's issues — they must be clean (see below)
zavod export <path>              # runs the dataset validators and assertions
contrib/lint_dataset.sh <path>   # ruff + mypy + pre-commit exactly as CI runs them
```

Use the lint script rather than bare `ruff`/`mypy`, which lack the repo config. A run's
issues and statements are in `data/datasets/<dataset>/_artifacts/<run version>/`
(`issues.json`), or directly in `data/datasets/<dataset>/` (`issues.log`) on older
zavod versions.

**Check that the status is true, not just well-formed.** A wrong `current`/`ended`
status is the costliest PEP defect, and no validator or assertion catches it:

```bash
python .claude/skills/crawler-pep/scripts/pep_summary.py <dataset_name>
```

The `current` column should come close to each body's seats (for a multi-term source,
the sitting term). A bigger gap means the dates don't mean what the crawler assumes —
find which records are affected and why, from the source itself. When some of the
source's dates are stale (e.g. re-elected members still carrying their previous term),
keep them and let `make_occupancy` derive the status anyway; explain the gap in a
maintainer comment in the YAML. Don't pass `status=` to paper over stale dates: an
explicit status skips `make_occupancy`'s checks entirely, so members who left long ago
or have died are still emitted. Don't reconstruct status from other endpoints (votes,
rosters): if you think a workaround is needed, bring the evidence and options to the
user instead of building it.

Then run the integrity checks in `.claude/skills/crawler-pep/validation.md` (each
should print nothing), and review your diff against
`zavod/docs/best_practices/merge_checklist.md`. Include the `pep_summary.py` table in
your final report.
