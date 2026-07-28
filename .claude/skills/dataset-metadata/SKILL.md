---
name: dataset-metadata
description: Bring a dataset .yml's metadata in line with house conventions (title, summary, description, coverage, publisher, maintainer comments). Use when the user asks to fix, improve or standardise a dataset's metadata.
argument-hint: "[dataset .yml path]"
allowed-tools: Read, Edit, Glob, Grep, Bash
---

# Dataset metadata

Standardise the metadata of the dataset at $ARGUMENTS.

**The rules live in [`zavod/docs/metadata.md`](../../../zavod/docs/metadata.md) — read it
first and follow it.** This skill is only the procedure for applying it; when a rule is
unclear, read the doc, do not invent a convention. For a legislature/parliament PEP
dataset, take the `title`, `description` and `coverage.frequency` patterns from the
`/legislature-metadata` skill; the rest of this checklist applies as written.

## 1. Read

The `.yml`, then the crawler (`entry_point`, usually `crawler.py`) for scope facts:
what the source covers, what is deliberately skipped, which lookups are not plain type
lookups and what they do.

## 2. Check each field against the doc

- `title` — the doc's Title rules.
- `summary` — length and style per the doc's Summary rules.
- `description` — the doc's Description rules, including its keep-out list.
- `coverage.frequency` — the doc's house default for the dataset type.
- `tags` — present and plausible for list type and target countries.
- `publisher` — all subfields per the doc's Publisher section.
- `data` — `url` fetchable; `format` and `lang` per the doc's Source data section.

## 3. Maintainer comments

- Top-of-file `#` block: known failure modes, source quirks, recurring-warning runbooks.
- One short `#` comment above each non-type lookup: what it matches, why it exists.

Only write down evidence: things observed in the code, the source data, `issues.log`,
git history, or stated by the user. Never speculate, and never delete existing comments
that still hold. Keep user-facing prose (description) and maintainer notes (comments)
strictly apart.

## Verify

```bash
python -c "from pathlib import Path; from zavod.meta import load_dataset_from_path; d = load_dataset_from_path(Path('<yml path>')); print(d.name, '-', d.model.title)"
```

Re-read the edited fields: every factual claim in title/description traces to the
source or crawler, the summary length is in range, and no sourcing or mechanics
language remains in the description.
