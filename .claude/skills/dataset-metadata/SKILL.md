---
name: dataset-metadata
description: Bring a dataset .yml's metadata in line with house conventions (title, summary, description, coverage, publisher, maintainer comments). Use when the user asks to fix, improve or standardise a dataset's metadata.
argument-hint: "[dataset .yml path]"
allowed-tools: Read, Edit, Glob, Grep, Bash
---

# Dataset metadata

Bring the metadata of the dataset at $ARGUMENTS in line with
[`zavod/docs/metadata.md`](../../../zavod/docs/metadata.md). The doc holds the rules;
this skill is only the procedure. When a rule is unclear, read the doc again rather
than inventing a convention.

## 1. Read

The `.yml`, then the crawler (`entry_point`, usually `crawler.py`) for scope facts:
what the source covers, what is deliberately skipped, and what each non-type lookup
does. Then read `metadata.md` in full.

## 2. Apply

Go through `metadata.md` section by section and make the dataset's fields and `#`
comments match it. Expect most fixes to be removals: a claim you can't trace, a count,
a field listing, ETL mechanics. Don't add text the doc doesn't ask for. Leave fields
that already comply as they are.

For a legislature or parliament PEP dataset, take `title`, `description` and
`coverage.frequency` from the `/legislature-metadata` skill.

## Verify

```bash
python -c "from pathlib import Path; from zavod.meta import load_dataset_from_path; d = load_dataset_from_path(Path('<yml path>')); print(d.name, '-', d.model.title)"
```

Then re-read every edited field against the doc.
