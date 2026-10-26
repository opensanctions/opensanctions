---
name: legislature-metadata
description: Refactor the title, description and coverage frequency of a legislature/parliament PEP dataset .yml into the house style. Use when the user asks to improve or standardise the metadata of a members-of-parliament dataset.
argument-hint: "[dataset .yml path]"
allowed-tools: Read, Edit, Glob, Grep
---

# Legislature dataset metadata

Standardise the `title`, `description` and `coverage.frequency` of a PEP dataset that
covers members of a national (or subnational) legislature. Target file: $ARGUMENTS

General metadata rules live in [`zavod/docs/metadata.md`](../../../zavod/docs/metadata.md).
This skill defines the legislature-specific pattern for `title`, `description` and
`coverage.frequency` only. For every other field — `summary`, `publisher`, `data`,
tags, maintainer comments — follow the `/dataset-metadata` skill; don't touch them
here unless the user asked for a full metadata pass.

## 1. Read the crawler first

Open the dataset's `entry_point` (usually `crawler.py` next to the `.yml`) and note the
scope facts the description depends on:
- unicameral vs bicameral, and (for a bicameral body) which chamber this is;
- whether the dataset is current-only or also historical (does it crawl past terms?);
- deliberate exclusions (e.g. alternates or substitutes skipped).

## 2. Title → `<Country> Members of <Parliament name>`

Use the English country name and the legislature's common English name:
- `Georgia Members of Parliament`
- `Mongolia Members of the State Great Khural`
- `Japan Members of the House of Councillors`

For one chamber of a bicameral body, name the chamber
(`Romania Members of the Chamber of Deputies`). Keep any established acronym the file
already uses only if it reads naturally; prefer the plain form.

## 3. Description — the legislature pattern

The rules are in [`zavod/docs/metadata.md`](../../../zavod/docs/metadata.md) →
Description. One short paragraph, optionally two:

**Paragraph 1 — who the members are + institutional context.** Scope-prefixed
("Current members of …" / "Current and historical members of …"), naming the body
with its original-language name in parentheses and its chamber type, plus one clause on
its role ("… which holds the country's legislative power, including passing laws and
approving the budget."). Constitutional facts such as seat count, term length or election
method may be added; they are optional. For an SAR or subnational body say "the region's"
rather than "the country's".

**Optional paragraph 2 — scope notes.** Only when there is something substantive to
say: deliberate exclusions ("alternates are not included"), current-only vs historical
coverage, a bounded period.

### Worked example

```yaml
title: Georgia Members of Parliament
description: |
  Current members of the Parliament of Georgia, the country's unicameral national
  legislature. Its 150 members are elected under proportional representation for a
  four-year term and hold the country's legislative power, including passing laws,
  approving the budget, and overseeing the government.
```

## 4. `coverage.frequency: monthly`

Set `coverage.frequency` to `monthly` (leave `coverage.start` and any `schedule`
unchanged). If it is already `monthly`, leave it.

## Verify

Re-read the edited fields. Confirm any constitutional fact you stated against a source,
any scope note reflects what the crawler actually does, and no sourcing, mechanics,
or per-field language remains.

If the user asked for a full metadata pass, continue with the `/dataset-metadata`
checklist for the remaining fields.
