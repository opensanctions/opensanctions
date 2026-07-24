---
name: legislature-metadata
description: Refactor the title, description and coverage frequency of a legislature/parliament PEP dataset .yml into the house style. Use when the user asks to improve or standardise the metadata of a members-of-parliament dataset.
argument-hint: "[dataset .yml path]"
allowed-tools: Read, Edit, Glob, Grep
---

# Legislature dataset metadata

Standardise the `title`, `description` and `coverage.frequency` of a PEP dataset that
covers members of a national (or subnational) legislature. Target file: $ARGUMENTS

Change **only** these three fields. Leave `summary`, `publisher`, `data`, `assertions`,
`lookups` etc. untouched unless the user asks.

## 1. Read the crawler first

Open the dataset's `entry_point` (usually `crawler.py` next to the `.yml`). The
description's field list must match what the crawler *actually emits* — read the
`person.add(...)`, `h.apply_name(...)`, `occupancy.add(...)` calls, not the source's
raw columns. Emitting `constituency`, `politicalGroup`, `birthPlace` etc. only counts
if it reaches an emitted entity.

Also note from the crawler / `.yml`:
- unicameral vs bicameral, and (for a bicameral body) which chamber this is;
- seat count, term length, and how members are elected (e.g. proportional
  representation, by district, appointed);
- whether the dataset is current-only or also historical (look for
  `earliest_term_start` / a PEP look-back cutoff → "Current and historical").

## 2. Title → `<Country> Members of <Parliament name>`

Use the English country name and the legislature's common English name:
- `Georgia Members of Parliament`
- `Mongolia Members of the State Great Khural`
- `Japan Members of the House of Councillors`

For one chamber of a bicameral body, name the chamber
(`Romania Members of the Chamber of Deputies`). Keep any established acronym the file
already uses only if it reads naturally; prefer the plain form.

## 3. Description — two short paragraphs, content only

Follow the `description` guidance in
[`zavod/docs/metadata.md`](../../../zavod/docs/metadata.md) and the house pattern
below. Describe **what is in the dataset** — the people and their data — not how it
was fetched.

**Paragraph 1 — who the members are + institutional context.** Scope-prefixed
("Current members of …" / "Current and historical members of …"), naming the body
with its original-language name in parentheses, then the defining facts: chamber type,
seat count, how and for how long members are elected, and one clause on the chamber's
role ("… and hold the country's legislative power, including passing laws and approving
the budget."). For an SAR or subnational body say "the region's" rather than "the
country's".

**Paragraph 2 — the per-member data.** "This dataset records each [member] with their
…" then list exactly the attributes the crawler emits (name, gender, date/place of
birth, party, parliamentary group, constituency, …). Keep genuinely substantive scope
notes (e.g. "alternates are not included", "not every profile carries the full set of
fields", "membership is treated as current on each run"). Give the original-language
term in parentheses where the source uses one (e.g. parliamentary group (bancada)).

**Strip** — provenance and mechanics belong in `publisher` / `data.url`, not here:
- "sourced from / as published on the official website / member API";
- anti-bot, browser-proxy, pagination, join-key or rolling-window details;
- PEP after-office cutoffs and other internal plumbing.

### Worked example

```yaml
title: Georgia Members of Parliament
description: |
  Current members of the Parliament of Georgia, the country's unicameral national
  legislature. Its 150 members are elected under proportional representation for a
  four-year term and hold the country's legislative power, including passing laws,
  approving the budget, and overseeing the government.

  This dataset records each seated member with their name, gender, and date and place
  of birth.
```

## 4. `coverage.frequency: monthly`

Set `coverage.frequency` to `monthly` (leave `coverage.start` and any `schedule`
unchanged). If it is already `monthly`, leave it.

## Verify

Re-read the edited fields. Confirm every attribute named in paragraph 2 is actually
emitted by the crawler, the seat count / term / election method match the source, and
no sourcing or mechanics language remains.
