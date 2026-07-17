# Sanctions program metadata

This directory defines the sanctions programs referenced by dataset records. A
program describes the legal regime or policy under which designations are made;
it is not a description of the dataset that collects those designations.

## Adding a program

Create one `.yml` file per program. The filename must equal the program `key`,
for example `EU-RUS.yml` for `key: EU-RUS`. Before adding a key, check whether
an existing program already covers the same regime.

Use keys of the form `{ISSUER}-{TARGET}` or `{ISSUER}-{SHORTNAME}`. Keys are
uppercase and contain only letters, numbers, and hyphens. Once used in data, a
key is a stable identifier and should not be renamed merely to improve its
wording.

```yaml
title: Example authority measures concerning Russia
key: ZZ-RUS
url: https://authority.example/programs/russia
summary: Targets people and entities responsible for undermining peace in
  Russia. Designated people are subject to an asset freeze and travel ban.
issuer: zz_authority
dataset: zz_designations
aliases:
  - Resolution 1234
target_territories:
  - ru
measures:
  - Asset freeze
  - Travel ban
```

Do not add an `id` to new files. Existing IDs are transitional values retained
from an earlier metadata system.

## Fields

- `title`: Use the official English title where one exists. Otherwise use a
  concise, consistent English translation or a near-official descriptive title.
- `key`: Use the stable identifier described above. It must match the filename.
- `url`: Link to an authoritative public page from the authority responsible for
  the program. See the source guidance below.
- `summary`: In two to four sentences, explain who or what the program targets,
  why, and the principal measures it imposes. Describe the regime rather than
  the ingestion dataset.
- `issuer`: Reference a filename, without `.yml`, from `meta/issuers/`. Use the
  authority legally responsible for the program.
- `dataset`: Name the primary dataset that ingests designations under the
  program. If several datasets cover it, select the main one.
- `aliases`: List alternative program names, legal citations, resolutions, and
  well-established abbreviations. Do not use aliases for translated variants of
  designated entity names.
- `target_territories`: List lowercase ISO 3166-1 alpha-2 codes for territories
  targeted by a geographic regime. Omit this field for thematic programs that
  target people regardless of geography.
- `measures`: List only measures imposed by the program's legal basis. Values
  must come from the `Measure` vocabulary in
  [`zavod.stateful.programs`](../../zavod/zavod/stateful/programs.py).

## Choosing the program URL

The URL should identify the program itself, not merely provide related context.
Prefer sources in this order:

1. The issuing authority's stable program or current-list page.
2. The official legal instrument that establishes the program.
3. The issuing authority's stable index of designation and delisting notices.

Use the canonical HTTPS URL when available. Avoid search results, media reports,
third-party summaries, internal or working spreadsheets, and individual
designation notices when a stable program-level page exists.

Check the substance of the page as well as whether the URL resolves. A law can
be related to a program without establishing that program. For a program that
groups actions by several authorities, choose an overarching legal instrument
that covers all of them rather than a narrower authority-specific mechanism.
For a transposed regime, link to the transposing authority's own program page or
legal instrument.

## Style and validation

Use `.yml`, two-space indentation, and indented sequence items. Keep field order
consistent with the example above and wrap prose to match nearby files. Loading
`get_all_programs_by_key()` validates filenames, keys, issuers, territory codes,
measure names, and duplicate keys; repository commit hooks also validate YAML
syntax and style.
