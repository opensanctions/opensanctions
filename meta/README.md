# Metadata authoring conventions

This directory contains metadata shared across OpenSanctions datasets. The
guidance below covers sanctions programs in `programs/`; conventions for other
metadata types can be added here as they are developed.

## Sanctions programs

A program identifies the legal regime, government policy, or source sub-list
under which an entity is designated. Programs are law- and policy-centred: they
explain the scope and consequences of a designation. Dataset metadata is
source-centred and explains what OpenSanctions ingests, how it is maintained,
and where its coverage is limited.

The [`Program` model and `Measure` vocabulary](../zavod/zavod/stateful/programs.py)
are the authoritative definitions of the supported fields and values. This
guide explains the authoring choices that cannot be expressed by the schema
alone.

Create one `.yml` file per program. Its filename must equal its `key`, for
example `EU-RUS.yml` for `key: EU-RUS`. Before adding a key, check whether an
existing program already covers the same regime.

Use keys of the form `{ISSUER}-{TARGET}` or `{ISSUER}-{SHORTNAME}`. Keys are
uppercase and contain only letters, numbers, and hyphens. A key becomes a
stable identifier once referenced by data and must not be renamed merely to
improve its wording.

```yaml
title: Example authority measures concerning Ukraine
key: ZZ-UKR
url: https://authority.example/programs/ukraine
summary: Targets people and entities responsible for undermining Ukraine's
  sovereignty and territorial integrity. Designated persons are subject to an
  asset freeze, and natural persons are barred from entry or transit.
issuer: zz_authority
dataset: zz_designations
aliases:
  - Resolution 1234
target_territories:
  - ua
measures:
  - Asset freeze
  - Travel ban
```

Do not add an `id` to new files. Existing IDs are transitional values retained
from an earlier metadata system.

### Fields

- `title`: Use the official English title where one exists. Otherwise use a
  concise, consistent English translation or a near-official descriptive title.
- `key`: Use the stable identifier described above. It must match the filename.
- `url`: Link to an authoritative public page that identifies the program. See
  the source guidance below.
- `summary`: Explain the program's scope, rationale, and practical consequences
  using the summary policy below.
- `issuer`: Reference a filename, without `.yml`, from `issuers/`. Use the
  authority legally responsible for the program.
- `dataset`: Name the primary dataset that ingests designations under the
  program. If several datasets cover it, select the main one.
- `aliases`: List alternative program names, legal citations, resolutions, and
  well-established abbreviations. Do not use aliases for names of designated
  entities.
- `target_territories`: List lowercase territory codes for the territories a
  regime is directed at. Codes are validated against rigour's territory
  registry, which extends ISO 3166-1 alpha-2 with historical entities (e.g.
  `csxx` for Serbia and Montenegro) and the pseudo-territories `zz` (global)
  and `ip` (cyberspace). Use `zz` when worldwide reach is a defining feature
  of a thematic program, and `ip` for regimes aimed at cyber activity. Omit
  the field when geography says nothing useful about the program.
- `measures`: Use the controlled vocabulary to describe the operative legal
  effects of the program. See the measures policy below.

### Writing a summary

Write a neutral, stand-alone explanation for a reader arriving from an
individual sanctions record. Prefer two to four present-tense sentences, but
organise them naturally rather than following a fixed template. A complete
summary answers the applicable questions:

- Who or what can be targeted?
- What conduct, situation, geography, sector, or policy objective puts them in
  scope?
- What practical restrictions follow from designation?
- Does the regime combine designation-level measures with country- or
  sector-wide restrictions?
- Do the measures vary by designation in a way the reader needs to understand?

Name the distinctive basis of the regime rather than relying on generic phrases
such as "threats to national security". State the principal measures in plain
language and keep them consistent with the controlled `measures` values.

Do not use the program summary for:

- crawler behaviour, update frequency, data coverage, or completeness caveats;
- a chronology of legislation or designation announcements;
- legal citations that fit in `aliases` or the linked authoritative source;
- political rhetoric, promotional language, or unsupported interpretation; or
- a list of individual targets.

Those details belong in dataset metadata, source records, aliases, or the source
page itself.

### Selecting measures

`measures` is a program-level normalization of the restrictions imposed under a
regime. It supports consistent display and filtering; it is not free text, a
list of reasons for designation, or a transcription of every power mentioned
in an enabling law.

Record the operative set of measures. Include a measure when it is a standard
consequence of designation under the program or has been expressly imposed by
a program decision. Do not include every option in a general enabling law when
the program has not selected or used it. If different designations receive
different measures, record the union of the operative measures and make that
variation clear in the summary.

Use the most specific applicable term. Common boundaries are:

- `Arms embargo` is a blanket, country- or regime-wide prohibition;
  `Arms restrictions` applies targeted military-goods restrictions.
- `Arms restrictions` covers military-list items; `Export control` covers
  dual-use goods, technology, software, luxury goods, and internal-repression
  equipment. Use both when both scopes apply.
- `Asset freeze` follows a designated person or entity and prevents funds or
  economic resources being made available to them; `Financial restrictions`
  follows prohibited financial activities or market access.
- `Services ban` describes a standalone prohibition on professional or
  advisory services. Services ancillary to restricted goods remain part of the
  applicable goods measure.
- `Sectoral sanctions` is a fallback for restrictions defined at sector level
  that cannot be represented without loss by a more specific measure.
- `Travel ban` applies to natural persons and prohibits entry or transit.

The complete vocabulary and its definitions are maintained in the
[public sanctions program documentation](https://www.opensanctions.org/docs/programs/).
Verify measures against the program page or operative legal instrument rather
than inferring them from a title or from common sanctions practice.

### Choosing the program URL

The URL must identify the program itself, not merely provide related context.
Prefer sources in this order:

1. The issuing authority's stable program or current-list page.
2. The official legal instrument that establishes the program.
3. The issuing authority's stable index of designation and delisting notices.

Use the canonical HTTPS URL when available. Avoid search results, media reports,
third-party summaries, internal or working spreadsheets, and individual notices
when a stable program-level page exists.

Check the substance of the page as well as whether the URL resolves. A law can
be related to a program without establishing it. For a program grouping actions
by several authorities, choose an overarching legal instrument that covers all
of them rather than a narrower authority-specific mechanism. For a transposed
regime, link to the transposing authority's own program page or legal instrument.

### Style and validation

Use `.yml`, two-space indentation, and indented sequence items. Keep field order
consistent with the example and wrap prose to match nearby files. Loading
`get_all_programs_by_key()` validates filenames, keys, territory codes, measure
names, and duplicate keys; repository commit hooks also validate YAML syntax
and style.
