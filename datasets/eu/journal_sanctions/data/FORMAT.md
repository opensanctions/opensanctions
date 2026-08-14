# EU Journal sanctions CSV format

The CSV files in this directory contain reviewed EU sanctions designations
transcribed from legal acts published in the Official Journal. Each row holds one
designated entity in one legal context. There are two file kinds:

- **amendment files** (`amendments/`) transcribe the additions and modifications
  made by one amending act; and
- **consolidated files** (`consolidated/`) contain a complete snapshot of a
  framework act's annexes, extracted from the consolidated version pinned in
  the dataset metadata.

Both kinds share the same sanctions-metadata and entity columns and differ only in
their leading CELEX provenance columns.

## File naming and headers

Files are UTF-8, comma-delimited, and start with the exact header for their kind —
same columns, same order, no extras.

Amendment files are named `{amendmentCelex}.csv` and use:

```csv
amendedCelex,amendmentCelex,recordId,programKey,annex,measure,startDate,reason,schema,name,alias,weakAlias,previousName,country,nationality,jurisdiction,birthDate,birthPlace,position,passportNumber,gender,incorporationDate,registrationNumber,taxNumber,idNumber,innCode,ogrnCode,kppCode,okpoCode,imoNumber,flag,address,phone,email,website
```

Consolidated files are named `{celex}.csv` after the framework act (e.g.
`32014R0833.csv`) — one file per framework, updated in place, with version
history in git — and use:

```csv
celex,recordId,programKey,annex,measure,startDate,reason,schema,name,alias,weakAlias,previousName,country,nationality,jurisdiction,birthDate,birthPlace,position,passportNumber,gender,incorporationDate,registrationNumber,taxNumber,idNumber,innCode,ogrnCode,kppCode,okpoCode,imoNumber,flag,address,phone,email,website
```

Every row in a file carries the same immediate source CELEX (`amendmentCelex` in
amendment files, `celex` in consolidated files), and that value equals the
filename stem.

## CELEX values

All CELEX cells hold exactly one bare, normalized, uppercase identifier — the
form produced by `zavod.shed.ojeu.celex.normalize`, such as `32026R1941`. EUR-Lex
URLs, `CELEX:` prefixes, lowercase forms, semicolon-separated lists, and
date-suffixed version identifiers such as `02014R0833-20260717` are invalid. Do not store source URLs anywhere; they are derived from the CELEX.

- Consolidated `celex` is the framework act itself; the date-suffixed consolidated
  version each snapshot was extracted from is pinned in the dataset YAML's
  `consolidation` lookup and updated in the same commit as the CSV.
- `amendedCelex` is the framework act whose annex is changed; `amendmentCelex` is
  the amending act the row was transcribed from. A row has exactly one
  `amendedCelex` — when one act amends several frameworks, or one designation
  applies to several programs or measures, repeat the whole entity row per
  distinct legal context.

## Sanctions metadata columns

| Column | Required | Content |
| --- | --- | --- |
| `recordId` | No | The entry identifier printed in the source annex. Empty when the act prints none — never invent one. |
| `programKey` | Yes | One OpenSanctions program key that resolves against `meta/programs/*.yml` (e.g. `EU-LBY`). |
| `annex` | When stated | The source's annex or section identifier, as compact Roman numerals with dot-separated parts: `IV`, `XIX.A`, `XLV.D`. |
| `measure` | Yes | One sanctions measure from the `Measure` vocabulary in `zavod/zavod/stateful/programs.py`, which must also appear in the selected program's `measures:` list. |
| `startDate` | No | The date the designation takes effect. Empty when the source does not establish it — never infer one. |
| `reason` | No | The source's rationale for listing, as prose (maps to `Sanction.reason`). |

There is no `unknown` value for any column; a row whose measure cannot be
classified is not ready for this directory. Amendment files encode positive state
only — additions and modifications, never removals — and there is no operation
column in either kind.

## Entity columns

`schema` and `name` are required. `schema` is one of `Person`, `LegalEntity`,
`Organization`, `Company`, `Vessel`, or `Asset`, and every populated entity
column must be a property that exists on that FollowTheMoney schema (e.g. `flag`
and `imoNumber` only on `Vessel`, `kppCode` only on `Company`).

The country columns are distinct — use the most specific one:

- `nationality` — a person's nationality;
- `jurisdiction` — where a legal entity is registered;
- `flag` — a vessel's flag state;
- `country` — a source association that fits no more specific property.

Country-valued cells preserve the territory wording exactly as the source states
it: `Flag State: Cameroon` produces `Cameroon`, not `cm`, and source distinctions
such as `Russian Federation` versus `Russia` are retained. A place of birth goes
in `birthPlace`, not a country column, unless the source states a separate
country.

Use `birthDate` for people and `incorporationDate` for legal entities. Put
identifiers in the most specific property the source supports (`innCode`,
`ogrnCode`, `kppCode`, `okpoCode`, `imoNumber`); use `taxNumber`, `idNumber`, or
`registrationNumber` only when no more specific system is established.

Use `Asset` for a directly restricted thing that is not a legal entity or vessel:
facilities and geographic zones use `name` with their published location in
`address` and an applicable `country`; a named crypto-asset may have only `name`.
Do not recast such targets as `LegalEntity` or `Organization`.

## Values and multiplicity

- Cells are trimmed. Missing values are empty cells — never placeholders such as
  `unknown`, `N/A`, or `-`.
- Every entity column except `name` is multi-valued, separated by `;` with a
  single space after the separator preferred: `Foo; Bar`. Elements must be
  non-empty and unique after trimming. All other columns — the CELEX columns,
  `recordId`, `programKey`, `annex`, `measure`, `startDate`, `reason`, `schema`,
  and `name` — are scalar and never split.
- Dates (`startDate`, `birthDate`, `incorporationDate`) are `YYYY`, `YYYY-MM`, or
  `YYYY-MM-DD` and must be calendar-valid.

## Row uniqueness

- Exact duplicate rows are invalid.
- A non-empty `recordId` may not repeat within the same amended act
  (`amendedCelex`, or the file's `celex` for consolidated files), `annex`,
  `programKey`, and `measure` unless the rows have different `schema` values.
- The same entity legitimately appears on multiple rows when its program,
  measure, or amended act differs.

## Validation

Every file must pass the validator before it is checked in:

```
python datasets/eu/journal_sanctions/validate_csv.py [CSV ...]
```

With no arguments it validates every CSV under `amendments/` and `consolidated/`.
It reports each violation with file, row, and column context and exits `0` on
success, `1` on validation failures, and `2` on usage errors. It runs entirely
offline. The validator checks structure only — it cannot verify that values match
the source act; that transcription fidelity, including exact country wording,
remains the reviewer's responsibility.
