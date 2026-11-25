# EU Journal sanctions op-log format

The CSV files under `regulations/` transcribe EU legal acts as operations on a
framework act's list of designations. Each file transcribes one act; each row
adds, modifies, or deletes one designation in one legal context. Replaying a
framework's files in order yields its list at any date, and dates each
designation by the acts that listed, changed, and removed it.

## Fidelity and the cleaning boundary

Cells hold the source's printed wording. Transcription performs structural
extraction only: mapping printed labels to columns, splits dictated by the
source's own labels, list structure, or enumeration, dropping printed
placeholders (`na`, `n/a`), collapsing whitespace, and stripping EUR-Lex
change markers. Semantic normalization — date parsing, country codes,
identifier canonicalization, and any name categorisation beyond printed
labels — belongs to the consuming crawler, via `type.*` lookups, the `dates`
configuration, and the names review system.

## Files

A framework's files live in `regulations/{framework}/`, named after the act
they transcribe:

- `{framework}.csv` — the framework act itself, all `ADD` rows;
- `{celex}.csv` — each act CELLAR relates to the framework (as amending it or
  as based on it). An act that changes no designation gets a header-only
  file, so coverage stays checkable;
- `{celex}R({nn}).csv` — a corrigendum, as `MOD` rows.

Names are bare, normalized, uppercase CELEX identifiers, the form produced by
`zavod.shed.ojeu.celex.normalize`. The path is authoritative: rows carry no
CELEX columns, and source URLs are derived from the CELEX.

Files are UTF-8, comma-delimited, and start with this exact header:

```csv
op,id,publishedDate,effectiveDate,recordId,programKey,annex,measure,listingDate,schema,name,alias,weakAlias,previousName,country,nationality,jurisdiction,birthDate,birthPlace,position,passportNumber,gender,appearance,ethnicity,fatherName,motherName,incorporationDate,legalForm,sector,registrationNumber,taxNumber,idNumber,innCode,ogrnCode,kppCode,okpoCode,leiCode,swiftBic,ticker,imoNumber,mmsi,flag,address,phone,email,website,notes,reason
```

## Operation columns

| Column | Content |
| --- | --- |
| `op` | `ADD`, `MOD`, or `DEL`. |
| `id` | The designation's ID, a bare lowercase 40-hex hash. Set on its first `ADD` and repeated verbatim on every later op, so renames and renumbering never change it. |
| `publishedDate` | The act's Official Journal publication date, ISO `YYYY-MM-DD`. Same on every row of a file. |
| `effectiveDate` | The date the provision making this op enters into force or applies, ISO. Usually the same across a file; differs where the act's final article says so. |

A new designation's `id` is
`make_entity_id(framework, recordId, name, key_prefix="eu_journal_sanctions")`
over the printed number and the name as first listed — the crawler's entity ID
formula, without the dataset prefix.

## Operations

- `ADD` — a new designation: the full entry as printed. An `ADD` of a
  previously deleted `id` is a relisting and starts a new listing period.
- `MOD` — a replaced entry: the full replacement as printed, never a diff.
- `DEL` — a removed entry. Only `op`, `id`, the two dates, `recordId`,
  `programKey`, `annex`, `measure`, `schema` and `name` are filled; `name` and
  `schema` must equal the live entry's.

An act that replaces a whole annex is a `MOD` for every entry it lists, a
`DEL` for each live entry it leaves out, and an `ADD` for each new one.

Replay applies files in `(effectiveDate, publishedDate, CELEX)` order and rows
in file order; a view as of a date applies every op with an `effectiveDate`
on or before it. A designation's key is `(id, programKey, measure)`. An `ADD`
of a live key and a `MOD` or `DEL` of a missing one are invalid.

## Sanctions metadata columns

| Column | Required | Content |
| --- | --- | --- |
| `recordId` | No | The entry number the act prints, as printed. Acts that add entries number them locally (`1.`, `2.`, …). A `MOD` or `DEL` that prints none takes its target's. Never invent one. |
| `programKey` | Yes | One OpenSanctions program key that resolves against `meta/programs/*.yml` (e.g. `EU-LBY`). |
| `annex` | When stated | The source's annex or section identifier, as compact Roman numerals with dot-separated parts: `IV`, `XIX.A`, `XLV.D`. An annex inserted by an amending act keeps its printed lowercase suffix: `Ia`. |
| `measure` | Yes | One sanctions measure from the `Measure` vocabulary in `zavod/zavod/stateful/programs.py`, which must also appear in the selected program's `measures:` list. |
| `listingDate` | No | The source's printed per-designation listing date, date-only — for regimes mirroring a UN list, the printed date of UN designation. Empty when the source prints none — never infer one. |
| `reason` | No | The source's rationale for listing, as prose: one value per separately printed passage (e.g. grounds for listing and a narrative summary of reasons). Consecutive paragraphs of one passage join into one value. |

A designation in several programs or under several measures has one row per
legal context, sharing its `id`.

## Mapping to the Sanction

- `listingDate` is the printed `listingDate`, else the `ADD`'s `publishedDate`.
- `startDate` is the `ADD`'s `effectiveDate`.
- `modifiedAt` is the `publishedDate` of the latest `MOD`.
- The entity ID is `context.make_slug(id)`.
- Deleted designations are not emitted.

## Entity columns

`schema` and `name` are required on `ADD` and `MOD`. `schema` is one of
`Person`, `LegalEntity`, `Organization`, `Company`, `Vessel`, or `Asset`, and
every populated entity column must be a property that exists on that
FollowTheMoney schema (e.g. `flag` and `imoNumber` only on `Vessel`,
`kppCode` only on `Company`).

Name cells preserve the source's wording and casing. A value is categorised
into `alias`, `weakAlias`, or `previousName` only when the source prints a
label or structure that says so: `a.k.a.`, `alias` and `Good quality a.k.a.`
mark an `alias`, `Low quality a.k.a.` a `weakAlias`, `f.k.a.` and `formerly`
a `previousName`. Never split a name on punctuation alone or extract
transliteration variants; an unlabeled combined form stays whole in `name`.
Irregular values are expected — the consuming crawler routes names through
the review helpers, where a human categorises them.

The country columns are distinct — use the most specific one:

- `nationality` — a person's nationality;
- `jurisdiction` — where a legal entity is registered;
- `flag` — a vessel's flag state;
- `country` — a source association that fits no more specific property.

Country-valued cells preserve the territory wording exactly as the source
states it: `Flag State: Cameroon` produces `Cameroon`, not `cm`. A place of
birth goes in `birthPlace`, not a country column, unless the source states a
separate country.

Use `birthDate` for people and `incorporationDate` for legal entities. Put
identifiers in the most specific property the source supports (`innCode`,
`ogrnCode`, `kppCode`, `okpoCode`, `leiCode`, `swiftBic`, `ticker`,
`imoNumber`, `mmsi`); use `taxNumber`, `idNumber`, or `registrationNumber`
only when no more specific system is established.

`legalForm` holds the source's stated type or legal form of a legal entity.
`position` holds a stated title or function. `appearance` holds
physical-description prose, `ethnicity` the stated ethnic background, and
`fatherName` and `motherName` a stated parent's name. `sector` holds the
source's stated business sector of a legal entity.

`notes` collects the source's other-information prose about the entity
itself when it fits no structured column, one value per printed line or
paragraph. Notes values are bare prose: a printed label is the instruction
for which column the value belongs in and is never part of any cell value —
`Other information: Owns a villa.` produces the notes value `Owns a villa.`
A labelled value that no column fits is not transcribed; cell values are
never assembled from parsed pieces. Identity-document validity lines
(`Date of issue: …`, `Expiration date: …`), kin names (`Brother's name: …`),
and relational lines that name other parties (`Associated individuals: …`)
are not transcribed.

Use `Asset` for a directly restricted thing that is not a legal entity or
vessel: facilities and geographic zones use `name` with their published
location in `address` and an applicable `country`; a named crypto-asset may
have only `name`.

## Values and multiplicity

- Cells are trimmed. Missing values are empty cells — never placeholders such
  as `unknown`, `N/A`, or `-`.
- `reason` and every entity column except `name` are multi-valued, separated by `;` with a
  single space after the separator preferred: `Foo; Bar`. A value that itself
  contains `;` or `"` is CSV-quoted within the cell, with embedded quotes
  doubled — `"Foo; Bar"; Baz` holds two values — so cells decode losslessly
  with a `;`-delimiter CSV parser. Elements must be non-empty and unique.
  All other columns are scalar and never split.
- Entity date columns preserve the source's wording (`4 Apr. 1944`,
  `Approximately 1952`); the crawler normalizes them. `listingDate` must be a
  bare date: ISO partial (`2011-02-28`), dotted (`28.2.2011`), worded
  (`2 December 1985`), or UN-abbreviated (`26 Feb. 2011`).
- Exact duplicate rows are invalid, and an `id` appears at most once per file.

## Validation

Every stack must pass `tools/validate.py` before it is checked in. Errors break this contract or the replay and fail the run; warnings flag rows
to re-check. `--online` also compares the files with the acts CELLAR relates
to the framework: a file for an unrelated act is an error, and a related act
without a file is a warning.
