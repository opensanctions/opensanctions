# EU Journal sanctions op-log format

The CSV files under `regulations/` transcribe EU legal acts as operations on a
framework act's list of designations. Each file transcribes one act; each row
adds, modifies, or deletes one designation in one legal context. Replaying a
framework's files in order yields its list at any date, and dates each
designation by the acts that listed, changed, and removed it.

## Fidelity

Cells hold the source's printed wording. Transcription performs structural
extraction only: mapping printed labels to columns, splits dictated by the
source's own labels, list structure, or enumeration, dropping printed
placeholders (`na`, `n/a`, `n.a.`), collapsing whitespace, trimming a value's
trailing full stop, and stripping EUR-Lex change markers. Cell values are
never assembled from parsed pieces. Semantic normalization — date parsing,
country codes, identifier canonicalization, name categorisation beyond
printed labels — belongs to the consuming crawler; irregular names are
expected and go through its names review.

## Files

A framework's files live in `regulations/{framework}/`, named after the act
they transcribe, as bare uppercase CELEX identifiers
(`zavod.shed.ojeu.celex.normalize`):

- `{framework}.csv` — the framework act itself, all `ADD` rows;
- `{celex}.csv` — each act CELLAR relates to the framework (as amending it or
  as based on it); an act that changes no designation gets a header-only
  file, so coverage stays checkable;
- `{celex}R({nn}).csv` — an English corrigendum of the framework or of a
  related act.

The path is authoritative: rows carry no CELEX columns. Files are UTF-8,
comma-delimited, and start with this exact header:

```csv
op,id,publishedDate,effectiveDate,recordId,programKey,annex,measure,listingDate,endDate,schema,name,alias,weakAlias,previousName,country,nationality,jurisdiction,birthDate,birthPlace,position,passportNumber,gender,appearance,ethnicity,fatherName,motherName,incorporationDate,legalForm,sector,registrationNumber,taxNumber,idNumber,innCode,ogrnCode,kppCode,okpoCode,leiCode,swiftBic,ticker,imoNumber,mmsi,flag,address,phone,email,website,notes,extra,reason
```

## Operations

- `ADD` — a new designation: the full entry as printed. An `ADD` of a
  previously deleted `id` is a relisting and starts a new listing period.
- `MOD` — a changed entry, always the full row, never a diff. When the act
  replaces the whole entry ("the entry for X is replaced by the following"),
  the row is the new entry and a column it does not print is empty. When the
  act changes part of an entry — a corrigendum's "for … read …", or "in entry
  23, the date … is replaced by …" — every value it does not change is
  carried over from the target. A corrigendum gets a `MOD` for every entry it
  names, even where a later act already made the change.
- `DEL` — a removed entry. Only `op`, `id`, the two dates, `recordId`,
  `programKey`, `annex`, `measure`, `schema` and `name` are filled, carried
  from the live entry; `name` and `schema` must equal the live entry's.

An act that replaces a whole annex is a `MOD` for every entry it lists, a
`DEL` for each live entry it leaves out, and an `ADD` for each new one. An
entry moves to another annex or part only when the act restructures the
annex or transfers the entry; that is a `MOD` changing `annex`, so the
listing continues. A designee listed in a second annex is a separate
designation with its own `id`.

Replay applies files in `(effectiveDate, publishedDate, CELEX)` order and rows
in file order. A designation's key is `(id, programKey, measure)`: an `ADD`
of a live key and a `MOD` or `DEL` of a missing one are invalid. A view as of
a date applies every op with an `effectiveDate` on or before it and treats a
designation whose `endDate` lies before that date as lapsed.

## Columns

| Column | Content |
| --- | --- |
| `op` | `ADD`, `MOD`, or `DEL`. |
| `id` | The designation's ID, a bare lowercase 40-hex hash, repeated verbatim by every op on the designation. A new designation's is `make_entity_id(framework, recordId, name, key_prefix="eu_journal_sanctions")` over the printed number and the name as first listed. |
| `publishedDate` | The act's Official Journal publication date, ISO `YYYY-MM-DD`; the same on every row of a file. |
| `effectiveDate` | The date the provision making this op enters into force or applies, ISO; a corrigendum's is its publication date. |
| `recordId` | The entry number the framework's annex prints, as printed, or empty — never invent one. A `MOD` or `DEL` carries its target's unless the act moves, numbers or renumbers the entry; a renumbering is a `MOD` for each entry whose number changes. |
| `programKey` | Required. An OpenSanctions program key from `meta/programs/*.yml` (e.g. `EU-LBY`). |
| `annex` | The framework's annex the entry is listed in, never the amending act's own annex or sections: compact Roman numerals with dot-separated parts (`IV`, `XIX.A`). A part is the framework annex's printed letter or, for an unlettered part heading, that heading in capitals (`I.PERSONS`), as the framework's latest consolidation prints them — also for entries listed before the parts existed. An annex inserted by an amending act keeps its lowercase suffix (`Ia`). A `MOD` or `DEL` carries its target's unless the act moves the entry. |
| `measure` | Required. A measure from the `Measure` vocabulary in `zavod/zavod/stateful/programs.py` that the program lists. A designation under several programs or measures has one row per legal context, sharing its `id`. |
| `listingDate` | The printed per-designation listing date — for UN-mirror regimes the printed date of UN designation — or empty. |
| `endDate` | The printed end of a time-limited designation (`valid … to 26 July 2016` gives `26 July 2016`); a renewal is a `MOD` with the new date. |
| `reason` | The rationale for listing, as prose: one value per separately printed passage (grounds for listing, a narrative summary); the paragraphs of one passage join with spaces. |
| `extra` | Each labelled value no column fits, verbatim with its label (`Deadweight: 4,996 t`); kept for evidence, not emitted. |

## Entity columns

`schema` and `name` are required on `ADD` and `MOD`. `schema` is `Person`,
`Vessel`, `Asset`, `Company` (share-capital companies only), `Organization`
(any other group: state enterprises and agencies, militias, networks,
unions, self-proclaimed republics), or `LegalEntity` (when it is unclear
whether the target is a person or an organisation). Every filled entity
column must be a property of that FollowTheMoney schema. `Asset` is a
directly restricted thing that is neither entity nor vessel: a facility or
zone, with its location in `address`, or a named crypto-asset with only
`name`.

**Names.** Name cells keep the source's wording and casing. A value goes to
`alias`, `weakAlias` or `previousName` only on a printed label or structure:
`a.k.a.`, `alias` and `Good quality a.k.a.` mark an `alias`, `Low quality
a.k.a.` a `weakAlias`, `f.k.a.` and `formerly` a `previousName`; whether
separators inside a labelled group divide names is judged from the source.
Every further rendering the source prints on its own line — native script
or Latin transliteration — is an `alias`, and non-Latin renderings are
always kept. Never transliterate or translate a name; an unlabeled combined
form stays whole in `name`. A name printed in numbered parts
(`1: ABU 2: ZAYD`) is the parts joined with spaces.

**Countries.** Use the most specific column — `nationality` (a person's),
`jurisdiction` (where an entity is registered), `flag` (a vessel's) or
`country` — and keep the source's territory wording (`Flag State: Cameroon`
gives `Cameroon`). A place of birth goes in `birthPlace`, not a country
column, unless the source states a separate country.

**Identifiers and documents.** Use `birthDate` for people and
`incorporationDate` for entities. Put identifiers in the most specific
column (`innCode`, `ogrnCode`, `kppCode`, `okpoCode`, `leiCode`, `swiftBic`,
`ticker`, `imoNumber`, `mmsi`), else `taxNumber`, `idNumber` or
`registrationNumber`. Documents printed with an alias go to the
designation's own columns. A document's validity is not transcribed when it
can be separated (`Passport: A005465 (Libya), issued …, expires …` gives
`A005465 (Libya)`); printed inside the identifier's own parenthesis it
cannot, and the whole value is kept.

**Prose.** `legalForm`, `position`, `sector`, `appearance`, `ethnicity`,
`fatherName` and `motherName` hold the stated value. `notes` holds the
source's other-information prose about the entity — anything printed as
"Other information" — one value per printed line or paragraph, without its
label; labelled remarks inside a paragraph stay with it. Kin names and
lines naming other parties (`Associated individuals: …`) are not
transcribed.

## Values

- Cells are trimmed; a missing value is an empty cell, never a placeholder.
- `reason`, `extra` and every entity column except `name` are multi-valued,
  separated by `; `. A value containing `;` or `"` is CSV-quoted within the
  cell, quotes doubled (`"Foo; Bar"; Baz` holds two values). Values are
  non-empty and unique.
- Entity dates keep the source's wording (`Approximately 1952`).
  `listingDate` and `endDate` are bare dates: ISO partial (`2011-02-28`),
  dotted (`28.2.2011`), worded (`2 December 1985`) or UN-abbreviated
  (`26 Feb. 2011`).
- A file has no duplicate rows, and an `id` appears at most once per
  `programKey` and `measure` in it.

## Mapping to the Sanction

- `listingDate` is the printed `listingDate`, else the `ADD`'s `publishedDate`.
- `startDate` is the `ADD`'s `effectiveDate`; `endDate` the printed `endDate`.
- `modifiedAt` is the `publishedDate` of the latest `MOD`.
- The entity ID is `context.make_slug(id)`.
- Deleted and lapsed designations are not emitted.
