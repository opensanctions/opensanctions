# EU Journal sanctions op-log format

The CSV files under `regulations/` transcribe EU legal acts as operations on a
framework act's list of designations. Each file transcribes one act; each row
adds, modifies, or deletes one designation in one legal context. Replaying a
framework's files in order yields its list at any date, and dates each
designation by the acts that listed, changed, and removed it.

## Principles

Every choice of operation, column, and value follows from these; the rest of
this document states the consequences that need fixing in one way.

1. **A row states the law.** A row is what the framework's list, after the
   act, says about one designation. What an act removes is gone; what it
   does not touch persists; what it restates replaces what was there.
2. **Two subjects.** A value is about either the *designee* — who or what
   is restricted — or the *designation* — why, on what legal basis, since
   when, and where in the list. Each column belongs to one of them.
3. **Meaning decides; the label is evidence.** A value goes to the column
   whose meaning matches what the value asserts. The printed label usually
   says what that is; where the content contradicts the label, the content
   decides.
4. **Fidelity.** A cell holds the source's words. Transcription only cuts
   the text where the source marks a boundary — a label, a list, an
   enumeration, a separator — and removes typography: runs of whitespace,
   EUR-Lex change markers, a value's trailing full stop. Cell values are
   never composed from pieces, translated, or transliterated. Interpreting
   them — parsing dates, coding countries, canonicalizing identifiers,
   discarding placeholders such as `na` — belongs to the consuming crawler.
5. **Everything about the designee is modelled.** A value goes to the most
   specific column whose meaning fits. `extra` exists only to make a missing
   column visible: a value in it is an extraction failure that marks a
   column to add. Metadata about a value — a document's issue or
   expiry, a listing's review or amendment history — is not about the
   designee and is not transcribed, nor are the kinds the annex lists as
   not modelled.
6. **A designation continues while the acts address it as the same entry**,
   whether they amend, rename, renumber, move, or re-register it. It ends at
   its `DEL`. A new entry is a new designation.
7. **Where the source leaves room for judgement**, the defaults below apply,
   and a different reading goes to review rather than to a new rule.

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
op,id,publishedDate,effectiveDate,recordId,programKey,annex,measure,listingDate,endDate,schema,name,alias,weakAlias,previousName,country,nationality,jurisdiction,birthDate,birthPlace,position,passportNumber,gender,appearance,fatherName,motherName,incorporationDate,legalForm,sector,registrationNumber,taxNumber,idNumber,innCode,ogrnCode,kppCode,okpoCode,leiCode,swiftBic,ticker,imoNumber,mmsi,flag,address,phone,email,website,notes,related,extra,reason
```

## Operations

- `ADD` — a new designation, as printed. An `ADD` of a deleted `id` is a
  relisting and starts a new listing period.
- `MOD` — the designation's full row after the act, never a diff. Whatever
  the act restates replaces the old values, including values it leaves out;
  whatever it does not address — the rest of an entry a corrigendum or a
  partial amendment touches, or a field the act's layout has no place
  for — is carried over. A corrigendum gets a `MOD` for every entry it
  names, even where nothing changes.
- `DEL` — a removed designation. Only `op`, `id`, the two dates, `recordId`,
  `programKey`, `annex`, `measure`, `schema` and `name` are filled, carried
  from the live entry.

An act that replaces a whole annex is a `MOD` for every entry it lists, a
`DEL` for each live entry it leaves out, and an `ADD` for each new one.
Renumbering, moving an entry to another annex or part, and re-registering an
entity under another legal form are `MOD`s of the same designation. A
designee listed in a second annex is a separate designation with its own
`id`, unless the act transfers the entry.

Replay applies files in `(effectiveDate, publishedDate, CELEX)` order and rows
in file order. A designation's key is `(id, programKey, measure)`: an `ADD`
of a live key and a `MOD` or `DEL` of a missing one are invalid. A view as of
a date applies every op with an `effectiveDate` on or before it and treats a
designation whose `endDate` lies before that date as lapsed.

## Designation columns

| Column | Content |
| --- | --- |
| `op` | `ADD`, `MOD`, or `DEL`. |
| `id` | The designation's ID, a bare lowercase 40-hex hash, repeated verbatim by every op on it. A new designation's is `make_entity_id(framework, recordId, name, key_prefix="eu_journal_sanctions")` over the printed number and the name as first listed. |
| `publishedDate` | The act's Official Journal publication date, ISO `YYYY-MM-DD`; the same on every row of a file. |
| `effectiveDate` | The date the provision making this op enters into force or applies, ISO; a corrigendum's is its publication date. |
| `recordId` | The entry number the list prints for the designation at the time, as printed, or empty. |
| `programKey` | An OpenSanctions program key from `meta/programs/*.yml` (e.g. `EU-LBY`). |
| `annex` | Where the list prints the entry at the time: the framework's annex, never the amending act's own annex or sections, as compact Roman numerals with dot-separated parts — the printed letter, or an unlettered part heading in capitals (`IV`, `XIX.A`, `I.PERSONS`). An annex inserted by an amending act keeps its suffix (`Ia`). |
| `measure` | A measure from the `Measure` vocabulary in `zavod/zavod/stateful/programs.py` that the program lists. A designation under several programs or measures has one row per legal context, sharing its `id`. |
| `listingDate` | The printed listing date of the designation — for UN-mirror lists the printed date of UN designation — or empty. |
| `endDate` | The printed end of a time-limited designation; a renewal is a `MOD` with the new date. |
| `reason` | Why the designee is listed and on what legal basis: the statement of reasons, a narrative summary of reasons, the provisions it is listed under. One value per separately printed passage; the paragraphs of one passage join with spaces. |

`programKey`, `annex` and `measure` are required on every row.

## Designee columns

`schema` and `name` are required on `ADD` and `MOD`, and every filled entity
column must be a property of the row's FollowTheMoney schema.

**Schema** is what the designee is: `Person`, `Vessel`, `Company` (only when
a share-capital form is printed), `Organization` (a group that is not a
company: an agency, a state enterprise, an armed group, a network), `Asset`
(a restricted thing that is neither: a facility or zone, with its location
in `address`), or `LegalEntity` — the default whenever the source does not
settle which.

**Names** are what the designee is called. `name` is the name the entry is
listed under. `alias` holds every other name the designee is known by,
including each rendering of a name in another script; `weakAlias` names
too unspecific to identify by themselves (low-quality a.k.a., a nom de
guerre or battlefield name); `previousName` names the designee no longer
uses. A name the designee was formerly *listed* under is an `alias`.
Separate names are split only where the source separates them (a label, an
enumeration, a line of its own); an unmarked combined form stays whole. A
name printed in numbered parts (`1: ABU 2: ZAYD`) is the parts joined with
spaces.

**Identity** columns hold the designee's own facts: `birthDate` and
`birthPlace` for a person, `incorporationDate` for an entity; `nationality`,
`jurisdiction` (where an entity is registered), `flag` (a vessel's) or
`country` (any other link to a territory), chosen by the link the source
states. An identifier goes to the column of the scheme that issued it
(`innCode`, `ogrnCode`, `imoNumber`, …), else `passportNumber`, `idNumber`,
`taxNumber` or `registrationNumber`. Documents printed with an alias belong
to the designee too.

**Description:** `position` holds offices, titles and functions held;
`legalForm`, `sector`, `gender`, `appearance`, `fatherName`, `motherName`,
`address`, `phone`, `email` and `website` hold what their names say.
`notes` holds the rest of what the source says about the designee — the
"other information" — one value per printed line or paragraph, without its
label.

**`related`** holds each printed statement that names another party —
owners, associates, relatives, an entity the designee acts for — as printed
with its label, one value per statement. It is copied, not interpreted.

**`extra`** holds a labelled value no column fits, verbatim with its label
(see principle 5).

## Values

- Cells are trimmed; a missing value is an empty cell.
- `reason`, `related`, `extra` and every designee column except `schema`
  and `name` are multi-valued, separated by `; `. A value containing `;`
  or `"` is CSV-quoted within the cell, quotes doubled (`"Foo; Bar"; Baz`
  holds two values). Values are non-empty and unique.
- Designee dates keep the source's wording (`Approximately 1952`).
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

## Annex: not modelled

Values of these kinds are deliberately not transcribed, and are not written
to `extra`:

- licence numbers (`licenseNumber`);
- a vessel's deadweight (`deadweightTonnage`).
