# Extracting Chinese designations

This guide describes how to turn an official Chinese government notice into rows
in `sanctions.csv`. The notice is the source of record: search results,
translations, and secondary reporting may help interpret it, but each designation
must cite an official source.

## Establish what the notice does

Read the notice body and any linked annexes before extracting names. Record:

- the issuing authority;
- the measure or list being changed;
- whether targets are added, removed, suspended, or amended;
- the effective date, which may differ from the page publication date; and
- the number of targets stated by the notice.

Treat numbered annex entries as part of the notice. Do not assume that the article
body contains the complete list.

An unavailable source page is not evidence of delisting. Preserve existing rows and
their source URLs unless an official notice explicitly ends or changes the measure.

## Add one row per designation event

Each row represents a designation, not a unique entity. Add a new row when the same
person or organisation is designated again under a different measure or on a
different date. Duplicate listings are expected: entities are merged downstream,
while the rows retain the provenance of each designation event.

For additions, create one row for every named target. For removals or expiries,
update `End date` on the matching designation row rather than deleting its history.
Do not infer a removal from changed wording, an inaccessible URL, or absence from a
later notice.

## Map the CSV fields

Keep the existing header and column order.

| Field | Extraction rule |
| --- | --- |
| `Type` | Use `Person` for natural persons, `Company` for commercial legal entities, and `Organization` for other bodies. |
| `QID` | Add a Wikidata identifier only when the identity is certain. |
| `Name` | Prefer the official Latin-script name published by the authority. |
| `Alias` | Put additional Latin-script names in one field, separated by semicolons. |
| `Chinese name` | Preserve the name as written in the Chinese notice. |
| `Country` | Use the target's country when it can be established from the notice. Prefer an ISO 3166-1 alpha-2 code. |
| `Topics` | Map the measure using the table below. Separate multiple topics with semicolons. |
| `Summary` | Add only source-supported context that materially identifies the designation. |
| `Chinese summary` | Chinese equivalent of `Summary`, when available. |
| `Body` | Use the English name of the issuing authority. |
| `List` | Use the source-facing list name mapped below, not the internal program key. |
| `Date` | Use the effective date in `DD.MM.YYYY` format. |
| `End date` | Set only when an official action ends the designation. |
| `Source URL` | Use the exact official notice URL. Every row must have one. |
| `Address` | Preserve an address published for that target, including its postal code. |

Use these established mappings:

| Authority and measure | `Body` | `List` | `Topics` |
| --- | --- | --- | --- |
| MOFCOM Export Control List | `Ministry of Commerce` | `List of Export Controls` | `export.control` |
| MOFCOM Export Control Watch List | `Ministry of Commerce` | `Export Control Watch List` | `export.control` |
| MOFCOM Unreliable Entity List | `Ministry of Commerce` | `UEL` | `sanction.counter` |
| MFA countermeasures | `Ministry of Foreign Affairs` | Use the value from comparable MFA rows | `sanction.counter` |
| Taiwan Affairs Office measures | Use the authority stated by the notice | Use the applicable existing value, if any | `sanction` |

The metadata maps these source-facing list values to internal program identifiers.
Do not put program identifiers such as `CN-ECL` or `CN-UEL` in the CSV.

## Preserve source names and structure

When a notice publishes both Chinese and English names, use those forms directly.
Do not silently replace an unusual official spelling. A well-supported corrected
form may be used as `Name` while retaining the published form as an alias.

Do not invent translations, aliases, identifiers, addresses, or corporate
relationships. If one numbered entry explicitly names a parent and several branches
or subsidiaries as targets, create rows for each named legal entity. Do not split a
descriptive address or business unit into a separate target unless the notice treats
it as designated.

Keep addresses in the language used by the notice. Quote CSV fields containing ASCII
commas. Chinese punctuation does not itself require CSV quoting.

## Validate the extraction

First validate the file structure:

```bash
qsv validate sanctions.csv
```

Then verify notice-specific invariants:

- the number of rows using the source URL matches the number of extracted targets;
- every row has the same expected authority, list, topic, and effective date;
- names and addresses correspond one-to-one with the source entries; and
- no pre-existing rows were changed unintentionally.

Finally run the crawler and inspect its issues. This checks dates, countries,
program mappings, entity schemas, and unexpected CSV columns:

```bash
zavod crawl --clear-data cn_sanctions
```

Review the complete diff before committing. If interpretation was required, explain
it in the pull request rather than encoding unsupported assumptions in the data.
