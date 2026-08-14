# Maintaining us_special_leg

The dataset is maintained as the CSV files in `data/` next to the crawler; edits
are made as pull requests against them. Each file holds exactly one act, report
or designation authority, and the crawler rejects a file that mixes several.

One row per entity per designation event: an entity named by successive editions
of the same list keeps one row per edition, keyed by `report-date`, and an entity
named by several acts appears in several files. Within one edition an entity must
appear only once, with all of its names collected in `aliases` — the crawler
rejects a name repeated within an edition.

## Columns

All files share the same header:

- `schema`: `Person` or `Company`.
- `name`: the name exactly as printed in the act, report or notice.
- `aliases`: additional names, separated by semicolons.
- `topics`: `debarment` for procurement exclusions, `sanction` for INKSNA.
- `program`: the full title of the act or list, verbatim. Every value must be
  matched in the `sanction.program` lookup in the yml; add a mapping when adding
  an act.
- `authority`: the body that issued the listing (e.g. United States Congress,
  U.S. Department of State).
- `reason`: the grounds for the listing, where the source states them.
- `report-date`: the edition the row was taken from (e.g. the fiscal year of a
  Section 1286 list). Emitted as the sanction's listing date.
- `start-date` / `end-date`: when the source gives explicit effective dates.
  INKSNA measures run for two years from the determination date.
- `country`: ISO 3166-1 alpha-2 code, or a value covered by the `type.country`
  lookup.
- `notes`: maintainer context, including judgment calls made while transcribing.
- `source_url`: the official act, report or notice the row was taken from;
  several URLs separated by semicolons.

## Adding a new act

Create a new file in `data/` with the same header, add the program title to the
`sanction.program` lookup with a new program key, describe the act in
`description`, and re-check `assertions`.

## Updating the Section 1286 list

Follow [SECTION_1286.md](SECTION_1286.md).

## Handling "Hash mismatch" warnings on the Federal Register API

The crawler polls the API for new INKSNA determinations and rewrites
`fr_notices.csv`. Read each new notice, append one `data/inksna.csv` row per
designated foreign person, then commit `fr_notices.csv` and update the hash in
`crawler.py`.
