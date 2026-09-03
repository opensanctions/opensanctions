# Extracting Swedish state-owned enterprise leadership

How to turn an edition of the government's annual report on the state's company
portfolio into rows in `leadership.csv`. The report PDF is the source of record.

Every edition so far has changed layout, so expect to write a new throwaway parser
rather than reuse one.

## What each edition publishes

| Edition | Title | Publishes |
| --- | --- | --- |
| 2024 | Verksamhetsberättelse för bolag med statligt ägande 2024 | Chair, CEO, board members, employee representatives, auditors |
| 2025 | Årlig information om bolag med statligt ägande 2025 | Chair and CEO only |

The *Verksamhetsberättelse* series ended after 2024. Its replacement carries board
data only as aggregate gender statistics. Watch for rosters returning in
machine-readable form; these were ruled out on 2026-09-03:

- `data.riksdagen.se` serves the companion skrivelse as structured data, but it has
  only aggregate board statistics, no names.
- Bolagsverket's free high-value datasets (API and bulk file) carry no
  representatives; "Statistik om företrädare" is aggregate only.
- Bolagsverket's paid company-information API does carry företrädare and would need
  ~38 lookups per run, but requires an agreement.

## Company pages

Company pages repeat a section header (`Bolag utan samhällsuppdrag` or `Bolag med
samhällsuppdrag`) and state the company's remit. The remit heading is
`Verksamhetsföremål`, except for Svenska skeppshypotekskassan, which is not a limited
company and uses `Verksamhetens ändamål`.

Map the page heading to the company name already in `leadership.csv` — a new spelling
creates a second `Company` entity and splits its board. Watch for report typos
(`RISE Reasearch Institutes of Sweden AB`), decorations the dataset omits (`SBAB Bank
(publ)`), and real renames (`SSC Space AB`, formerly Svenska rymdaktiebolaget), which
belong in the `previous_names` lookup.

## Reading names

Crop each name's column bounding box; don't read page text line by line. Names wrap
onto a second line, and line-based extraction silently truncated three of them in the
2025 report. Also de-hyphenate names split across lines (`Susanne Anders- son`).

## The CEO is prose, not a field

The chair is a labelled field, but the CEO is named only in a quote attribution:
`"…" – Caroline Arehult, VD`. Never take a footnoted or `tf` attribution as the
current CEO — read the footnote and record what it says, with the reason in `notes`.
Three of 37 were out of date in the 2025 report.

## Acting officeholders

Give an acting officeholder their own row on the same position as the substantive
holder; don't invent an "Acting CEO" position. Note that they were acting, and who
took over.

Add them only where the report names them. Search the whole PDF for `tf VD`, `t.f.`
and `tillförordnad`, since these appear in footnotes and prose, then check scope:
Industrifonden and Norrlandsfonden are foundations, not companies with state
ownership, and get no rows here.

## Verify before committing

- Compare each `(position, company)` slot against the previous edition. A changed
  occupant is usually a real appointment, but can be one person under two spellings —
  `Maria Håkansson` and `Maria Hammarskjöld Håkansson` were one person.
- Check surnames against the company's own site. The 2025 report gives Svenska Spel's
  CEO as `Anna Johansson`; the company spells it `Anna Johnson`.
- Scan for single-token names, stray punctuation, digits and double spaces.

Put a real former name in `alias`. Don't alias a typo — `Anna Johansson` is a common
Swedish name and would cause false matches. Note it in `notes` instead.

## Merging

The Google Sheet used until September 2026 is frozen history — edit the CSV.

One row per appointment, not per edition. When an edition re-confirms an appointment,
add its year to `report` and its PDF to `source_url`; don't add a second row. The
crawler rejects duplicate `(name, position, company)` keys and a `source_url` that
disagrees with `report`.

Rows for skipped positions (`Auditor`, `Employee Representative`) are kept so the
transcription stays faithful to the source.
