# FinCEN special measures details

`details.csv` is a manually maintained record of facts stated in the
rulemaking documents (findings, notices of proposed rulemaking, final rules)
linked from the [FinCEN special measures table](https://www.fincen.gov/resources/statutes-and-regulations/311-and-9714-special-measures):
addresses, aliases, identifiers, and the sub-entities the documents name.
The crawler reads the live table for the measures themselves and joins this
file for everything document-derived. Changes should be submitted as pull
requests.

When the table links a document that no `details.csv` row cites, the crawler
warns ("Unreviewed special measures document"); the runbook for resolving
that warning is in the `config.discovery` section of the dataset `.yml`.

## Columns

- `Measure` — the target name exactly as it appears in the table's first
  column (including footnote asterisks); this is the join key, and the crawl
  fails if it no longer matches a table row.
- `Relationship` — how the row applies to the measure's main entity:
  - `self` — a fact about the main entity itself (address, alias,
    registration number). `Name` repeats the measure string.
  - `target` — an entity covered by the measure but not the table entry
    (e.g. members of a "class" measure). Gets its own Sanction record with
    the parent measure's dates.
  - `subsidiary` / `owner` — creates the entity plus an Ownership link to /
    from the main entity.
  - `related` — creates the entity plus an UnknownLink; for parties the
    documents connect to the target without designating them.
- `Type` — FTM schema for non-`self` rows (Company, Organization, Person, …).
- `Name`, `Alias` — verbatim from the document; `Alias` is `;`-separated.
  No cleaning, recasing or translation.
- `Country` — country stated or clearly implied by the document.
- `Address` — verbatim postal address; one address per row, repeat rows for
  more (rows for the same entity merge during processing).
- `Registration number` — SWIFT/BIC, tax or company number, verbatim.
- `Topics` — `;`-separated FTM topics, set explicitly per row: entities the
  measure covers mirror the parent's status (`sanction` while a final rule is
  in force, `reg.warn` otherwise); documented-but-not-designated connections
  use `sanction.linked` or stay empty.
- `Notes` — at most a short, near-verbatim qualifier from the document.
  Never invented prose.
- `Source URL` — the document the fact was taken from, exactly as linked
  from the table. Every row must cite one; the set of cited URLs doubles as
  the record of which documents have been reviewed.

## Removed documents are not delistings

A document URL going dead on fincen.gov does not end a measure or invalidate
mined facts. Keep the rows and the original URL; if FinCEN replaces the link
(e.g. a public-inspection URL becoming a published /documents/ URL), move the
citations to the new URL in the same change.
