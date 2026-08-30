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
  - `target` — an entity the measure applies to that is not the table entry:
    a member of a "class" measure, or an affiliate the rule's definition of
    the target names (e.g. "CIBanco ... including Finanmadrid"). Gets its own
    Sanction record with the parent measure's dates.
  - `subsidiary` / `owner` — creates the entity plus an Ownership link to /
    from the main entity.
  - `controller` — like `owner`, but for a party the document says controls
    the main entity without stating ownership ("controlled and operated by");
    the Ownership link carries `role: control`.
  - `related` — creates the entity plus an UnknownLink; for parties the
    documents connect to the target without designating them.

  An entity may appear in several rows with different relationships: a
  subsidiary that the rule folds into the target has a `subsidiary` row (the
  ownership) and a `target` row (the coverage). Rows with the same `Measure`
  and `Name` merge into one entity.

  When a table row names a *class of transactions* rather than an entity we can
  represent (e.g. "Convertible Virtual Currency Mixing", "Mexican Gambling
  Establishments"), it is listed in the `skip_entity` lookup in the dataset
  `.yml`. The class itself is not emitted; only its `target` members are. A
  `subsidiary`/`owner`/`related` row under such a measure has no main entity to
  link to, so it is emitted on its own.
- `Type` — FTM schema for non-`self` rows (Company, Organization, Person, …).
- `Name`, `Alias` — verbatim from the document; `Alias` is `;`-separated.
  No cleaning, recasing or translation.
- `Country` — country stated or clearly implied by the document.
- `Address` — verbatim postal address; one address per row, repeat rows for
  more (rows for the same entity merge during processing).
- `Registration number` — SWIFT/BIC, tax or company number, verbatim.
- `Notes` — verbatim quotation(s) from the document only, each in double
  quotes, separated by `;`. Quote the sentence or clause that names the
  entity; trim at clause boundaries, never paraphrase, summarise, hedge or
  insert words (no "described as", no bracketed glosses). Fix only the
  source's character encoding (`Z[uuml]rich` → `Zürich`).
- `Source URL` — the document the fact was taken from, exactly as linked
  from the table. Every row must cite one; the set of cited URLs doubles as
  the record of which documents have been reviewed.

## Topics

Topics are never set per row; the crawler derives them from the relationship
and from the measure's current stage in the table:

- The main entity and every `target` carry the measure's status: `sanction`
  while a final rule or Section 9714 order is in force, `reg.warn` while only
  a finding or proposed rule exists, and no topic once the measure is
  rescinded (the Sanction record's end date is the history).
- `subsidiary`, `owner`, `controller` and `related` entities are `poi` for as long as the
  measure is live, and untagged once it is rescinded. FinCEN names them as
  part of its finding; it does not designate them, so they are never
  `sanction`, `sanction.linked` or `reg.warn` here. Derived topics such as
  `sanction.control` come from the graph analyzer, not from this file.

## Removed documents are not delistings

A document URL going dead on fincen.gov does not end a measure or invalidate
mined facts. Keep the rows and the original URL; if FinCEN replaces the link
(e.g. a public-inspection URL becoming a published /documents/ URL), move the
citations to the new URL in the same change.
