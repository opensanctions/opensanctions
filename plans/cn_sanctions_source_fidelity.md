---
description: Proposal for representing Chinese sanctions notices without editorial narrative or unsupported inferences.
date: 2026-09-15
tags: [cn_sanctions, data-model, provenance, legal-fidelity]
---

# Source-faithful China sanctions data

## Problem

`sanctions.csv` currently uses `Summary` for several different jobs: identifying a
person, explaining a measure, recording a later suspension, and documenting a source
conflict. The recent status update made this visible. Phrases such as “This is not a
delisting”, “the Chinese text is followed here”, and links to secondary reporting are
editorial assertions. They are useful working notes, but they are not text published
by a Chinese authority and should not be emitted as if they were source data.

The same row also cannot faithfully represent a sequence of status events. The 16
entities in MOFCOM Announcement 2025 No. 21 were suspended in May, continued in
August, and continued again in November. Replacing that sequence with one latest
paragraph loses the event history; adding more prose increases legal and maintenance
risk.

## Design principles

1. **Source fields contain source text only.** A quote is copied from the issuing
   notice in its published language. No translation, explanation, negation, or
   conclusion is added to the quote.
2. **Derived classifications stay in the detector.** An internal classification such
   as `suspend`, `cease`, or `remove` may help identify rows for review, but it is not
   committed as source data.
3. **Rows describe current state.** A designation row records the latest known state
   of that designation. Earlier notices may remain in the detector’s review history,
   but they are not projected as separate FTM objects.
4. **No deletion from absence.** A row is ended only by an explicit source event or an
   explicit expiry. A missing page, changed list snapshot, or cessation of one measure
   does not remove designation history.
5. **Provenance is first-class.** Every extracted value points to an official notice,
   notice identifier, and (where interpretation could matter) an exact source quote.

## Recommended model

Keep one row per designation and make that row describe the **latest known state**.
The row is not an event log. When a later official notice changes the state, update
the row’s current-state fields and retain the latest supporting official source. The
original designation source remains available through its own provenance fields or a
reviewed source inventory.

Add these columns to `sanctions.csv`:

| Column | Content | Source requirement |
| --- | --- | --- |
| `Notice ID` | Stable identifier for the original instrument, such as `MOFCOM-2025-21` | Reproducible key, not a legal assertion |
| `Notice title` | Title of the current source notice | Verbatim; CSV-only field |
| `Designation quote` | Exact passage establishing the designation and measure | Verbatim |
| `Current status (source)` | Exact status/action wording from the latest notice, such as `继续暂停相关措施` or `停止执行相关措施` | Verbatim; blank if no later status notice exists |
| `Current status date` | Effective date stated for that status | Directly stated in the notice |
| `Current status source URL` | Official notice that supports the current status | Official source only |
| `Current status quote` | Exact passage supporting the current status and target scope | Verbatim |

`Source URL` remains the original designation citation. `Notice title` is intentionally
kept in the CSV even though it has no direct FTM property. Existing `Summary` and
`Chinese summary` remain as empty compatibility columns so downstream CSV consumers
keep their positional schema; source text belongs in the quote fields. No history
columns and no separate event file are needed for the FTM-facing dataset.

## Treatment of the current problem cases

- **Suspensions and cessations:** update the current-state fields with the exact Chinese
  action phrase, effective date, URL and quote. Do not set `End date` for
  “停止执行相关措施” unless the notice explicitly removes the entity or establishes
  expiry of the designation itself.
- **Bank removals:** retain the designation rows and their explicit `End date`; put the
  removal notice in the current-status fields with the exact “移出反制清单” quote.
- **Beth Edler / Amber Dolan:** retain the name supported by the chosen source version
  in the designation table. Store the conflicting official-language version as a
  provenance conflict (URL plus exact quote), not as a narrative explanation in the
  entity row.
- **Jónas Haraldsson:** retain the embassy statement as the official source and
  record the identification as unresolved provenance unless an official source names
  him. Secondary reporting may remain in a review manifest, never in a source field.

## Migration and validation

1. Inventory every non-empty `Summary` and classify each sentence as verbatim source
   text, structured identity data, derived interpretation, or secondary provenance.
2. Move only exact source passages into `Designation quote` or `Current status quote`. Drop or
   relocate editorial prose to the review manifest and PR discussion.
3. Update the crawler so public notes use only exact source quotes. Do not emit
   current-status prose on the target entity.
4. Add validation that every quote-bearing row has an official URL, every current
   status has an action phrase and effective date when stated, and every `End date` is
   backed by an explicit removal or expiry notice.
5. Add a source-diff review step for official-language variants and changed pages. A
   content change creates a provenance conflict rather than silently overwriting text.

The first implementation should cover the recent MOFCOM and UEL status notices and the
Beth/Amber conflict. Once the model is accepted, migrate older summaries in batches.
This keeps the schema decision reviewable before undertaking a full historical rewrite.

## Projection into FollowTheMoney

The CSV is an input format; its columns do not become FTM properties automatically.
The crawler must choose a supported FTM property or preserve the value as provenance.
The current `Sanction` schema already provides a useful direct mapping:

| Source-faithful field | FTM property | Rule |
| --- | --- | --- |
| `Notice ID` | `Sanction.recordId` | Keep the reproducible authority/instrument identifier |
| `Notice title` | No direct FTM property | Retain in the CSV; do not put it in entity notes |
| `Designation quote` | `Sanction.provisions` or `Sanction.summary` | Emit the exact quote with its source language; use `reason` only for a reason explicitly stated by the issuer |
| `Current status (source)` | No direct FTM property | Keep the exact phrase in the CSV; do not translate it in the source data |
| `Current status quote` | `Sanction.provisions` or `Sanction.summary` | Emit the exact supporting quote when the latest state needs to be visible in FTM |
| `Current status source URL` | `Sanction.sourceUrl` | Include the latest-state citation; retain the original designation URL in the CSV |
| `List` | `Sanction.program` / `programId` | Existing program mapping |
| `Body` | `Sanction.authority` | Existing authority mapping |
| `Date` | `Sanction.listingDate` or `startDate` | Preserve the distinction between publication/listing and effective dates |
| `End date` | `Sanction.endDate` | Only explicit removal or expiry |
| `Source URL` | `Sanction.sourceUrl` | Include the original citation; add the current-status URL when the designation has changed state |

The existing crawler currently puts `Summary` on the target entity’s `notes`. That is
the wrong projection for a notice quote: it makes a measure explanation look like a
biographical fact. The migration should put exact source text on the `Sanction`
object and leave the target’s `notes` empty unless the notice actually publishes an
identity note.

`Sanction.status` describes the latest designation state, not its history. Use the
normalized FTM value only where it is an unambiguous projection of the source, such as
`suspended` for an explicitly suspended measure. For “stopped implementing measures
while the entity remains listed”, do not infer `inactive` or an `endDate`: retain the
exact current-status phrase and quote in the CSV, while leaving the designation
interval current in FTM. Explicit removals continue to project to `endDate` and the
normal inactive status derived from it.

The recommended first phase is therefore: source-faithful latest-state columns in the
CSV, exact quotes on `Sanction`, no editorial target notes, and no history or schema
changes in FollowTheMoney. The detector may retain prior notices internally to decide
which current-state values need review, but those notices are not emitted as FTM
objects.

## Open decisions

- Should `Source note` replace `Summary` immediately, or should the old column be kept
  for one compatibility cycle?
- Should exact quotes be Chinese only, or may an official English version be used when
  it is itself the selected source of record?
- Should prior notice URLs be retained in a separate detector manifest, or only in Git
  history and review notes after the latest-state row is updated?
