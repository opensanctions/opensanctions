# Maintaining us_dod_chinese_milcorps

The dataset is maintained as `sanctions.csv` next to the crawler; edits are made
as pull requests against that file. There is no automated crawl of the PDF.

One row per company per edition or notice that prints it: `Start date` is the
publication date of that edition or notice, `End date` the publication date of
the edition or notice that removed the company. Older editions were transcribed
as one row per listing period, so not every edition has a row.

The crawler merges all rows of a company (same `Clean Name`) into one Sanction
spanning the listing:

- `listingDate` and `startDate` are the earliest `Start date`. A company is
  listed from the publication of the edition or notice that added it; Section
  1260H attaches no consequence to the listing itself.
- `endDate` is the latest row's `End date`: the latest row decides whether the
  company is still listed.
- `modifiedAt` is the latest edition whose row changes `Alias`, `Previous
  Name`, `Parent Name` or `Note`, or the removal; otherwise the listing date.

Topics follow `debarment_from` in the dataset config, the date the Section 805
procurement ban applies from: a listed company is `debarment` from that date and
`reg.warn` before it, and a removed company has no topic. When a statute or a
court order changes when the ban applies, update that date.

## Columns

- `Name`: the entry exactly as printed in the list, including any parenthesised
  aliases. Kept as the original value behind the cleaned name.
- `Clean Name`: the name alone, without alias or annotation, as printed in that
  edition. Keys the entity.
- `Alias`: an alternative name; emitted as `weakAlias` when it is a short
  all-caps acronym.
- `Previous Name`: a former name given by the list ("formerly ...").
- `Parent Name`: the listed parent, when the list groups an entry under one. It
  is emitted as a separate company with an ownership link.
- `Note`: the justification the list gives for the listing, emitted as the
  sanction's `reason`. Statutory citations are kept verbatim.
- `Removal Note`: the reason the removing edition or notice gives, verbatim,
  emitted as the sanction's `summary`.
- `Start date` / `End date`: ISO dates of the editions or notices that added and
  removed the entry.
- `Source Url`: the edition PDF or notice the row was transcribed from, emitted
  as the sanction's `sourceUrl`. Never blank.
- `Removal Url`: the edition PDF or notice that removed the entry, also emitted
  as the sanction's `sourceUrl`. Set whenever `End date` is.

Values are single-line: the published PDF wraps text mid-sentence, so newlines
are collapsed to spaces when transcribing.

## Adding a new edition

1. Get the PDF linked from the press release and reconcile its entry count
   against the number the release claims.
2. For each entry the edition prints, add a row with `Start date` set to the
   release date and `Source Url` to the PDF.
3. For each entry that has dropped off, set `End date` and `Removal Url` on every
   open row of that company rather than deleting the rows, and record any reason
   the list gives in `Removal Note`. Each edition is the complete list, so an
   entry whose name it no longer prints has dropped off even when no removal
   section names it. That includes a company the edition prints under a new
   name: its new-name row carries the listing on, with the old name as
   `Previous Name` where the edition says "formerly".
4. Transcribe names as the new list prints them, in both `Name` and `Clean
   Name`. Where an entry is clearly an existing entry (e.g. original start date) but includes a
   correction to the name, update the entry in-place. The entity ID might
   change since IDs are generated from names. That's ok and normal for a correction.
   The deduplication process will handle that as it normally does.
5. Re-check `assertions` against the new company count.

## Handling "Assertion failed" warnings on the release list

The crawler hashes the DoD release list and warns when it changes. Review the
new release: if it announces an edition of the list, follow the steps above; if
it is unrelated, just update `RELEASES_HASH` in `crawler.py` and the enumeration of
releases above it.

## Designations between editions

Additions and removals made between editions are published in the Federal
Register but not always as a press release, so the release-list check does not
see them. They are covered by `manual_check` instead — see the message in the
dataset yml.

Record a notice like an edition, dated by its Federal Register publication and
citing its `https://www.federalregister.gov/d/<document number>` page as
`Source Url` or `Removal Url`.
