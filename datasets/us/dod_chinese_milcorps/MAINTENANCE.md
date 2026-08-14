# Maintaining us_dod_chinese_milcorps

The dataset is maintained as `sanctions.csv` next to the crawler; edits are made
as pull requests against that file. There is no automated crawl of the PDF.

One row per listing: `Start date` is the date of the edition that added the
company, `End date` the date of the edition or notice that removed it. A company
that is listed, removed and listed again keeps one row per listing period — the
sanction is keyed on `Start date`, so the periods do not collide.

## Columns

- `Name`: the entry exactly as printed in the list, including any parenthesised
  aliases. Kept as the original value behind the cleaned name.
- `Clean Name`: the name alone, without alias or annotation. Keys the entity, so
  a changed name splits it — see the note on renames below.
- `Alias`: an alternative name; emitted as `weakAlias` when it is a short
  all-caps acronym.
- `Previous Name`: a former name given by the list ("formerly ...").
- `Parent Name`: the listed parent, when the list groups an entry under one. It
  is emitted as a separate company with an ownership link.
- `Note`: the justification given by the list, or maintainer context. Statutory
  citations are kept verbatim.
- `Start date` / `End date`: ISO dates of the editions that added and removed the
  entry.
- `Source Url`: the PDF the edition was transcribed from.

Values are single-line: the published PDF wraps text mid-sentence, so newlines
are collapsed to spaces when transcribing.

## Adding a new edition

1. Get the PDF linked from the press release and reconcile its entry count
   against the number the release claims.
2. For each entry not yet listed, add a row with `Start date` set to the release
   date and `Source Url` to the PDF.
3. For each entry that has dropped off, set `End date` on its open row rather
   than deleting the row.
4. Where the new list prints a different name for an entry that is plainly the
   same company, keep the existing `Clean Name` — changing it creates a second
   entity. The printed form is preserved in `Name`; record a genuinely new name
   in `Alias` or `Previous Name`. This applies to punctuation and capitalisation
   too: `AVIC JONHON ...` and `AVIC Jonhon ...` are two entities, not one.
5. Re-check `assertions` against the new company count.

## Handling "Assertion failed" warnings on the release list

The crawler hashes the DoD release list and warns when it changes. Review the
new release: if it announces an edition of the list, follow the steps above; if
it is unrelated, update `RELEASES_HASH` in `crawler.py` and the enumeration of
releases above it.

The release list is fetched through the Zyte API because defense.gov blocks
plain requests. Without an API key configured the check is skipped, so a local
run neither fails nor detects a new release.

## Designations between editions

Additions and removals made between editions are published in the Federal
Register but not always as a press release, so the release-list check does not
see them. They are covered by `manual_check` instead — see the message in the
dataset yml.
