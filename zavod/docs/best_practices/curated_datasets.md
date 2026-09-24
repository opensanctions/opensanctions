# Curating a dataset in the repository

A curated dataset is built from a CSV in the dataset directory, such as `datasets/cn/sanctions/sanctions.csv`. The crawler reads it row by row and maps each row to FollowTheMoney entities, and the rows themselves only change through reviewed pull requests.

Curate when the source publishes designations only as prose — a ministry notice, an annex to a regulation, a PDF edition of a list — and there is no structured document to parse instead.

The file is tracked in git, so every change to the data carries an author, a date and a diff, and a reviewer can see which rows a pull request touches before it is merged.

## File and column conventions

### Every cell is verbatim or a metadata column

The metadata columns are the ones extraction assigns: e.g. `type`, `topics`, the program key, the QID. Every other cell holds the source's own text, unmodified.

Cleaning belongs downstream, in the crawler and in [datapatch lookups](datapatch_lookups.md) or name-cleaning Data Reviews, where one option corrects every row carrying the value and the correction re-runs on the next crawl. A value tidied up in the file applies to that row alone, fixed to whichever model extracted it, and nothing can re-derive it when the cleaning rules change.

### Split a corrected column into `x_original` and `x_clean`

When the raw value is not usable, add a second column rather than overwriting the first. `x_original` holds what the source prints, `x_clean` holds the value to use — both carry a value on every row, so when the raw value is already valid, `x_clean` simply repeats it unchanged. On the parsing side, the crawler uses the `_clean` value and passes `_original` as `original_value=` only when the two differ: `entity.add("name", clean, original_value=raw if raw != clean else None)`. Passing it unconditionally would record an original statement identical to the value on every uncorrected row.

Use the same two suffixes on every column that takes corrections, so a reviewer can tell which values came from the source without reading the crawler. To explain a correction, use the file's shared `notes` column rather than adding a notes column per corrected field.

### One row per designation, not per entity

Normally, the same person designated twice under different measures — or re-listed after a removal — gets a row for each designation. The crawler can then emit one `Sanction`, or one `Occupancy` for a term of office, straight from the row. Entities are merged downstream, while the rows keep the program, the dates and the source for each designation.

Keep to a row per designation even when the source publishes notices, as far as that is practical. A row per notice forces the crawler to group events under a common key to reconstruct each designation — which produces duplicate entities whenever the key is imperfect, and makes the file harder to review.

One row per entity leaves nowhere to record the date a single designation ended, so end dates and removals cannot be expressed at all.

### Every row cites its official source

The source URL is mandatory and must never be blank: without one, a row cannot be checked against the document it was extracted from, and there is nothing to re-fetch if the extraction is questioned.

For event sources, the row also carries the notice's identity — the issuing authority and its decree or announcement number. The URL locates the notice on the current site; the decree number still identifies it after the site is redesigned.

### Name columns in snake_case, after the source's own terms

Use snake_case English, and name each column for what the source calls it — when the source labels a field `explanation`, the column is `explanation`, not `notes`. The crawler maps it to the FollowTheMoney property, so the transcription stays a faithful representation of the source.

### Separate multiple values with a semicolon

Pack multiple values into one cell with `;`, and split them in the crawler. Keep the same delimiter across every curated dataset.

## Updating the file as the source changes

### Discovery is a separate step from extraction

Extraction turns one document into rows. Discovery is the separate job of finding which documents exist that no row and no review record yet accounts for.

A discovery step needs to define what gets polled, how it detects edits as well as additions, and how a candidate gets marked out of scope. Skip that last part, and the same false positive gets raised on every crawl. `cn_sanctions` keeps that acknowledgement as `discovery.reviewed_urls` in its metadata; `us_bis_mieu` keeps it as `discovery.reviewed_versions`.

Discovery produces a review queue and does not write to the CSV. Deciding who is designated, under which program and from when, requires reading the official document, so that write has to happen as a pull request. [Change detection](change_detection.md) covers the sentinel helpers to build the polling step from.

### Every dataset needs a removal mechanism

Detecting that a designation has ended matters as much as detecting a new one. Which mechanism applies depends on what the source publishes.

- **Consolidated sources** republish the complete current list, so a row vanishing from the source is the signal. Discovery compares the source's current membership against the reviewed file; without that comparison, the dataset only ever records additions and grows with every update.
- **Event sources** publish a stream of notices, so a removal is its own notice, extracted like any other event. Absence from a later notice is not a removal, and neither is a URL that stops resolving.

Set the end date rather than deleting the row. Deleting the row throws away the record of which notice designated the entity and when, while both the next extraction and the next review depend on reading that.

### Write an extraction script only where it helps

The default is no script: read the document, write the rows. When a document yields to inference but not to parsing, that inference is the extraction, and no script is owed.

When a script does help, keep it small and reusable, next to the crawler, and name it for what it parses — `datasets/se/soe/extract_edition_2024.py` is one module per annual report edition. This pays off most when the extraction recurs against the same document shape: each new edition of a report, or version of a regulation. It is that recurrence that justifies a script, not the row count.

## Migrating from a Google Sheet

A source whose curated data currently lives outside the repository moves in as two commits, not one.

The first reproduces the current source exactly, with nothing cleaned, corrected or re-extracted, so there is a version in git that can be diffed against the sheet it came from. The first LLM pass is the second commit, reviewed on its own against that baseline.
