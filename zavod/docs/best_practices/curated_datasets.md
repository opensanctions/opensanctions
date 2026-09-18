# Curating a dataset in the repository

A curated dataset is built from a CSV in the dataset directory, such as `datasets/cn/sanctions/sanctions.csv`: the crawler reads it row by row and maps each row to FollowTheMoney entities, and the rows themselves change only through reviewed pull requests.

Curate when the source publishes designations only as prose — a ministry notice, an annex to a regulation, a PDF edition of a list — and there is no structured document to parse instead.

Because the file is tracked in git, every change to the data carries an author, a date and a diff, and a reviewer sees which rows a pull request touches before it is merged.

## File and column conventions

### Every cell is verbatim or a metadata column

The metadata columns are the ones extraction assigns: e.g. `type`, `topics`, the program key and the QID. Every other cell holds the source's own text, unmodified.

Cleaning belongs downstream, in the crawler and in [datapatch lookups](datapatch_lookups.md) or name-cleaning Data Reviews, where one option corrects every row carrying the value and the correction re-runs on the next crawl. A value tidied up in the file applies to that row alone, fixed to whichever model extracted it, and nothing can re-derive it when the cleaning rules change.

### Split a corrected column into `x_original` and `x_clean`

When the printed value is not usable, add a second column instead of overwriting the first. `x_original` holds what the source prints and `x_clean` the value to use; both carry a value on every row, so where the printed value stands `x_clean` repeats it unchanged. The crawler reads `_clean` and falls back to `_original`.

Use the same two suffixes on every column that takes corrections, so a reviewer can tell which values came from the source without reading the crawler. Explain a correction in the file's shared `notes` column rather than adding a notes column per corrected field.

On the emit side the crawler adds the `_clean` value and passes `_original` as `original_value=` only where the two differ: `entity.add("name", clean, original_value=raw if raw != clean else None)`. Passing it unconditionally would record an original statement identical to the value on every uncorrected row, which is most of the file.

### One row per designation, not per entity

Normally the same person designated twice under different measures, or re-listed after a removal, gets a row for each designation. The crawler can then emit one `Sanction`, or one `Occupancy` for a term of office, straight from the row; entities are merged downstream, while the rows retain the program, the dates and the source of each one.

Keep to a row per designation even where the source publishes notices, wherever that is practical. A row per notice makes the crawler group events under a common key to reconstruct each designation, which produces duplicate entities wherever the key is imperfect, and is harder to review.

One row per entity has nowhere to record the date a single designation ended, so end dates and removals cannot be expressed at all.

### Every row cites its official source

The source URL is mandatory and never blank. A row without one cannot be checked against the document it was extracted from, and there is nothing to re-fetch when the extraction is questioned.

For event sources the row carries the notice identity as well: the issuing authority and its decree or announcement number. The URL locates the notice on the current site, while the decree number still identifies it after a site redesign.

### Name columns in snake_case, after the source's own terms

Use snake_case English, and name each column for what the source calls it: where the source labels a field `explanation`, the column is `explanation`, not `notes`. The crawler maps it to the FollowTheMoney property. The transcription then stays a faithful representation of the source.

### Separate multiple values with a semicolon

Pack multiple values into one cell with `;` and split them in the crawler. The delimiter is the same in every curated dataset: a per-dataset delimiter means the splitting rule has to be read out of the crawler before a cell can be interpreted.

## Updating the file as the source changes

### Discovery is a separate step from extraction

Extraction turns one document into rows. Discovery establishes which documents exist that no row and no review record accounts for.

A discovery step defines what gets polled, how it detects edits as well as additions, and how a candidate is marked out of scope. Without the last of those, the same false positive is raised on every crawl. `cn_sanctions` keeps that acknowledgement as `discovery.reviewed_urls` in its metadata, and `us_bis_mieu` as `discovery.reviewed_versions`.

Discovery produces a review queue and does not write to the CSV. Deciding who is designated, under which program and from when requires reading the official document, so the write is a pull request. [Change detection](change_detection.md) covers the sentinel helpers to build the polling step from.

### Every dataset needs a removal mechanism

Detecting that a designation has ended is as important as detecting a new one. Which mechanism applies depends on what the source publishes.

- **Consolidated sources** republish the complete current list, so a row vanishing from the source is the signal. Discovery compares the source's membership against the reviewed file; without that comparison the dataset records only additions and grows on every update.
- **Event sources** publish a stream of notices, so the removal is its own notice, extracted like any other event. Absence from a later notice is not a removal, and neither is a URL that stops resolving.

Set the end date rather than deleting the row. Deleting discards the record of which notice designated the entity and when, which both the next extraction and the next review read.

### Write an extraction script only where it helps

The default is no script: read the document, write the rows. Where a document yields to inference but not to parsing, that inference is the extraction, and no script is owed.

Where a script does help, keep it small and reusable, next to the crawler and named for what it parses. `datasets/se/soe/extract_edition_2024.py` is one module per annual report edition. This pays off most when the extraction recurs against the same document shape, each new edition of a report or version of a regulation, and recurrence is what justifies one rather than size: no row count makes a script necessary.

## Migrating from a Google Sheet

A source whose curated data lives outside the repository moves in as two commits, not one.

The first reproduces the current source exactly, with nothing cleaned, corrected or re-extracted, so there is a version in git that can be diffed against the sheet it came from. The first LLM pass is the second commit, reviewed on its own against that baseline.
