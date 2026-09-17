# Curating a dataset in the repository

A curated dataset is built from a CSV in the dataset directory, such as `datasets/cn/sanctions/sanctions.csv`: the crawler reads it row by row and maps each row to FollowTheMoney entities, and the rows themselves change only through reviewed pull requests.

Curate when the source publishes designations only as prose — a ministry notice, an annex to a regulation, a PDF edition of a list — and there is no structured document to parse instead.

Because the file is tracked in git, every change to the data carries an author, a date and a diff, and a reviewer sees which rows a pull request touches before it is merged.

## File and column conventions

### Every cell is verbatim or a metadata column

The metadata columns are the ones extraction assigns: e.g. `type`, `topics`, the program key and the QID. Every other cell holds the source's own text, unmodified.

Cleaning belongs downstream, in the crawler and in [datapatch lookups](datapatch_lookups.md) or name-cleaning Data Reviews, where one option corrects every row carrying the value and the correction re-runs on the next crawl. A value tidied up in the file applies to that row alone, fixed to whichever model extracted it, and nothing can re-derive it when the cleaning rules change.

### Split a corrected column into `x_original` and `x_clean`

When the printed value is not usable, add a second column instead of overwriting the first. `x_original` holds what the source prints and `x_clean` the correction; the crawler reads `_clean` and falls back to `_original`.

Use the same two suffixes on every column that takes corrections, so a reviewer can tell which values came from the source without reading the crawler.

On the emit side the crawler passes the correction as the value and the source text as `original_value=`, so the statement records both: `entity.add("name", clean_name, original_value=raw_name)`, as in `datasets/us/dod_chinese_milcorps/crawler.py`.

### One row per designation, not per entity

The same person designated twice under different measures, or re-listed after a removal, gets a row for each designation. Entities are merged downstream, while the rows retain the program, the dates and the source of each one.

One row per entity has nowhere to record the date a single designation ended, so end dates and removals cannot be expressed at all.

### Every row cites its official source

The source URL is mandatory and never blank. A row without one cannot be checked against the document it was extracted from, and there is nothing to re-fetch when the extraction is questioned.

For event sources the row carries the notice identity as well: the issuing authority and its decree or announcement number. The URL locates the notice on the current site, while the decree number still identifies it after a site redesign.

### Name columns in snake_case, after the FtM property

Use the FollowTheMoney property name wherever one exists, and descriptive snake_case English for columns that are not properties. `datasets/se/soe/leadership.csv` heads its columns `name`, `alias`, `position`, `company`, `report` and `source_url`.

A column named after the property it populates states what the crawler does with the cell, and one spelling across datasets lets a reviewer read any curated file without first reading its crawler.

### Separate multiple values with a semicolon

Pack multiple values into one cell with `;` and split them in the crawler. The delimiter is the same in every curated dataset: a per-dataset delimiter means the splitting rule has to be read out of the crawler before a cell can be interpreted.

### Force-add the file: `*.csv` is gitignored

`.gitignore` ignores `*.csv`, so `git add` skips a new curated file and leaves it untracked:

```bash
git add -f datasets/xx/source/sanctions.csv
```

The crawler opens the file from the working directory, so an untracked file crawls correctly on the machine that wrote it and fails in CI.

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

### Write a parser program when the extraction recurs

The default is no program: read the document, write the rows.

Write one when the extraction recurs against the same document shape — each new edition of a report, each new version of a regulation. Recurrence is the trigger, not size: no row count makes a parser necessary, so a one-off transcription of a several-hundred-entry PDF gets none, while a twenty-row annex reissued every few months does.

Keep the program next to the crawler and name it for what it parses, so the next edition is a change to reviewable code rather than a fresh extraction. `datasets/se/soe/extract_edition_2024.py` is one module per annual report edition.

## Migrating from a Google Sheet

A source whose curated data lives outside the repository moves in as two commits, not one.

The first reproduces the current source exactly, with nothing cleaned, corrected or re-extracted, so there is a version in git that can be diffed against the sheet it came from. The first LLM pass is the second commit, reviewed on its own against that baseline.
