# Plan: find and fix inconsistent data reviews in the review UI

Status: proposal, not started. Written 2026-09-03 after evaluating the ch_seco_sanctions
other-information prompt against reviewer-accepted extractions (see
`data/prompt_eval/ch_seco_sanctions/ANALYSIS.md`, not committed).

## Problem

The accepted reviews are our gold standard for LLM extraction, but reviewers answered several
unstated policy questions differently from one another and from themselves (e.g. whether
"X, listed as Y (QDe.115)" is one related entity or two; whether a ministry listed under
"Associated entities" is extracted; whether a Russian "Registration number" is an ogrnCode).
Finding these today means exporting the review table and grepping. The review list page at
`ui/app/review/dataset/[dataset]/page.tsx` has free-text search but shows nothing of the
extracted data beyond a 100-character snippet of `original_extraction`, and cannot narrow to
reviews a human actually changed.

## Goal

Make the list page sufficient for a curation pass: search a specific field, restrict to reviews
where the reviewer changed the extraction, and see what changed, without leaving the UI. Fixes
are then made on the existing per-review edit page, so they flow to production output and to
the fixture export (`datasets/ch/seco_sanctions/build_fixtures.py`) alike.

## Changes

All dataset-agnostic. The UI is Postgres-only (`getDb` in `ui/lib/db.ts` throws on anything
else), so Postgres JSON operators are fine.

### 1. Query: `getExtractionEntries` in `ui/lib/db.ts`

New optional parameters, all read from URL search params:

- `scope`: `all` (default) | `source` | `original` | `extracted` | `url` | `reviewer`.
  Keeps only the matching `ILIKE` clause(s) of the existing per-token OR. `all` keeps the
  current behaviour.
- `differs`: boolean. Adds
  `CAST(review.original_extraction AS jsonb) <> CAST(review.extracted_data AS jsonb)`.
  The columns are `json`, which has no equality operator; the jsonb cast is required and
  also ignores key order and whitespace.
- Select `extracted_data` and `source_value` in addition to `original_extraction` so the
  page can render them. Keep the row shape backwards compatible (`raw_data_snippet` stays).

Also return per row a `diff` computed server-side (see 2), so the page does not ship two full
JSON blobs per row just to show what changed.

### 2. Diff helper: `ui/lib/reviewDiff.ts` (new)

`diffExtraction(original: unknown, extracted: unknown): DiffEntry[]` walking both values:

- Objects: recurse per key, path joined with `.`.
- Arrays: compare as multisets of canonical JSON strings, report `added`/`removed` items
  (order changes are not edits).
- Scalars: report `changed` with before/after.
- `null` and `[]` are treated as equal (see
  ~/.claude memory `reviews-null-vs-empty-list`: both mean "no value in source").

Output is a flat list of `{ path, kind, before?, after? }`. Rendered as one short line per
entry, e.g. `related_entities −{"UnknownLink": ["Hottak tribe"]}` or
`simple_values −registrationNumber=1077757722206 +ogrnCode=1077757722206`.

Unit test in `ui/lib/reviewDiff.test.ts` (jest, same setup as `db.test.ts`): reordering only
gives no entries; null vs [] gives no entries; added/removed array items; nested scalar change.

### 3. Controls: `ui/app/review/dataset/[dataset]/SearchInput.tsx`

Alongside the text box:

- `<select>` for scope.
- Checkbox "only reviews the reviewer changed" (differs).
- Checkboxes "show source", "show original", "show extracted" (columns, default off).

All state lives in URL params (`scope`, `differs`, `cols`) so a filtered view is a shareable
link and survives navigating into a review and back.

### 4. Table: `ui/app/review/dataset/[dataset]/page.tsx`

- Always-on new column "Changes" rendering the diff entries (empty when unchanged).
- Optional columns for source value, original extraction, extracted data, rendered in a
  `<pre>` with `max-height` and scroll; source value truncated with a title tooltip.
- Show a row count above the table.
- Replace the raw `any` row type with an exported interface from `db.ts`.

### 5. Not in scope

- Paging. With `differs` on the list is a few hundred rows for the largest dataset; without
  it the current page already renders everything. Revisit if a dataset grows past ~2000.
- Inconsistencies inside the *unedited* set (original equals extracted but the accepted value
  is wrong or disagrees with a sibling review). Those are found with scope=extracted searches
  or with the prompt evaluation harness, not with the differs filter.
- Bulk edit. Fixes stay on the per-review page.

## How it gets used

1. Write down the policy answers first (crawler.py "Data review tips" header for the dataset,
   prompt text), otherwise a re-review reproduces the inconsistency.
2. For each policy question, search with scope=source and differs=on for its trigger phrase
   ("listed as", "Associated entit", "tribe", "Ministry", "Rank:", "Registration number"),
   read the Changes column and the reviewer column, open the outliers, fix, accept.
3. Re-export fixtures with `build_fixtures.py`, commit, re-run
   `datasets/ch/seco_sanctions/evaluate_prompt.py` to see the gold standard move.

## Follow-ups on the evaluation side (separate small PR)

- `evaluate_prompt.py`: score the `relationship` string, not just schema and names; fold
  curly/straight quotes and parse dates before comparing.
- `build_fixtures.py`: export `reviewer` per example.

## Estimate

One session: query params and SQL (~40 lines), diff helper plus tests (~120 lines), controls
and columns (~80 lines).
