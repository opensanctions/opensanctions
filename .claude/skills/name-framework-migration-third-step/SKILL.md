---
name: name-framework-migration-third-step
description: Complete the name framework migration in a crawler (Step 3) by removing all custom name cleaning/splitting logic and the Step 1 review scaffolding, replacing it with a single h.apply_reviewed_name_string or h.apply_reviewed_names call. Use only after Step 1 has been deployed and run in production and the dataset's name reviews have been completed (Step 2).
argument-hint: "[crawler.py path]"
disable-model-invocation: true
---

Perform Step 3 of the name framework migration in $ARGUMENTS.

`zavod/docs/extract/names.md#migrating-to-the-name-cleaning-helpers` (the "Step 3" subsections) is the authoritative procedure and holds the exact code — follow it, do not rely on a paraphrase here. This skill only orients you and covers the mechanics of running it.

## Preconditions

Do not run Step 3 until Step 1 has been deployed and run in production (the crawler already calls `h.review_names`) and Step 2 is done (the dataset's name reviews are completed). Step 3 hands cleaning to the review system, and unaccepted reviews fall back to the raw string — so incomplete reviews change the output. If you cannot confirm both, stop and tell the user.

## Branch setup

Derive a branch name from the crawler path: take the dataset name (the directory containing `crawler.py`) and prefix it with `name-migration-step3/` (e.g. `datasets/us/ga/med_exclusions/crawler.py` → `name-migration-step3/us-ga-med-exclusions`). Then `git checkout -b <branch-name>` and confirm you are on it before proceeding.

## Crawler source

!`cat $ARGUMENTS`

## Read first

- `zavod/docs/extract/names.md#migrating-to-the-name-cleaning-helpers` — the "Step 3" subsections are authoritative for the exact code
- `zavod/zavod/helpers/names.py` — signatures for `apply_reviewed_name_string`, `apply_reviewed_names`, `Names`

## What Step 3 changes

- Remove **all** custom splitting/cleaning and the Step 1 scaffolding it was added alongside: `original`, `suggested`, `h.check_names_regularity`, `h.review_names`, and the manual `entity.add("name"/"alias", ...)` calls that drove output.
- Keep the raw source string capture (the `.pop(...)`) and the entity id.
- Follow the doc for the replacement call: sanctions/debarment → `apply_reviewed_name_string` with no `llm_cleaning`; non-sanctions → the same with `llm_cleaning=True`; multiple source name fields → `apply_reviewed_names` with an `h.Names(...)`.
- `string=` (or the `Names` values) must be the unmodified raw source string; preserve any `lang=` the removed `entity.add` used; call the helper at most once per entity.

After the edit, tell the user to check, once the Step 3 deployment has run, that new names were not auto-accepted between deploying Step 1 and Step 3.

## After changes

After every edit, run `uvx ruff check --fix $ARGUMENTS && uvx ruff format $ARGUMENTS` and fix anything it reports.

Once ruff passes, `git add $ARGUMENTS` and output this suggested commit message (do not commit):

```
[<dataset_slug>] name migration step 3
```

where `<dataset_slug>` strips `datasets/` and `/crawler.py` and replaces `/` with `_` (e.g. `datasets/us/ga/med_exclusions/crawler.py` → `[us_ga_med_exclusions] name migration step 3`).

## Do not

- Do not run Step 3 before its preconditions hold (Step 1 deployed and run, Step 2 reviews completed)
- Do not enable `llm_cleaning` for sanctions datasets
- Do not pass a cleaned/split/modified string to `string=`
- Do not leave any custom cleaning or Step 1 scaffolding behind
- Do not guess `Names` field names or helper signatures — read `zavod/zavod/helpers/names.py`
