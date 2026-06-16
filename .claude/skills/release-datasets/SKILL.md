---
name: release-datasets
description: Release one or more datasets by adding them to a topical collection, bumping coverage.start, and verifying. Use when the user wants to release/publish specific crawled datasets into the OpenSanctions product.
---

# Release Datasets

Release the datasets the user names into the OpenSanctions product. The datasets
to release: $ARGUMENTS

The full release process — how collections work, choosing a collection, the
edit steps, and verification — is documented in
[`zavod/docs/best_practices/release.md`](../../../zavod/docs/best_practices/release.md).
**Read that doc and follow it.** This skill only covers how to drive it.

## Before following the doc

1. **Only release the datasets the user explicitly lists.** Never release every
   unreleased dataset in the pipeline — most are work in progress or test scratch
   and are unreleased on purpose. If the user gave no list, ask which datasets
   they want to release before doing anything.

2. **Determine the target collection for each dataset.**
   - If the user specified one, use it.
   - If not, suggest one from the dataset's topic — inspect its `.yml` `tags:`
     (e.g. `list.pep`), `summary`/`description`, and the entities it emits, then
     match against the collection table in `release.md`. **Confirm the suggestion
     with the user** before editing; don't guess silently.

## When editing

- Follow the steps in `release.md`: add the dataset to the collection's
  `children:` (alphabetically sorted), bump `coverage.start` to today, and verify
  with `python contrib/check_hierarchy.py datasets`.
- Preserve each dataset file's existing date quoting style when bumping
  `coverage.start` (some files quote the date, some don't).
- After editing a collection, sanity-check it stays sorted and dupe-free:
  ```
  python -c "import yaml; c=yaml.safe_load(open('datasets/_collections/<collection>.yml'))['children']; assert c==sorted(c) and len(c)==len(set(c)); print('ok', len(c))"
  ```

## Commit

Group the collection edit and the coverage bumps together. Use the house
`[dataset_name]` commit-title prefix (one dataset → `[xx_foo] Release into peps`;
several → describe the batch). Do not push during an in-flight session without
explicit confirmation.
