---
name: deprecate-dataset
description: Deprecate a dataset whose source is gone or replaced — announce it in the dataset metadata, remove it from collections on the announced date, or delete its files. Use when asked to deprecate, retire, remove or delete a dataset, or to prepare a dataset-removal PR.
---

# Deprecate a dataset

Take one stage of a dataset deprecation for the dataset the user names: $ARGUMENTS

See [Deprecating a dataset](../../../zavod/docs/best_practices/deprecation.md).
**Read that doc and follow it.** This skill only covers how to drive it.

## 1. Work out the stage

Read the dataset's `.yml` and search for its name in `datasets/_collections`,
`datasets/_externals`, `datasets/_analysis` and `meta`. Then pick the stage:

| Repo state | Stage |
|---|---|
| No `deprecation:` block | Announce |
| `deprecation:` block, still listed in a collection, enricher input or program, or not `hidden` | Remove from collections |
| `deprecation:` block, `hidden: true`, no references left outside its own directory | Delete |

Tell the user which stage you picked and why, and confirm before editing. If the
user named a stage that doesn't match the repo state — for example, removal from
collections while no notice has been published — point out the gap and ask.

## 2. Gather what the stage needs

Ask the user for anything you can't find yourself. The dates and the reason go into a
public notice and changelog entry, so ask rather than guess.

- **Announce:** why the source is deprecated, the GitHub issue that tracks it, any
  replacement dataset, the planned removal date, and whether the source still works.
  Use `python -m contrib.maintenance.diagnose <dataset_name>` to find the date of the
  last successful run for `coverage.end`.
- **Remove from collections:** the changelog entry that announced the removal, and
  its removal date.

## 3. Edit

Work in a new worktree off `origin/main`, on a branch named
`deprecate-<dataset-name>` (announce), `remove-<dataset-name>` (remove from
collections) or `delete-<dataset-name>` (delete). Do one dataset per branch, so
each removal can be merged or reverted on its own date.

Make only the edits the doc lists for the stage.

## 4. Verify

- Load the dataset, unless the stage is Delete:
  `python -c "from zavod.meta import load_dataset_from_path; from pathlib import Path; d = load_dataset_from_path(Path('<path to yml>')); print(d.name, d.model.deprecated, d.model.coverage)"`
- Lint each changed YAML file:
  `contrib/lint_dataset.sh <path>`
- Run `python contrib/check_hierarchy.py datasets` after removing from collections.
- Grep the repo for the dataset's name and report every reference that is left.

## 5. Hand over

Don't commit or push unless the user asks. When they do:

- Commit title: `[<dataset_name>] Announce deprecation`,
  `[<dataset_name>] Remove from collections` or `[<dataset_name>] Delete dataset`.
- For removal from collections and deletion, the PR is prepared ahead of the
  announced date and merged on it. Add the `dataset-removal` label.
- Link the changelog entry in the PR description.
