---
name: release-datasets
description: Release one or more datasets by adding them to a topical collection, bumping coverage.start, and verifying. Use when the user wants to release/publish specific crawled datasets into the OpenSanctions product.
---

# Release Datasets

Release the datasets the user names by adding each to a topical collection's
`children:` list and bumping its `coverage.start`. The datasets to release: $ARGUMENTS

**Only release the datasets the user explicitly lists.** Never release every
unreleased dataset in the pipeline — most unreleased datasets are work in
progress or test scratch and are unreleased on purpose. If the user gives no
list, ask which datasets they want to release before doing anything.

## How a release works

A crawler dataset (`datasets/<cc>/<name>/<name>.yml`, has an `entry_point`)
crawls data but is **unreleased** until it is named in some collection's
`children:` list. Releasing = editing the collection file, not the dataset file:

- Collections live in `datasets/_collections/*.yml` (`type: collection`) and own
  a `children:` list of dataset names. Membership is declared **only** there —
  there is no reverse `collections:` key on the dataset itself.
- The `default` collection lists the *topical collections* as its children, so
  membership rolls up recursively: add a dataset to `peps` and it is
  automatically in `default` too. **Do not edit `default.yml`** for an ordinary
  dataset — only topical collections.
- `contrib/check_hierarchy.py datasets` is the verifier: it warns
  `"<name> has no collections"` for any non-collection, non-disabled dataset that
  is not a child of any collection. A clean run for a dataset = released.

## Steps

For each dataset the user named:

1. **Determine the target collection.**
   - If the user specified one, use it.
   - If not, suggest one from the dataset's topic — inspect its `.yml` `tags:`
     (e.g. `list.pep`), `summary`/`description`, and the kind of entities it
     emits. Use the table below. **Confirm the suggestion with the user** before
     editing; don't guess silently.
   - A dataset may join more than one collection if it genuinely fits.

2. **Add the dataset name to `datasets/_collections/<collection>.yml`** under
   `children:`, keeping the list **alphabetically sorted**. Insert at the correct
   sorted position rather than appending.

3. **Bump `coverage.start`** in the dataset's own `.yml` to today's release date,
   **preserving the file's existing quoting style** (some files quote the date,
   some don't).

4. **Verify** with `python contrib/check_hierarchy.py datasets` — confirm the
   dataset no longer warns `"has no collections"`. Remaining warnings for
   datasets you were *not* asked to release are expected; leave them.

5. **Validate the edited collection** (sorted + no dupes), e.g.:
   ```
   python -c "import yaml; c=yaml.safe_load(open('datasets/_collections/<collection>.yml'))['children']; assert c==sorted(c), 'not sorted'; assert len(c)==len(set(c)), 'dupes'; print('ok', len(c))"
   ```

## Collection suggestion guide

These are the topical collections that roll up into `default` (the public
product). Suggest from these:

| Collection       | Holds                                                        |
|------------------|--------------------------------------------------------------|
| `peps`           | Parliaments, elected officials, PEP/asset declarations       |
| `sanctions`      | Sanctions lists (consolidated)                               |
| `eu_sanctions`   | EU-specific sanctions sources                                |
| `crime`          | Criminal / investigative / leaked-records entities           |
| `wanted`         | Wanted-persons lists                                         |
| `debarment`      | Debarred companies and individuals                           |
| `regulatory`     | Regulatory watchlists, investor alerts                       |
| `enforcement`    | Enforcement actions, litigation, regulatory notices          |
| `maritime`       | Vessels, port-state-control / maritime sources               |
| `securities`     | Sanctioned/flagged securities issuers                        |
| `special_interest` | Special-interest watchlists                                |
| `enrichers`      | External enrichment sources                                  |

If a dataset's topic is ambiguous, ask the user rather than picking.

## Don't release

- Test/scratch datasets (e.g. `alert_testing`).
- `_externals/` and `_analysis/` specials — these are wired differently; flag
  them to the user instead of adding them to a topical collection.

## Commit

Group the collection edit and the coverage bumps together. Use the house
`[dataset_name]` commit-title prefix (one dataset → `[xx_foo] Release into peps`;
several → describe the batch). Do not push during an in-flight session without
explicit confirmation.
