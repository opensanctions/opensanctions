# Releasing a dataset

A crawler that has been merged still only runs in isolation — it produces data,
but that data is not yet part of any published OpenSanctions product. A dataset
is **released** once it is added to a collection. This usually happens after the
crawler has passed [review](merge_checklist.md), an initial run, and de-duplication
against existing data.

## How collections work

Collections are datasets that combine the entities of other datasets. They are
defined as YAML files in
[`datasets/_collections/`](https://github.com/opensanctions/opensanctions/tree/main/datasets/_collections),
each with a `children:` list naming its member datasets:

```yaml
type: collection
title: Politically Exposed Persons Datasets
children:
  - ad_consell_general
  - al_kuvendi
  - am_hetq_officials
  # ...
```

Membership is declared **only** in the collection's `children:` list. There is no
reverse key on the dataset itself — to release a dataset you edit the collection,
not the dataset's own metadata.

Collections nest: the `default` collection lists the *topical* collections
(`sanctions`, `peps`, `crime`, …) as its children, and those in turn list the
individual datasets. Membership rolls up recursively, so adding a dataset to a
topical collection automatically includes it in `default` (the broadest public
distribution) as well. **You normally only edit a topical collection** — never
add an ordinary dataset directly to `default`.

## Choosing a collection

Add the dataset to the most appropriate topical collection based on the
[kind of entities](https://www.opensanctions.org/docs/topics/) it produces. The
quickest way to decide is to find a
[similar dataset](https://www.opensanctions.org/datasets/) and see which
collection it is included in.

| Collection         | Holds                                                       |
|--------------------|-------------------------------------------------------------|
| `peps`             | Parliaments, elected officials, PEP/asset declarations      |
| `sanctions`        | Consolidated sanctions lists                                |
| `eu_sanctions`     | EU-specific sanctions sources                               |
| `crime`            | Criminal / investigative / leaked-records entities          |
| `wanted`           | Wanted-persons lists                                        |
| `debarment`        | Debarred companies and individuals                          |
| `regulatory`       | Regulatory watchlists, investor alerts                      |
| `enforcement`      | Enforcement actions, litigation, regulatory notices         |
| `maritime`         | Vessels, port-state-control and other maritime sources      |
| `securities`       | Sanctioned/flagged securities issuers                       |
| `special_interest` | Special-interest watchlists                                 |

A dataset may belong to more than one collection if it genuinely fits. If the
topic is ambiguous, ask the team rather than guessing.

## Steps

1. **Add the dataset name** to the chosen collection's `children:` list in
   `datasets/_collections/<collection>.yml`. Keep the list alphabetically sorted.

2. **Bump `coverage.start`** in the dataset's own YAML to the release date (today).
   This field records the date the dataset first entered the `default` collection
   — i.e. the release date, which is usually later than when the crawler was
   scaffolded. Do **not** set it to the date the source data begins covering.

3. **Verify** that the dataset is now part of a collection:

   ```bash
   python contrib/check_hierarchy.py datasets
   ```

   This warns `"<name> has no collections"` for any non-collection dataset that
   is not a child of any collection. After releasing, the dataset you added
   should no longer appear in the output. Warnings for other unreleased datasets
   are expected — release only the datasets you intend to.

## What not to release

- Test or scratch datasets.
- `_externals/` and `_analysis/` datasets, which are wired into the pipeline
  differently — leave these to the maintainers.
