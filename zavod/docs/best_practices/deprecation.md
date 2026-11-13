# Deprecating a dataset

A dataset is deprecated when its source stops being available and nothing can take its place. Deprecation is the reverse of [releasing a dataset](release.md), and it happens in three stages. Removing a dataset is disruptive for users who screen against it, so it's announced before it takes effect.

## When to deprecate

Start a deprecation when a source has been unavailable for some time and there is no replacement that could be substituted into the same dataset. Before deprecating, try to reach the publisher, and look for an alternative source.

If the publisher has clearly retired the data, skip the outreach and announce the deprecation straight away.

Crawl a replacement source that publishes the same list into the existing dataset. If the replacement needs a new dataset, deprecate the old one once the new one is released, and name the replacement in the deprecation notice.

## Stages

| Stage | What changes |
|---|---|
| Announce | The dataset's metadata, plus a changelog entry |
| Remove from collections | Collections, enricher inputs and programs |
| Delete | The dataset's directory |

Publish the removal date in the changelog entry when announcing, and keep to it.

## Announce

Edit the dataset's YAML:

1. **Set `coverage.end`** to the date the dataset stopped being updated. For a source that is gone, that is the date of the last successful crawl. For a source that still works but is being replaced, it is the planned removal date.
2. **Add a `deprecation` notice.** See [Writing the notice](#writing-the-notice).
3. **Set `coverage.frequency` to `never`, and keep an explicit `coverage.schedule` or `deploy.schedule`.** A dataset with `frequency: never` and no schedule cannot be deployed. If the source is gone, use `coverage.schedule: "@weekly"`.

The dataset's page on the website shows the metadata of its newest run, so the notice only appears once a run has published it. A run that fails because the source is gone still publishes the metadata.

Publish a [changelog entry](https://www.opensanctions.org/changelog/) at the same time. It names the dataset, the collections it leaves, the removal date, the reason and any replacement.

### Writing the notice

The notice is shown on the dataset's page on the website. Write it for a user who screens against the dataset:

- Say why the dataset is deprecated, as a known fact: the source returns a 404, the publisher replaced the list, the website no longer lists the members.
- Link the GitHub issue that tracks the source problem.
- Name the replacement dataset, if there is one.
- End with the planned removal date in `YYYY-MM-DD` format, matching the changelog.

```yaml
deprecation: |
  The source for the Palestine Monetary Authority Local Freezing List dataset is gone,
  [returning a 404, since at least May 2026](https://github.com/opensanctions/opensanctions/issues/4143).
  Therefore, we are marking this dataset as deprecated and will remove it on 2026-11-16.
```

## Remove from collections

On the removal date, take the dataset out of everything that consumes it. The dataset's own YAML keeps its metadata, so its page stays up and explains where the data went.

1. **Remove the dataset from every collection** in `datasets/_collections/` that lists it in `children:`.
2. **Remove the dataset from the `inputs:` of any enricher or analysis dataset** in `datasets/_externals/` and `datasets/_analysis/`.
3. **Update the programs that point to it.** For each file in `meta/programs/` with `dataset:` set to the deprecated dataset, set `dataset:` to the replacement if it covers the same program. Otherwise delete the program file. See [Sanctions programs](../sanctions/programs.md).
4. **Set `hidden: true`** in the dataset's YAML. Search engines then drop the dataset's page before it is deleted, while anyone with a link can still open it.

Find every reference with:

```bash
grep -rn -w <dataset_name> datasets/_collections datasets/_externals datasets/_analysis meta
```

Then confirm the dataset is no longer part of any collection:

```bash
python contrib/check_hierarchy.py datasets
```

The removed dataset now shows up as `"<dataset_name> has no collections"`, which is expected.

## Delete

Delete the dataset's directory under `datasets/`, including the crawler code and the YAML.

Search the repository once more for the dataset's name, and remove any references that are left:

```bash
grep -rn -w <dataset_name> datasets meta zavod/docs
```
