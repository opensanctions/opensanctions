# Dataset metadata

Excellent dataset metadata is a relatively low-effort way to demonstrate the transparency which underpins OpenSanctions. Write it considering the perspective of data users ranging from startup software developers and business analysts, to investigative journalists and researchers.

Remember to give the context that people from different countries need to make sense of systems they are not entirely familiar with. Share what you learned when figuring out what a source dataset represents.

Metadata fields are user-facing prose. Knowledge addressed to future maintainers of the crawler — failure modes, source quirks, the purpose of a lookup — belongs in [YAML comments](#maintainer-notes-yaml-comments) instead.

Use the `.yml` extension.

## Title

As close as possible to an official title for what this dataset contains, starting with the name readers use to find it in a sorted list:

- Start with the short English name of the issuing country, never possessive: `Canada Members of Parliament`, not "Members of the Canadian Parliament" or "Canada's Members of Parliament".
- Datasets issued by international bodies start with the issuer's name or established acronym instead: `EU Financial Sanctions Files (FSF)`, `UN Security Council Consolidated Sanctions`, `INTERPOL Red Notices`.
- Keep the agency acronym directly after the country when it identifies the list (`US OFAC Specially Designated Nationals (SDN) List`, `US SEC Litigation Releases`), and keep established list acronyms or original-language names in parentheses (`Mongolia Members of the State Great Khural`, `Slovakia Public Sector Partners Register (Register partnerov verejného sektora)`).
- If the dataset is a subset of its source data, try to capture that: e.g. `Plural Legislators` if the Plural portal includes committees but the dataset only captures the legislators.

## Summary

A short line (50–90 characters — aim just above the lower bound) shown with the title in search results and listings.

- One plain sentence or fragment, no trailing period.
- Complement the title: add what it doesn't already convey (kind of measure, scope, legal basis) rather than repeating title words.
- No humor, no editorializing.

## Description

One to three short paragraphs describing the scope of the dataset. Write for a compliance or domain-expert reader, not an engineer.

- Open with what the dataset covers, and the institutional or legal context a reader from another country needs: what kind of body issues it, under what mandate, what inclusion on the list means.
- Describe scope boundaries: what is included and excluded, current-only vs. historical coverage, any period the dataset is limited to.
- Note significant limitations that affect how a reader should trust or interpret the data — for example a dataset maintained manually because the source is a PDF or behind an access block, or coverage that updates irregularly.

Keep out of the description:

- Per-record field listings ("records each person with their name, gender and date of birth") — that is visible from the data and its statistics, and drifts as the crawler changes.
- Routine ETL mechanics: fetching, parsing, pagination, lookups.
- Sourcing narration ("the data is sourced from the official website") — that is what `publisher` and `data.url` convey.

## Maintainer notes (YAML comments)

Document technical knowledge about the crawler as `#` comments in the `.yml`, adjacent to the thing they explain:

- A top-of-file comment block for crawler-level notes: known failure modes and how to handle them, source publication quirks, runbooks for recurring warnings.
- A short comment above each lookup that is not a plain type lookup (`type.country`, `type.date`, …), explaining its structure and purpose: what raw values it matches, and what the crawler does with the result.

Comments record known facts — observed failures, documented source behaviour, decisions that were made — never speculation. Good examples: `datasets/cn/sanctions/cn_sanctions.yml` (a runbook for handling designation-notice warnings), `datasets/fr/tresor/fr_tresor_gels_avoir.yml` (one-line annotations on the groups within a large lookup).

## Properties

### Basics

- `entry_point` e.g. `crawler.py:crawl_peps` - the file name, optionally followed by a method name called by the zavod `crawl` command. Defaults to the `crawler.py:crawl` calling an entry point in the dataset directory.
- `prefix` - The prefix used by entity id helpers, e.g. `gb-coh` or `ofac` - try to make this short but unique across datasets, unless you would like different datasets to intentionally generate overlapping keys. See the [entity ID guide](best_practices/entity_id.md) for the shape and stability rules that apply to IDs.
- `url` - the home page or most authoritative place where someone can read about this particular dataset at its source. E.g If a source publishes 5 different datasets, try to link to the page describing the data actually contained in this dataset.
- `disabled` - boolean, default `false`. Set to `true` for sources that are not available any more or should not be crawled at the moment: no crawl job is deployed and the coverage frequency is forced to `never`, but the metadata still gets published.
- `hidden` - boolean, default `false`. Set to `true` to keep the dataset out of the website and other user interfaces while still running and publishing it.

### Data Coverage

- `coverage`
    - `frequency` - e.g. `daily`, `weekly`, `monthly`, `never`. This represents how often it is expected that this dataset will be updated. It conveys to users how often to expect updates, and is used to derive a crawling schedule unless one is defined explicitly. House defaults by dataset type:
        - sanctions and wanted lists: `daily`
        - PEP sources (legislatures, governments): `monthly`
        - company registries and other bulk sources: `weekly`
        - frozen one-off dumps: `never`, with an explicit `schedule` (usually `@monthly`) so exports stay consistent with FollowTheMoney updates.
    - `start` - The date the dataset was first included in the `default` collection — i.e. the date the crawler was added to OpenSanctions. Use today's date when scaffolding a new crawler. A string in the format `YYYY-MM-DD`. Do **not** set this to the date the source data begins covering (e.g. an election date or the start of a parliamentary term).
    - `end` - The end date of a dataset which covers only a specific period in time, e.g. for a dataset specific to a data dump or parliamentary term. A string in the format `YYYY-MM-DD`. Future dates imply an expected end to the maintenance and coverage period of the dataset. Past end dates result in the datasets last_change date being fixed to that date, while its last_exported date remains unchanged.
    - `schedule` - a cron style schedule defining what time and frequency a crawler should run, e.g `30 */6 * * *`. The deployed schedule is resolved as `coverage.schedule`, then `deploy.schedule`, then the mapping of `frequency` (falling back to daily); the minute is randomized per dataset to spread load, so only pin an exact time when it matters (e.g. right after the source's own publication time).
- `manual_check` - for datasets that need periodic human re-verification, e.g. manually maintained data or a source that changes without machine-readable signals. A maintenance script reports datasets whose check is due and bumps `last_checked`.
    - `last_checked` - quoted string `"YYYY-MM-DD"` (the exact format is required for the automatic update).
    - `interval` - number of days between checks.
    - `message` - what the reviewer should verify.

### Publisher

- `publisher`
    - `name` - The publisher's official name. If this is by default in a primary non-english language from the originating country, use that language here, and the english form in `publisher.name_en`.
    - `name_en` - Their name in English, ideally the official form, otherwise a translation.
    - `acronym` - Add if there's an official acronym, e.g. check in their domain name, footer, about page.
    - `description` - This can be one to two paragraphs of text. Use the publisher description field to explain to someone from a country other than the publisher who the publisher is, and why they do what they do.
    - `url` - The home page of their official website
    - `country` - The Alpha-2 or two-letter ISO 3166-1
    - `official` - `true` if the publisher is an authority overseeing the subject data, generally a government entity releasing their sanctions list or legislator data, otherwise `false`.

### Source data

- `data`
    - `url`- The link to a bulk download or API base URL or endpoint - ideally something you can use within the crawler via `context.data_url` to request the data, and which ideally returns a useful response when followed by dataset users. It's not the end of the world if you make other requests to expand the data available to the crawler.
    - `format` a string defining the format of the data at that URL, e.g. `JSON`, `HTML`, `XML`. A Zip file containing thousands of YAML files might be more usefully annoted with `YAML` than `ZIP` because it conveys the structural syntax of the data.
    - `lang` - ISO 639-3 code (e.g. `deu`, `slk`) of the source's primary language. It is applied as the default language of every statement the crawler emits, so set it when the source is predominantly in one non-English language.

### Tags

`tags` are a controlled vocabulary used to categorize datasets by shared attributes such as legal basis, list type, target country, or sector. They support cross-referencing within specific scopes, such as distinguishing between sanctions, PEPs, and regulatory actions, and enable users to select the most relevant datasets for a given country, sector, or risk category.

Currently, tags cover the following dimensions:
- list type (e.g. `list.sanction`, `list.pep`);
- issuer and jurisdiction (e.g. `issuer.west`, `juris.eu`);
- target countries (e.g. `target.ru`, `target.us`)
- sectors (e.g. `sector.financial`, `sector.maritime`)
- risk themes (e.g. `risk.klepto`).

Tag matching is by exact string, not by prefix: a dataset tagged only `list.pep.bulk` does not match `list.pep`. Sub-tags qualify a base tag and should be applied alongside it — `list.pep.bulk` marks PEP datasets (also tagged `list.pep`) that are excluded from broad PEP cross-referencing, such as declaration registries and sub-national officeholder lists.

You can find a full overview of available tags [here](https://www.opensanctions.org/docs/metadata/).

### Deployment

The `deploy` section configures the Kubernetes job that runs the crawler in production. It is consumed by the deployment tooling, not by zavod itself. Override the defaults only when a crawler demonstrably needs it:

- `deploy`
    - `memory` / `memory_limit` - memory request and limit (defaults `700Mi` / `1600Mi`), e.g. `2000Mi` for crawlers that hold large source files in memory.
    - `cpu` / `cpu_limit` - CPU request and limit (defaults `200m` / `1600m`).
    - `disk` / `disk_limit` - scratch disk (default `9Gi`) for crawlers that download large source archives.
    - `premium` - `boolean` - whether its compute instance may be evicted, restarting the job. Set to `true` for jobs running for several hours.
    - `schedule` - cron schedule; only consulted when `coverage.schedule` is not set.

### Continuous Integration

- `ci_test` - boolean, default `true`. If true, the crawler is run when its python or yaml is modified in CI. Set to false for extremely slow crawlers, or those that require credentials, and then take extra care when modifying them.

### Exports

- `exports` - An array of strings matching the [export formats](https://www.opensanctions.org/docs/bulk/), e.g. `"targets.nested.json"`. The default is best for most cases.
- `load_statements` - Whether the statements should be loaded to a SQL table after the run. Usually `false` for collections and enrichment targets like company registries, and true for normal datasets and enrichers.
- `resolve` - boolean, default `true`. Whether entities are resolved to canonical (deduplicated) IDs. Set to `false` only for special-purpose datasets that must retain their local IDs.
- `full_dataset` - the name of the complete dataset a subset is derived from; required for datasets used as local enrichment targets, so that matches can be expanded from the full data.

### Date formatting

- `dates` - date formatting used by [helpers.apply_date and apply_dates](helpers.md#zavod.helpers.apply_date) but also accessible via the context for use in `helpers.parse_date`. See the [date parsing guide](best_practices/dates_meta.md) for usage patterns and worked examples.
  - `formats`: Array of date format strings for parsing dates into partial ISO dates
  - `months`: Map where values like `März` are translated into keys like `"3"` so that it could then be parsed by a format string like `%m`

### HTTP options

HTTP requests for GET requests are automatically retried for connection and HTTP errors. Some of this retry behaviour can be configured from the dataset metadata if needed.

- `http`
    - `user_agent`: string, defaults to the value of the FTM_USER_AGENT setting. Set a custom value for the `User-Agent` header if needed.
    - `timeout`: integer in seconds, default `60`. Connect and read timeout for the context HTTP session. Increase it for sources that are slow to respond.
    - `zyte_timeout`: integer in seconds, default `300`. Connect and read timeout for Zyte API requests, which is separate because the timeout covers a whole proxied fetch — exit node selection, browser rendering and Zyte's own ban retries — rather than a single request to the source. Lower it only to fail faster on a source known to be unreachable; timing out below Zyte's own budget discards work it has already done.
    - `backoff_factor`: float, default `1`. [Scales the exponential backoff](https://urllib3.readthedocs.io/en/stable/reference/urllib3.util.html#urllib3.util.Retry.DEFAULT_ALLOWED_METHODS:~:text=with%20None.-,backoff_factor,-(float)%20%E2%80%93).
    - `max_retries`: integer in seconds, default `3`
    - `retry_methods`: List of strings, [default](https://urllib3.readthedocs.io/en/stable/reference/urllib3.util.html#urllib3.util.Retry.DEFAULT_ALLOWED_METHODS) `['DELETE', 'GET', 'HEAD', 'OPTIONS', 'PUT', 'TRACE']`
    - `retry_statuses`: List of integers of HTTP error codes to retry, default `[413, 429, 500, 502, 503, 504]`.

### Data assertions

Data assertions are intended to "smoke test" the data. Assertions are checked on export. If assertions aren't met, warnings are emitted.

Data assertions are checked when running `zavod run` (and `zavod validate` is useful when developing a crawler).

Data assertions are useful to communicate our expectations about what's in a dataset. `min` validations set a baseline for what should be in the dataset and are fatal to the export if they fail. `max` validations emit a warning when the dataset has grown beyond the validity of our earlier baseline (or if something's gone horribly wrong and emitted way more than expected)

It's a good idea to add assertions at the start of writing a crawler, and then see whether those expectations are met when the crawler is complete. A good rule of thumb for datasets that change over time is minima 10% below the expected number to allow normal variation, unless there's a known hard minimum, and a maximum around twice the expected number of entities to leave room to grow.

A basic assertion block can look like this:

```yaml
assertions:
  min:
    schema_entities:
      Person: 160  # at least 160 Person entities
      Position: 30  # at least 30 Position entities
    entities_with_prop:
      Company:
        taxNumber: 10  # at least 10 Companies have a tax number set
  max:
    schema_entities:
      Person: 400  # at most 400 Person entities
      Position: 80  # at most 80 Position entities
```


#### Assertion types

**`schema_entities`** asserts on the number of entities of a given schema.

**`country_entities`** asserts on the number of entities associated with a country in any of its properties. All properties with type `country` are considered (among them the usual suspects such as `country`, `jurisdiction` and `citizenship`). Countries are given as ISO 3166-1 Alpha-2 country codes.

**`countries`** asserts on the number of distinct countries expected to appear in the dataset.

**`entities_with_prop`** asserts on the number of entities of a given schema that have a given property set.

**`property_fill_rate`** asserts on the proportion of entities of a given schema that have a given property set, expressed as a float between 0 and 1.

```yaml
assertions:
  min:
    property_fill_rate:
      Person:
        birthDate: 0.7  # at least 70% of Persons have a birth date
```
