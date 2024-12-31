# Dataset metadata

Excellent dataset metadata is a relatively low-effort way to demonstrate the transparency which underpins OpenSanctions. Write it considering the perspective of data users ranging from startup software developers and business analysts, to investigative journalists and researchers.

Remember to give the context that people from different countries need to make sense of systems they are not entirely familiar with. Share what you learned when figuring out what a source dataset represents.

Use the `.yml` extension.

## Properties:

### Basics

- `title` - As close as possible to an official title for what this dataset contains. If it is a subset of its source data, try to capture that. e.g. `Plural Legislators` - if the Plural portal includes committees but the dataset only captures the legislators.
- `entry_point` e.g. `crawler.py:crawl_peps` - the file name, optionally followed by a method name called by the zavod `crawl` command. Defaults to the `crawler.py:crawl` calling an entry point in the dataset directory.
- `prefix` - The prefix used by entity id helpers, e.g. `gb-coh` or `ofac` - try to make this short but unique across datasets, unless you would like different datasets to intentionally generate overlapping keys.
- `summary` - Capture what a user of the dataset needs to know to decide if it's what they're looking for in a single clear concise line. This is used in search results.
- `description` - This can be one to three paragraphs of text. A more complete description of the dataset, perhaps with a bit more detail about what it includes, what it excludes, and how it is kept up to date if it is not from an official publisher.
- `url` - the home page or most authoritative place where someone can read about this particular dataset at its source. E.g If a source publishes 5 different datasets, try to link to the page describing the data actually contained in this dataset.

### Data Coverage

- `coverage`
    - `frequency` - e.g. `daily`, `weekly`, `monthly`. This represents how often it is expected that this dataset will be updated. It conveys to users how often to expect updates, and will also be used to generate a crawling schedule unless a specific schedule is defined.
    - `start` - The start date of a dataset which covers only a specific period in time, e.g. for a dataset specific to a data dump or parliamentary term. A string in the format `YYYY-MM-DD`.
    - `end` - The end date of a dataset which covers only a specific period in time, e.g. for a dataset specific to a data dump or parliamentary term. A string in the format `YYYY-MM-DD`. Future dates imply an expected end to the maintenance and coverage period of the dataset. Past end dates result in the datasets last_change date being fixed to that date, while its last_exported date remains unchanged.

### Deployment

- `deploy`
    - `schedule` - `string` - a cron style schedule defining what time and frequency a crawler should run, e.g `30 */6 * * *`
    - `premium` - `boolean` - whether its compute instance may be evicted, restarting the job. Set to `true` for jobs running for several hours.

### Continuous Integration

- `ci_test` - boolean, default `true`. If true, the crawler is run when its python or yaml is modified in CI. Set to false for extremely slow crawlers, or those that require credentials, and then take extra care when modifying them.

### Exports

- `exports` - An array of strings matching the [export formats](https://www.opensanctions.org/docs/bulk/), e.g. `"targets.nested.json"`. The default is best for most cases.
- `load_db_uri` - Should be `${OPENSANCTIONS_DATABASE_URI}` in most datasets. Used to define the database into which statements will be loaded to be accessed from the statements API. It is not set for datasets including other datasets, or whose data isn't included in full in the main data products.

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

### Date formatting

- `dates` - date formatting used by [helpers.apply_date and apply_dates](helpers.md#zavod.helpers.apply_date) but also accessible via the context for use in `helpers.parse_date`.
  - `formats`: Array of date format strings for parsing dates into partial ISO dates
  - `months`: Map where values like `März` are translated into keys like `"3"` so that it could then be parsed by a format string like `%m`

### HTTP options

HTTP requests for GET requests are automatically retried for connection and HTTP errors. Some of this retry behaviour can be configured from the dataset metadata if needed.

- `http`
    - `user_agent`: string, defaults to the value of the HTTP_USER_AGENT setting. Set a custom value for the `User-Agent` header if needed.
    - `backoff_factor`: float, default `1`. [Scales the exponential backoff](https://urllib3.readthedocs.io/en/stable/reference/urllib3.util.html#urllib3.util.Retry.DEFAULT_ALLOWED_METHODS:~:text=with%20None.-,backoff_factor,-(float)%20%E2%80%93).
    - `max_retries`: integer in seconds, default `3`
    - `retry_methods`: List of strings, [default](https://urllib3.readthedocs.io/en/stable/reference/urllib3.util.html#urllib3.util.Retry.DEFAULT_ALLOWED_METHODS) `['DELETE', 'GET', 'HEAD', 'OPTIONS', 'PUT', 'TRACE']`
    - `retry_statuses`: List of integers of HTTP error codes to retry, default `[413, 429, 503]`.
  
### Data assertions

Data assertions are intended to "smoke test" the data. Assertions are checked on export. If assertions aren't met, warnings are emitted.

Data assertions are checked when running `zavod run` (and `zavod validate --clear` is useful when developing a crawler).

Data assertions are useful to communicate our expectations about what's in a dataset, and a soft indication (they don't cause the export to fail) to us that something's wrong in the dataset or crawler and needs attention.

We usually use the minima to set a baseline for what should be in the dataset, and one or more simpler maxima to just identify when the dataset has grown beyond the validity of our earlier baseline, or if something's gone horribly wrong and emitted way more than expected.

It's a good idea to add assertions at the start of writing a crawler, and then see whether those expectations are met when the crawler is complete. A good rule of thumb for datasets that change over time is minima 10% below the expected number to allow normal variation, unless there's a known hard minimum, and a maximum around 20% above the expectation to leave room to grow, if the number is expected to fluctuate.

- `assertions`
  - `min` and `max` can each have the following children
    - `schema_entities` - its children are Thing descendants with their value being an integer indicating the minimum or maximum number of entities of that schema
    - `country_entities` - its children are ISO 3166-1 Alpha-2 country codes, with their value being an integer indicating the min or max entities with that country
    - `countries` - its value is an integer indicating the min or max number of countries expected to come up in the dataset


e.g. the following means

- at least 160 Persons
- at least 30 Positions
- at least 40 entities for the US
- at least 30 entities for China
- at least one entity for Brunei
- at least 6 countries
- at most 200 Persons
- at most 40 Positions

```yaml
assertions:
  min:
    schema_entities:
      Person: 160
      Position: 30
    country_entities:
      us: 40
      cn: 30
      bn: 1
    countries: 6
  max:
    schema_entities:
      Person: 200
      Position: 40
```
