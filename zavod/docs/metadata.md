# Dataset metadata

Excellent dataset metadata is a relatively low-effort way to demonstrate the transparency which underpins OpenSanctions. Write it considering the perspective of data users ranging from startup software developers and business analysts, to investigative journalists and researchers.

Remember to give the context that people from different countries need to make sense of systems they are not entirely familiar with. Share what you learned when figuring out what a source dataset represents.

- `title` - As close as possible to an official title for what this dataset contains. If it is a subset of its source data, try to capture that. e.g. `Plural Legislators` - if the Plural portal includes committees but the dataset only captures the legislators.
- `entry_point` e.g. `crawler.py:crawl_peps` - the file name, optionally followed by a method name called by the zavod `crawl` command. Defaults to the `crawler.py:crawl` calling an entry point in the dataset directory.
- `prefix` - e.g. `ofac` 
- `coverage`
    - `frequency` - e.g. `daily`, `weekly`, `monthly`. This represents how often it is expected that this dataset will be updated. It conveys to users how often to expect updates, and will also be used to generate a crawling schedule unless a specific schedule is defined.
    - `start` - The start date of a dataset which covers only a specific period in time, e.g. for a dataset specific to a data dump or parliamentary term. A string in the format `YYYY-MM-DD`.
    - `end` - The end date of a dataset which covers only a specific period in time, e.g. for a dataset specific to a data dump or parliamentary term. A string in the format `YYYY-MM-DD`. Future dates imply an expected end to the maintenance and coverage period of the dataset. Past end dates result in the datasets last_change date being fixed to that date, while its last_exported date remains unchanged.
- `deploy`
    - `schedule` - a cron style schedule defining what time and frequency a crawler should run, e.g `30 */6 * * *`
- `exports` - An array of strings matching the [export formats](https://www.opensanctions.org/docs/bulk/), e.g. `"targets.nested.json"`. The default is best for most cases.
- `summary` - Capture what a user of the dataset needs to know to decide if it's what they're looking for in a single clear concise line. This is used in search results.
- `description` - This can be one to two paragraphs of text. A more complete description of the dataset, perhaps with a bit more detail about what it includes, what it excludes, and how it is kept up to date if it is not from an official publisher.
- `publisher`
    - `name` - The publisher's official name 
    - `description` - This can be one to two paragraphs of text. Use the publisher description field to explain to someone from a country other than the publisher who the publisher is, and why they do what they do. 
    - `url` - The home page of their official website
    - `country` - The Alpha-2 or two-letter ISO 3166-1 
    - `official` - `true` if the publisher is an authority overseeing the subject data, generally a government entity releasing their sanctions list or legislator data, otherwise `false`.
- `url` - the home page or most authoritative place where someone can read about this particular dataset at its source. E.g If a source publishes 5 different datasets, try to link to the page describing the data actually contained in this dataset.
- `data`
    - `url`- The link to a bulk download or API base URL or endpoint - ideally something you can use within the crawler via `context.data_url` to request the data, and which ideally returns a useful response when followed by dataset users. It's not the end of the world if you make other requests to expand the data available to the crawler.
    - `format` a string defining the format of the data at that URL, e.g. `JSON`, `HTML`, `XML`. A Zip file containing thousands of YAML files might be more usefully annoted with `YAML` than `ZIP` because it conveys the structural syntax of the data.
