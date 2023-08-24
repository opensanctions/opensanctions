# Developing a crawler

A crawler is a small Python script that will import data from a web origin and
store it as entities as a data source. `zavod` defines a framework for
crawlers to retrieve data, parse it and emit structured data about people or 
companies into a database.

!!! info "Please note"
    Before you contribute a crawler, please consider if you are willing
    to remain involved in its maintenance after having the code included in
    `zavod`. Maintaining a crawler is an ongoing commitment, and the OpenSanctions
    team does not automatically assume that responsibility for code contributed by
    others. See our general [inclusion critera](https://www.opensanctions.org/docs/criteria/).

1. Make sure you have [installed zavod](install.md).
2. File a [GitHub issue](https://github.com/opensanctions/opensanctions/issues)
   to discuss the suggested source.
3. Create a [YAML metadata](#data-source-metadata) description for the new source.
4. Create a [Python script](#developing-a-crawler-script) to fetch and process the data.
5. Address any data normalisation issues the framework might report.

## Data source metadata

Before programming a crawler script, you need to create a YAML file with some basic metadata to describe the new dataset. That information includes the dataset name (which is normally derived from the YAML file name), information about the source publisher and the source data URL.

The metadata file must also include a reference to the entry point, the Python code that should be executed in order to crawl the source.

Create a new YAML file at the path ``datasets/cc/source/cc_source.yml`` replacing `cc` with the relevant ISO 3166-2 country code, and `source` with an acronym or short name for the source, separating name parts using underscores. Other codes may be derived from standard acronyms instead of country codes for regions that span beyond one country.

!!! warning "Important"

    Metadata is essential to making our data useable. We will not merge additional
    crawlers which don't have metadata, or where the descriptions are cryptic.

    Read your metadata as if it's your first time, and ask yourself if other
    readers will understand the scope and limitations of the dataset. For simple
    crawlers, writing the metadata can take as much time as writing the code.

The contents of the new metadata file should look something like this:

```yaml
name: eu_fsf_demo
title: "Financial Sanctions Files (FSF)"
url: https://eeas.europa.eu/

# The description should be extensive, and can use markdown for formatting:
description: >
    As part of the Common Foreign Security Policy thr European Union publishes
    a sanctions list that is implemented by all member states.

# The Python module in the same director that contains the crawler code:
entry_point: crawler.py

# A prefix will be used to mint entity IDs. Keep it short.
prefix: eu-fsf

# This section provides information about the original publisher of the data,
# often a government authority:
publisher:
    name: European Commission
    organization: European Commission
    authority: European Union External Action Service
    acronym: EEAS
    country: eu
    url: https://eeas.europa.eu/topics/sanctions-policy/8442/consolidated-list-of-sanctions_en

# Information about the data, including a deep link to a downloadable file, if
# one exists.
data:
    url: https://webgate.ec.europa.eu/europeaid/fsd/fsf/public/files/xmlFullSanctionsList_1_1/content
    format: XML
```

## Running a dataset crawler

Once that YAML file is stored in the correct folder, you should be able to run command-line operations against the dataset. For example (if your metadata file is named `eu_fsf_demo.yml`):

```bash
$ zavod crawl datasets/eu/fsf/eu_fsf_demo.yml
....
2023-08-01 12:36:24 [warning  ] No backfill bucket configured  [zavod.archive] 
2023-08-01 12:36:24 [info     ] Running dataset                [eue_fsf_demo] dataset=eue_fsf_demo path=/home/you/opensanctions/data/datasets/eue_fsf_demo
2023-08-01 12:36:24 [error    ] Runner failed: Could not load entry point: crawler [eue_fsf_demo] dataset=eue_fsf_demo
```

Don't worry about the backfill bucket warning - that is not needed when developing crawlers. It is used in production to automatically track when data was previously seen and updated.

The `Runner failed: Could not load entry point: crawler` error indicates that it looked for our crawler and couldn't find it. Adding the crawler script is the next step.

!!! info "Dry run mode"
    You can switch zavod to dry run during crawler development by adding the `-d` (or `--dry-run`) flag on the command line. A dry run will not store any of the emitted data, and disable the generation of correct timestamps, which is slow.

    `zavod crawl -d datasets/eu/fsf/eu_fsf_demo.yml`

## Developing a crawler script

In order to actually feed data into the data source, we need to write a crawler script. The script location is specified in the YAML metadata file as ``entry_point:``. This also means you could reference the same script for multiple data sources, for example in a scenario where two data sources use the API, except with some varied parameters.

In our example above, we'd create a file in `datasets/eu/fsf/crawler.py` with a crawler skeleton:

```python
from zavod import Context

def crawl(context: Context):
    context.log.info("Hello, World!")
```

Running the crawler (`zavod crawl datasets/eu/fsf/eu_fsf_demo.yml`) should now produce a log line with the message *Hello, World!*

You'll notice that the ``crawl()`` function receives a [`Context`][zavod.context.Context] object. Think of it as a sort of sidekick: it helps you to create, store and document data in your crawler.

### Fetching and storing resources

Many crawlers will start off by downloading a source data file, like a CSV table or a
XML document. The [`context`][zavod.context.Context] provides utility methods that let you fetch a file and store it into the crawlers working directory. Files stored to the crawler home directory and [exported as resources][zavod.context.Context.export_resource] will later be uploaded and published to the web.

```python
def crawl(context):
    # Fetch the source data URL specified in the metadata to a local path:
    source_path = context.fetch_resource('source.xml', context.dataset.data.url)
    with open(source_path, 'r') as fh:
        print(len(fh.read()))

    # You can also register the file as a resource with the dataset that
    # will be included in the exported metadata index:
    context.export_resource(source_path, title="Source data XML file")
```

Other crawlers might not be as lucky: instead of fetching their source data as a single bulk file, they might need to crawl a large number of web pages to collect the necessary data. For this, access to a pre-configured Python ``requests`` session object is provided:

```python
from lxml import html

def crawl(context):
    response = context.http.get(context.dataset.data.url)
    
    # Parse the HTTP response into an lxml DOM:
    doc = html.fromstring(response.text)

    # Query the DOM for specific elements to extract data from:
    for element in doc.findall('.//div[@class="person"]'):
        context.log.info("Element", element=element)
```

Responses from the ``context.http`` session can also be cached using built-in helper methods:

```python
from lxml import html

def crawl(context):
    # Fetch, cache and parse the HTTP response into an lxml DOM:
    doc = context.fetch_html(context.dataset.data.url, cache_days=7)

    # Query the DOM for specific elements to extract data from:
    for element in doc.findall('.//div[@class="person"]'):
        context.log.info("Element", element=element)
```

### Creating and emitting entities

The goal of each crawler is to produce data about persons and other entities of interest. To enable this, the [`context`][zavod.context.Context] provides a number of helpers that construct and store [entities](https://www.opensanctions.org/docs/entities/):

```python
def crawl(context):

    # Create an entity object to which other information can be assigned: 
    entity = context.make("Person")

    # Each entity needs an ID which is unique within the source database, and
    # ideally consistent over time.
    # This is often ideally derived from its ID in the source database,
    # or a string with the above properties. See Patterns below.
    entity.id = context.make_id('Joseph Biden')

    # Assign some property values:
    entity.add('name', 'Joseph Robinette Biden Jr.')
    entity.add('alias', 'Joe Biden')
    entity.add('birthDate', '1942-11-20')
    
    # Invalid property values ('never' is not a date) will produce a log
    # error:
    entity.add('deathDate', 'never')
    
    # Store or update the entity in the database:
    context.emit(entity, target=True)
```

The [entity object][zavod.entity.Entity] is based on the [entity proxy in FollowTheMoney](https://followthemoney.tech/reference/python/followthemoney/proxy.html#EntityProxy), so we suggest you also check out the [FtM documentation](https://followthemoney.tech/docs/api/) on entity construction. Some additional utility methods are added in the [`Entity`][zavod.entity.Entity] class in `zavod`.

### Verifying your output

Now that you're extracting data, it's a good idea to start verifying your output. Start by exportng your crawler's data:

```bash
zavod export datasets/eu/fsf/eu_fsf_demo.yml
```

This will log a number of different file types that are exported by default. A nice way
to explore the output is using the JSON command line utility `jq` and your favourite text
pager like `less` together to browse and search within the `[targets.nested.json](https://www.opensanctions.org/docs/bulk/json/#:~:text=targets.nested.json)`
output using a command like

```bash
jq . data/datasets/eu_fsf_demo/targets.nested.json --color-output | less
```

Good things to check are

- The number of entities produced of each type are as expected for your dataset
- Spot checking some specific persons, companies, and relations between them, as relevant to your data
- Any warnings in the crawler output

### Next steps

You may now want to level up your crawler by looking at

- [helpers](helpers.md) for common tasks,
- [common patterns](patterns.md) for building crawlers,
- the [PEPs guide](peps.md) if you're crawling Politically Exposed Persons and their Relatives and Close Associates.

## Checklist

When contributing a new data source, or some other change, make sure of the following:

* You've created a metadata YAML file with detailed descriptions and links to the source
  URL.
* Your code should run after doing a simple ``pip install`` of the codebase. Include
  additional dependencies in the ``setup.py``. Don't use non-Python dependencies like
  ``Headless Chrome`` or ``Selenium``.
* The output data for your crawler should be Follow The Money objects. If you need more
  fields added to the ontology, submit a pull request upstream. Don't include left-over
  data in an improvised way.
* Include verbose logging in your crawler. Make sure that new fields or enum values
  introduced upstream (e.g. a new country code or sanction program) will cause a warning
  to be emitted. [Warnings](https://www.opensanctions.org/issues) are checked regularly to identify when a crawler needs attention.
  Info and lower level logs are useful for debugging with the `-v` flag.
* Bonus points: your Python code is linted and formatted with ``black``.
