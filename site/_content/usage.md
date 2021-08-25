---
title: Using the data
summary: |
    The datasets published by the project are made available in multiple
    formats, suitable for different purposes.
---

> If you would like to see another file format or slice of the data included in
> this project, please [get in touch](/contact/) to discuss your idea.

Please also refer to the FAQ about [API access](/docs/faq/#api) and the
[data structure reference](/docs/reference/).

## <a id="formats" /> Formats

Bulk data is made available in the following formats for each data source and
[collection](/docs/faq/#collections) we maintain:

### <a id="targets.simple.csv"> Simplified CSV (comma-separated values) table

The tabular data export is targeted at analysts who wish to access the OpenSanctions
data in a spreadsheet application like Microsoft Excel (help: [open CSV in Excel](https://support.microsoft.com/en-us/office/import-or-export-text-txt-or-csv-files-5250ac4c-663c-47ce-937b-339e391393ba)).

Unfortunately, the structure of persons of interest data does not easily lend itself to
a simple tabular form. For example, a person might have multiple nationalities, or have
been a member of several political parties in their career.

The "Simplified CSV" format addresses this by presenting a highly limited view of the
data, in which only a select set of key columns is provided. These include:

* ``id``: the unique identifier of the given entity.
* ``schema``: the [entity type](/docs/reference/#schema).
* ``name``: the display name of the given entity.
* ``aliases``: any alias names (e.g. other scripts, nom de guerre) provided by the data sources.
* ``birth_date``: for people, their birth date.
* ``countries``: a list of [countries](/docs/reference/#type.country) linked to this entity. Includes countries of residence, nationalities and corporate jurisdictions.
* ``addresses``: a list of known addresses for the entity.
* ``identifiers``: identifiers such as corporate registrations, passport numbers or tax identifiers linked to this sanctions target.
* ``sanctions``: details regarding the sanctions designation, if any.
* ``phones``: a list of phone numbers in E.164 format.
* ``emails``: a list of email addresses linked to the entity.
* ``dataset``: the dataset this entity is in.
* ``last_seen``: the last time this entity was observed in source data.
* ``first_seen``: the earliest date this entity has been noticed by OpenSanctions.

Further technical notes:

* The CSV is formatted using ``,`` (comma) as a delimiter, encoded as ``utf-8``.
* Some fields in the CSV can contain multiple values in one cell. These are stored as a
  nested CSV using the ``;`` (semicolon) delimiter.
* The export contains only [targeted entities](/docs/reference/#targets), not all entities
  in the dataset.

### <a id="names.txt"></a> Names-only text file 

The simplest format we publish is a simple text file with the names of all
persons and companies targeted in each dataset, one name per line. The format can
be used for:

* [Batch searches in ICIJ DataShare](https://icij.gitbook.io/datashare/all/batch-search-documents)
* Simple text matching using regular expressions.

The plain text files are encoded in ``utf-8``. If non-latin names don't show up
correctly in your application, make sure you've opened the file with the
right encoding.

### <a id="entities.ftm.json"></a><a id="targets.nested.json"></a> JSON/FollowTheMoney-based formats

We offer two JSON-based export formats that are both based on [the FollowTheMoney](https://followthemoney.readthedocs.io/en/latest/index.html) (FtM). They are a close representation of the [internal data structure](/docs/reference) of OpenSanctions. The *nested JSON* format should be the preferred import method for software-based data consumers.

You can get [a basic overview of the entity data structure](https://followthemoney.readthedocs.io/en/latest/entity.html#id1) in the FtM documentation, and browse the [data dictionary](/docs/reference) to see details regarding the properties used by OpenSanctions.

Both formats use [line-delimited JSON](https://en.wikipedia.org/wiki/JSON_streaming#Line-delimited_JSON): each line of the exported files is a separate entity. While the *FollowTheMoney entities* (``entities.ftm.json``) export contains one entity per line, the *nested JSON* (``targets.nested.json``) format contains one line per [target](/docs/reference/#targets), with adjacent entities (e.g. addresses, sanctions) nested inside the properties section of the data structure.

The nested format and some of the provided metadata (``dataset``, ``first_seen``, ``last_seen``) are not part of FtM, but extensions developed for OpenSanctions.

Some further documentation regarding FtM tooling:

* [Converting FtM entities to a Neo4J property graph](https://docs.alephdata.org/developers/followthemoney/ftm#exporting-data-to-a-network-graph)
* [Converting an FtM file to a Gephi file (GEXF)](https://docs.alephdata.org/developers/followthemoney/ftm#gexf-for-gephi-sigma-js)
* [Converting to RDF/Linked Data](https://docs.alephdata.org/developers/followthemoney/ftm#exporting-entities-to-rdf-linked-data)
* [Command-line Aleph import](https://docs.alephdata.org/developers/alephclient#writing-a-stream-of-entities-to-a-collection) (you can also import the *FollowTheMoney entities* (``entities.ftm.json``) data into Aleph by
uploading the file to an investigation)