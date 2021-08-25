---
title: Using the data
summary: |
    The datasets published by the project are made available in multiple
    formats, suitable for different purposes.
---

> If you would like to see another file format or slice of the data included in
> this project, please [get in touch](/contact/) to discuss your idea.

Please also refer to the FAQ about [API access](/docs/faq/#api).

## <a id="formats" /> Formats

Bulk data is made available in the following formats for each data source and
[collection](/docs/faq/#collections) we maintain:

### Simplified CSV (comma-separated values) table

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
* ``countries``: a list of countries linked to this entity. Includes countries of residence, nationalities and corporate jurisdictions.
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

### Names-only text file

The simplest format we publish is a simple text file with the names of all
persons and companies targeted in each dataset, one name per line. The format can
be used for:

* [Batch searches in ICIJ DataShare](https://icij.gitbook.io/datashare/all/batch-search-documents)
* Simple text matching using regular expressions.

The plain text files are encoded in ``utf-8``. If non-latin names don't show up
correctly in your application, make sure you've opened the file with the
right encoding.