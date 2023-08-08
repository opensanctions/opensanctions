# Using the zavod command-line tool 

Before we dive into using the software, however, let's explore some of the concepts underlying the system.

## Datasets, sources and collections

`zavod` collects data from a variety of sanctions lists and other data providers and converts it into a common, simple-to-use data model. These data are grouped into **datasets**. Some datasets are **sources** and refer to a data origin (e.g. ``eu_fsf``, the EU sanctions list. Other datasets combine data from multiple sources into a **collection** (e.g. ``sanctions``, which collects all sanctions entities from multiple sources).

Both **source** and **collection datasets** have a metadata definition, stored as a YAML file in ``opensanctions/metadata``. **Sources** also include crawler code to parse and import the material. This code is usually located in ``opensanctions/crawlers``.

## Entities and targets

The main objective of `zavod` is to combine data from multiple sources into a common data model. To this end, the system uses [FollowTheMoney](/docs/entities/) (FtM), a data modelling and validation library which defines a set of [entity schemata](/reference/#schema), such as [Person](/reference/#schema.Person), [Company](/reference/#schema.Company), 
[Address](/reference/#schema.Address) or [Sanction](/reference/#schema.Sanction). FtM-based entities are stored in a local database and then exported to a variety of file formats.

A peculiarity of the data in OpenSanctions is that sources may mention entities that are merely adjacent to a sanctions target, but not themselves sanctioned. To distinguish the sanctioned entities, they are flagged as **targets** in the database. For most end users that wish to download and use a simple CSV file, chances are that they will want sanctions targets, without the secondary entities in the dataset.
