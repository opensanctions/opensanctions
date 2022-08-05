Using the system
=================

Below you'll find instructions on how to run the OpenSanctions software and how to
add additional crawlers to the system. Before we dive into that, however, let's
explore some of the concepts underlying the system.

.. _datasets:

Datasets, sources and collections
----------------------------------

OpenSanctions collects data from a variety of sanctions lists and other data providers
and converts it into a common, simple-to-use data model. These data are grouped into
**datasets**. Some datasets are **sources** and refer to a data origin (e.g. ``eu_fsf``,
the EU sanctions list. Other datasets combine data from multiple sources into a
**collection** (e.g. ``sanctions``, which collects all sanctions entities from multiple
sources).

Both **source** and **collection datasets** have a metadata definition, stored as a
YAML file in ``opensanctions/metadata``. **Sources** also include crawler code to parse
and import the material. This code is usually located in ``opensanctions/crawlers``.


.. _entities:

Entities and targets
---------------------

The main objective of OpenSanctions is to combine data from multiple sources into a common
data model. To this end, the system uses
`FollowTheMoney <https://followthemoney.readthedocs.io/en/latest/index.html>`_ (FtM),
a data modelling and validation library which defines a set of
:ref:`entity schemata <followthemoney:entities>`, such as
:ref:`Person <followthemoney:schema-Person>`, :ref:`Company <followthemoney:schema-Company>`, 
:ref:`Address <followthemoney:schema-Address>`
or :ref:`Sanction <followthemoney:schema-Sanction>`.
FtM-based entities are stored in a local database and then exported to a variety of file
formats.

A peculiarity of the data in OpenSanctions is that sources may mention entities that are
merely adjacent to a sanctions target, but not themselves sanctioned. To distinguish
the sanctioned entities, they are flagged as **targets** in the database. For most end
users that wish to download and use a simple CSV file, chances are that they will want
sanctions targets, without the secondary entities in the dataset.


.. _cli:

Using the command-line tool
----------------------------

Once you've successfully :ref:`installed <install>` OpenSanctions, you can use the
built-in command-line tool to run parts of the system:

.. code-block:: bash

    # Crawl and export the US consolidated list:
    $ opensanctions crawl us_ofac_cons
    $ opensanctions export us_ofac_cons

    # This works for both sources and collections. Running a collection will
    # crawl all related sources and then export the collection data:
    $ opensanctions crawl sanctions
    $ opensanctions export sanctions

    # Crawling without a specified dataset name will default to using the
    # `all` collection which contains all sources:
    $ opensanctions crawl
    $ opensanctions crawl all

    # If you're developing the crawler, you can skip generating the exports and
    # only run the crawl stage:
    $ opensanctions crawl us_ofac_cons

    # Inversely, you can also export a dataset without re-crawling the sources:
    $ opensanctions export us_ofac_cons

    # During development you might also want to force delete all data linked
    # to a source:
    $ opensanctions clear us_ofac_cons

The available dataset names are determined from the set of metadata YAML files
found in ``OPENSANCTIONS_METADATA_PATH`` (see: :ref:`configuration <config>`).

