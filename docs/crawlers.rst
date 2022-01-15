
Developing a crawler
=====================

.. note::

   Please consult the `contribution guidelines`_ before developing new crawlers to
   learn about inclusion criteria for new data sources.

.. _contribution guidelines: https://github.com/pudo/opensanctions/blob/main/CONTRIBUTING.md

A crawler is a small Python script that will import data from a web origin and store
it as entities as a :ref:`source dataset <datasets>`. The basic process for creating a
new crawler is as follows:

1. File a `GitHub issue <https://github.com/pudo/opensanctions/issues>`_ to discuss the
   suggested source
2. Create a YAML metadata description for the new source
3. Create a Python script to fetch and process the data
4. Address any data normalisation issues the framework might report

In the future, an additional step will be required to link up duplicate entities against
other sources and to define canonical/merged entities.

Source metadata
----------------

Before programming a crawler script, you need to create a YAML file with some basic
metadata to describe the new dataset. That information includes the dataset name
(which is normally derived from the YAML file name), information about the source
publisher and the source data URL.

The metadata file must also include a reference to the *entry point*, the Python
code that should be executed in order to crawl the source.

Create a new YAML file in the path ``opensanctions/metadata`` named after your new
dataset. By convention, a dataset name should start with the ISO 3166-2 code of the
country it relates to, and name parts should be separated by underscores. The
contents of the new metadata file should look like this:

.. warning:: 

   The dataset metadata format is going to be subject to significant change.

.. code-block:: yaml

   title: "Financial Sanctions Files (FSF)"
   url: https://eeas.europa.eu/

   # The description should be extensive, and can use markdown for formatting:
   description: >
      As part of the Common Foreign Security Policy thr European Union publishes
      a sanctions list that is implemented by all member states.
   
   # The Python module that contains the crawler code:
   entry_point: opensanctions.crawlers.eu_fsf_demo

   # A prefix will be used to mint entity IDs. Keep it short.
   prefix: eu-fsf

   # Define what collections the source is part of. All sources are added to a
   # magical collection called 'all'. Each collection also has its own YAML 
   # metadata file, but the link between a source and the collections it is a
   # part of is established via the source metadata, not the collection metadata.
   collections:
      - sanctions
   
   # This section provides information about the original publisher of the data,
   # often a government authority:
   publisher:
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

Once that YAML file is stored in the correct folder, you should be able to run
command-line operations against the dataset, for example (if your metadata file is
named `eu_fsf_demo.yml`):

.. code-block:: bash

   $ opensanctions run eu_fsf_demo
   ....
   ModuleNotFoundError: No module named 'opensanctions.crawlers.eu_fsf_demo'

That error will be addressed in the next section, by adding a crawler script.

Developing a crawler script
-----------------------------

In order to actually feed data into the data source, we need to write a crawler script.
The script location is specified in the YAML metadata file as ``entry_point:``. This
also means you could reference the same script for multiple data sources, for example
in a scenario where two data sources use the API, except with some varied parameters.

In our example above, we'd create a file in ``opensanctions/crawlers/eu_fsf_demo.py``
with a crawler skeleton:

.. code-block:: python

   def crawl(context):
       context.log.info("Hello, World!")

Running the crawler (``opensanctions crawl eu_fsf_demo``) should now produce a log
line with the message *Hello, World!*

You'll notice that the ``crawl()`` function receives a
:py:class:`context <opensanctions.core.context.Context>` object. Think of it as a
sort of sidekick: it helps you to create, store and document data in your crawler.

Fetching and storing resources
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Many crawlers will start off by downloading a source data file, like a CSV table or a
XML document. The :py:class:`context <opensanctions.core.context.Context>` provides
utility methods that let you fetch a file and store it into the crawlers working
directory. Files stored to the crawler home directory (``context.path``) will later be
uploaded and published to the web.

.. code-block:: python

   def crawl(context):
       # Fetch the source data URL specified in the metadata to a local path:
       source_path = context.fetch_resource('source.xml', context.dataset.data.url)
       with open(source_path, 'r') as fh:
           print(len(fh.read()))

       # You can also register the file as a resource with the dataset that
       # will be included in the exported metadata index:
       context.export_resource(source_path, title="Source data XML file")

Other crawlers might not be as lucky: instead of fetching their source data as a
single bulk file, they might need to crawl a large number of web pages to collect
the necessary data. For this, access to a pre-configured Python ``requests``
:ref:`session object <requests:session-objects>` is provided:

.. code-block:: python

    from lxml import html

    def crawl(context):
        response = context.http.get(context.dataset.data.url)
        
        # Parse the HTTP response into an lxml DOM:
        doc = html.fromstring(response.text)

        # Query the DOM for specific elements to extract data from:
        for element in doc.findall('.//div[@class="person"]'):
            context.log.info("Element", element=element)

Responses from the ``context.http`` session are cached between different runs of
the crawler and will be cached for up to 10 days. You can partially disable this
by adding a timestamp parameter to the fetched URLs.

Creating and emitting entities
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The goal of each crawler is to produce data about :ref:`persons and other entities of
interest <entities>`. To enable this, the ``context`` provides a number of helpers
that construct and store :ref:`entities <entities>`:

.. code-block:: python

    def crawl(context):

       # Create an entity object to which other information can be assigned: 
       entity = context.make("Person")

       # Each entity needs a unique ID. In OpenSanctions, this is often derived
       # from the ID of a source database, or a string:
       entity.make_slug('Joseph Biden')

       # Assign some property values:
       entity.add('name', 'Joseph Robinette Biden Jr.')
       entity.add('alias', 'Joe Biden')
       entity.add('birthDate', '1942-11-20')
       
       # Invalid property values ('never' is not a date) will produce a log
       # error:
       entity.add('deathDate', 'never')
       
       # Store or update the entity in the database:
       context.emit(entity, target=True)

The entity object is based on the entity proxy in FollowTheMoney, so we suggest you
also check out the :ref:`FtM documentation <followthemoney:api>` on entity
construction. Some additional utility methods are added in the
:py:class:`entity class <opensanctions.core.entity.Entity>` in OpenSanctions.


Checklist
---------

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
  to be emitted.
* Bonus points: your Python code is linted and formatted with ``black``.

Using the context
------------------

.. autoclass:: opensanctions.core.context.Context
   :members:
   :undoc-members:


.. autoclass:: opensanctions.core.entity.Entity
   :members:
   :undoc-members:


Type lookups
-------------

TODO