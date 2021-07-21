
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
   entry_point: opensanctions.crawlers.eu_fsf

   # A prefix will be used to mint entity IDs. Keep it short.
   prefix: eu-fsf

   # Define what collections the source is part of. All sources are added to a
   # magical collection called 'all'.
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


Developing a crawler script
-----------------------------


Using the context
------------------

.. autoclass:: opensanctions.core.context.Context
   :members:
   :undoc-members:
