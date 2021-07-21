
OpenSanctions technical documentation
=====================================

This technical documentation is intended to be read by Python developers who wish to
run the OpenSanctions crawlers on their own infrastructure, or plan to add their own
crawlers to the system.

For users who merely want to consume the datasets produced by the project, we suggest
you browse the `project home page <https://opensanctions.org>`_ instead.

System overview
---------------

The OpenSanctions pipeline handles the following key steps:

* Fetching entity data from online sources and parsing the original format
* Normalising the entities in source data into the :ref:`OpenSanctions data model <entities>`
* Storing entities to a local staging database for processing
* Merging entities across different :ref:`sources <datasets>` (TODO)
* Exporting data into a variety of target formats (JSON, CSV)

These steps are triggered using a command-line utility, ``opensanctions``, which can
run parts or all of this process for specific segments of the data.


.. toctree::
   :maxdepth: 2
   :caption: Contents:

   install.rst
   usage.rst
   crawlers.rst
   helpers.rst


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
