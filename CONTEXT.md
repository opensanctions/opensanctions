The OpenSanctions repository contains the zavod library and crawler implementations in the datasets directory.


## Zavod

Zavod is our ETL framework. An introduction/toturial is found in [zavod/docs/tutorial.md](zavod/docs/tutorial.md)


## Crawlers

Each crawler consists of a yaml file which refers to a python function under the entrypoint key.
The yaml file for a dataset has the of the metadata for the crawler.
The metadata fields are specified in [zavod/docs/metadata.md](zavod/docs/metadata.md).

The best practices for crawler implementation is specified under `zavod/docs/best_practices`,
e.g. [zavod/docs/best_practices/patterns/]([zavod/docs/best_practices/patterns/])

Crawlers sometimes have data fixes under the lookups key in their metadata yaml file.
