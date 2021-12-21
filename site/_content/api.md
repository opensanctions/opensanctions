---
title: Programmatic data use via the API
summary: >
    You can search our database using the API, or conduct batch entity matching to see if any people or companies are listed on a sanctions or PEPs list.
---

The [OpenSanctions API](https://api.opensanctions.org/) is an open source search appliance built to let users search and cross-reference sanctions, criminals and PEPs data.

* [API documentation (ReDoc)](https://api.opensanctions.org/)
* [API documentation (Swagger)](https://api.opensanctions.org/docs)
* [Installation instructions](https://github.com/pudo/opensanctions/blob/main/api/README.md) for the API service, based on `docker-compose`.
* [License terms](/licensing/) for the data (CC 4.0 BY NC).

***Important notice:** `api.opensanctions.org` is a demo service. It should only be used for experimentation or one-off projects. If you want to use the API as part of an application your are building, you should either [operate the service locally](https://github.com/pudo/opensanctions/blob/main/api/README.md), or [contact us](/contact/) to discuss a service agreement.*

Running the API on your own premises will guarantee privacy and security, while ensuring your independence from OpenSanctions as a service provider.


## Endpoints

In its current iteration, the API includes the following endpoints:

* [``/search``](https://api.opensanctions.org/#operation/search_search__dataset__get) enables conventional full-text search of the database, including the boolean operators and Lucene syntax available in ElasticSearch.

* [``/match``](https://api.opensanctions.org/#operation/match_match__dataset__post) lets programmatic users submit a set of entity examples and search all of them at once. The results will be scored by considering all the supplied attributes (e.g. persons with nationality, birth dates, etc.), rather than search the name only. This endpoint should be used to implement know-your-customer (KYC) style list checks against a local set of records.

* [``/reconcile``](https://api.opensanctions.org/#tag/Reconciliation) implements the [Reconciliation Service API](https://reconciliation-api.github.io/specs/latest/). This [can be used in conjunction with the OpenRefine desktop application](https://docs.openrefine.org/manual/reconciling) to match the rows in a spreadsheet against sanctions or PEPs lists.

* [``/entities/<ID>``](https://api.opensanctions.org/#operation/fetch_entity_entities__entity_id__get) gives users direct access to all the data linked to a specific entity. This includes the local graph context of the entity, i.e. any entities linked to or from the given item.

* [``/statements``](https://api.opensanctions.org/#operation/statements_statements_get) gives users direct access the raw data storage of OpenSanctions, which is a statement-based data model. This can be used to determine which property values on a given entity stem from which data source, and when they were first included in that source.

