# OpenSanctions

OpenSanctions aggregates and provides a comprehensive open-source database of sanctions data, politically exposed persons, and related entities. Key functionalities in this codebase include:

- **Parsing** of raw source data.
- **Cleaning** and standardization of data structures.
- **Deduplication** to maintain data integrity.
- **Exporting** the data into a variety of output formats.

We build on top of the [Follow the Money](https://www.followthemoney.tech) framework, a JSON-focused anti-corruption data model, as the schema for all our crawlers. FtM data is then optionally exposed to simplified formats like CSV.

## Quick Links
* [OpenSanctions Website](https://www.opensanctions.org/)
    * [Datasets](https://www.opensanctions.org/datasets/)
    * [FAQs](https://www.opensanctions.org/docs/faq/)
    * [API Documentation](https://api.opensanctions.org/)
* [zavod Documentation](https://zavod.opensanctions.org/)
* [Data Sources Roadmap](https://bit.ly/osa-sources) - See our current plans!
* [Data Licensing Details](https://www.opensanctions.org/licensing/)
* [Reach Out to Us](https://www.opensanctions.org/contact/)

## Collaborate with us in Development

### Introduction

At the heart of our project is a crawler framework dubbed ``zavod``. To activate the project, you have the option to employ either `docker-compose.yml` or the `Makefile`.

For an enriched experience backed by extensive documentation, we recommend opting for the `Makefile`. More details can be found in the [zavod documentation](https://zavod.opensanctions.org/).

### Environment Setup

1. **Database Initialization**:

``zavod`` can use a database in order to cache information from the data sources. Launch a terminal and set up your database with:

```bash
docker compose up -d db
```

2. **Project Building**:

Next, commence the build process with:

```bash
make build
# Alternatively, for direct execution:
docker-compose build --pull
```

### Deploying the Crawler

Kickstart the crawling process with:

```bash
# This zeroes in on the dataset located in the datasets directory
docker compose run --rm app zavod crawl datasets/de/abgeordnetenwatch/de_abgeordnetenwatch.yml
```

## Associated Repositories

- [opensanctions/nomenklatura](https://github.com/opensanctions/nomenklatura): building on top of FollowTheMoney, `nomenklatura` provides a framework for storing data statements with full data lineage, and for integrating entity data from multiple sources. It also handles the data enrichment function that links OpenSanctions to external databases like OpenCorporates.
- [opensanctions/yente](https://github.com/opensanctions/yente): API for entity matching and searching.

## Licensing

The code within this repository is licensed under MIT. For content and data, we adhere to [CC 4.0 Attribution-NonCommercial](https://www.opensanctions.org/licensing/).