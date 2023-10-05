# OpenSanctions Codebase

OpenSanctions aggregates and provides a comprehensive open-source database of sanctions data, politically exposed persons, and related entities. Key functionalities in this codebase include:
- **Parsing** of raw source data.
- **Cleaning** and standardization of data structures.
- **Deduplication** to maintain data integrity.
- **Database Creation** for a unified repository of records.

We adopt the [Follow the Money](https://www.followthemoney.tech) framework, a JSON-focused anti-corruption data model, as the schema for all our crawlers. Feature enhancements for exporting data in CSV and JSON formats are underway.

## Quick Links
* [OpenSanctions Main Site](https://www.opensanctions.org/)
    * [Datasets](https://www.opensanctions.org/datasets/)
    * [FAQs](https://www.opensanctions.org/docs/faq/)
    * [API Documentation](https://www.opensanctions.org/api/)
* [Zavod Software Documentation](https://zavod.opensanctions.org/)
* [Data Sources Roadmap](https://bit.ly/osa-sources) - See our current plans!
* [Data Licensing Details](https://www.opensanctions.org/licensing/)
* [Reach Out to Us](https://www.opensanctions.org/contact/)

## Collaborate with Us in Development

### Introduction

At the heart of our project is a crawler dubbed **Zavod**. Should you need to set up a database, we're here to guide you every step of the way. To activate the project, you have the option to employ either `docker-compose.yml` or the `Makefile`.

For an enriched experience backed by extensive documentation, we recommend opting for the `Makefile`. More details can be found on the [Zavod](https://zavod.opensanctions.org/) website.

### Environment Setup

1. **Database Initialization**:
   Launch a terminal and set up your database with:
   ```bash
   docker compose up db
   ```

2. **Project Building**:
   In a separate terminal, commence the build process with:
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

- [opensanctions/site](https://github.com/opensanctions/site): The official website for OpenSanctions. Contains TypeScript React components for visualizing FtM data.
- [opensanctions/yente](https://github.com/opensanctions/yente): API for entity matching and searching.

## Licensing

The code within this repository is licensed under MIT. For content and data, we adhere to [CC 4.0 Attribution-NonCommercial](https://www.opensanctions.org/licensing/).