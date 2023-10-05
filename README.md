Certainly! Here's a more streamlined and refined version for contributors:

---

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

## Join Us in Development

### Getting Started

This project uses a crawler named **Zavod**. Sometimes you might need a database we will help you set this up to. To run the project, you can utilize either the `docker-compose.yml` or the `Makefile`.

We recommend using the `Makefile` due to comprehensive documentation available on the [Zavod](https://zavod.opensanctions.org/) site.

### Configuration

Setup in 1 terminal your database:

```bash
docker compose up db
```

Set a other terminal with the build:
```bash
make build
# Alternatively, you can run this directly:
docker-compose build --pull
```

### Execute the Crawler

To crawl the dataset:
```bash
# This targets the dataset in the datasets folder
docker compose run --rm app zavod crawl datasets/de/abgeordnetenwatch/de_abgeordnetenwatch.yml
```

## Associated Repositories

- [opensanctions/site](https://github.com/opensanctions/site): The official website for OpenSanctions. Contains TypeScript React components for visualizing FtM data.
- [opensanctions/yente](https://github.com/opensanctions/yente): API for entity matching and searching.

## Licensing

The code within this repository is licensed under MIT. For content and data, we adhere to [CC 4.0 Attribution-NonCommercial](https://www.opensanctions.org/licensing/).

---

This version offers clarity and structure, making it easier for contributors to navigate and understand.