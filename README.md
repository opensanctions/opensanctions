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
    * [FAQs](https://www.opensanctions.org/faq/)
    * [Licensing](https://www.opensanctions.org/licensing/)
* [Contributing](https://www.opensanctions.org/docs/opensource/contributing/)
* [zavod Documentation](https://zavod.opensanctions.org/)
* [API Documentation](https://api.opensanctions.org/)
* [Contact](https://www.opensanctions.org/contact/)

# Running

The easiest way to run zavod is to use Docker. [See the documentation](https://zavod.opensanctions.org/install/#using-docker).

# Developing

If you want to make changes to the code or debug an issues, you'll want to set up a development environment. Read more about how to install zavod in a local Python virtual environment in [the documentation](https://zavod.opensanctions.org/install/#python-virtual-environment)


## Associated Repositories

- [opensanctions/nomenklatura](https://github.com/opensanctions/nomenklatura): building on top of FollowTheMoney, `nomenklatura` provides a framework for storing data statements with full data lineage, and for integrating entity data from multiple sources. It also handles the data enrichment function that links OpenSanctions to external databases like OpenCorporates.
- [opensanctions/yente](https://github.com/opensanctions/yente): API for entity matching and searching.

## Licensing

The code within this repository is licensed under the MIT License. For content and data, we adhere to [CC 4.0 Attribution-NonCommercial](https://www.opensanctions.org/licensing/).
