# opensanctions data pipeline

The codebase for OpenSanctions, an open-source database of sanctions data, politically exposed persons, and other entities of interest. This repository contains the code used to parse, clean, and deduplicate source data and build the combined database.

OpenSanctions uses [Follow the Money](https://followthemoney.readthedocs.io/en/latest/index.html), a JSON-based anti-corruption data model, as a common target for all crawlers. Additonal exports into CSV and JSON formats are planned.

* [opensanctions.org](https://opensanctions.org)
    * [Datasets](https://opensanctions.org/datasets/)
    * [About the project](https://opensanctions.org/docs/about/)
    * [FAQ](https://opensanctions.org/docs/faq/)
    * [API](https://api.opensanctions.org/)
* [Technical documentation](https://docs.opensanctions.org/en/latest/) (readthedocs.org)
* [Data sources roadmap](https://bit.ly/osa-sources)
* [Data licensing](https://opensanctions.org/licensing/)
* [Contact us](https://opensanctions.org/contact/)

### Technical overview

Repository layout:

* ``opensanctions/``: Python project with data extraction and cleaning components
* ``docs/``: Sphinx [technical documentation](https://opensanctions.org/docs/faq/)

Related repositories:

* [opensanctions/site](https://github.com/opensanctions/site): web site for the OpenSanctions project, contains TypeScript React components for rendering FtM data.
* [opensanctions/yente](https://github.com/opensanctions/yente): API matching and entity search service. 

**Licensing:** code is MIT-licensed, content & [data is CC 4.0 Attribution-NonCommercial](https://opensanctions.org/licensing/).