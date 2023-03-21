# opensanctions data pipeline

The codebase for OpenSanctions, an open-source database of sanctions data, politically exposed persons, and other entities of interest. This repository contains the code used to parse, clean, and deduplicate source data and build the combined database.

OpenSanctions uses [Follow the Money](https://www.followthemoney.tech), a JSON-based anti-corruption data model, as a common target for all crawlers. Additonal exports into CSV and JSON formats are planned.

* [opensanctions.org](https://www.opensanctions.org/)
    * [Datasets](https://www.opensanctions.org/datasets/)
    * [About the project](https://www.opensanctions.org/docs/about/)
    * [FAQ](https://www.opensanctions.org/docs/faq/)
    * [API](https://api.opensanctions.org/)
* [Software documentation](https://www.opensanctions.org/docs/opensource/)
* [Data sources roadmap](https://bit.ly/osa-sources)
* [Data licensing](https://www.opensanctions.org/licensing/)
* [Contact us](https://www.opensanctions.org/contact/)

Related repositories:

* [opensanctions/site](https://github.com/opensanctions/site): web site for the OpenSanctions project, contains TypeScript React components for rendering FtM data.
* [opensanctions/yente](https://github.com/opensanctions/yente): API matching and entity search service. 

**Licensing:** code is MIT-licensed, content & [data is CC 4.0 Attribution-NonCommercial](https://www.opensanctions.org/licensing/).