# OpenSanctions.org

The codebase for OpenSanctions, an open-source repository of sanctions data, politically exposed persons, and other entities of interest.

OpenSanctions uses [Follow the Money](https://docs.alephdata.org/developers/followthemoney), a JSON-based anti-corruption data model, as a common target for all crawlers. Additonal exports into CSV and JSON formats are planned.

* [opensanctions.org](https://opensanctions.org)
    * [Datasets](https://opensanctions.org/datasets/)
    * [About the project](https://opensanctions.org/docs/about/)
    * [FAQ](https://opensanctions.org/docs/faq/)
* [Technical documentation](https://docs.opensanctions.org/en/latest/)
    * [Installation](https://docs.opensanctions.org/en/latest/install.html)
    * [Adding crawlers](https://opensanctions.org/docs/contribute/)
* [Data sources roadmap](https://bit.ly/osa-sources)
* [Contact us](https://opensanctions.org/contact/)

### Technical overview

Repository layout:

* ``opensanctions/``: Python project with data extraction and cleaning components
* ``docs/``: Sphinx [technical documentation](https://opensanctions.org/docs/faq/)

Daily data extraction and processing runs on GitHub Actions. Status:

[![production](https://github.com/pudo/opensanctions/actions/workflows/production.yml/badge.svg)](https://github.com/pudo/opensanctions/actions/workflows/production.yml)

**Licensing:** code is MIT, content & data is CC BY 4.0.