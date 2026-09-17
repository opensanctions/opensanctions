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

## Review UI

Crawlers that use [Data Reviews](https://zavod.opensanctions.org/data_reviews/) need human review of extracted data. See [`ui/README.md`](ui/README.md) for how to run the review UI locally.


## Associated Repositories

- [opensanctions/nomenklatura](https://github.com/opensanctions/nomenklatura): building on top of FollowTheMoney, `nomenklatura` provides a framework for storing data statements with full data lineage, and for integrating entity data from multiple sources. It also handles the data enrichment function that links OpenSanctions to external databases like OpenCorporates.
- [opensanctions/yente](https://github.com/opensanctions/yente): API for entity matching and searching.

# Contributing & AI tools

Regarding the use of AI tools, while we have no definite policy, we think that contributions should have meaningful human effort, judgement and context. Reviewing PRs is work for maintainers, so please make sure that the human effort you put into a contribution is in a reasonable proportion to the human effort required from maintainers reviewing it.

In other words, if you're just pasting an issue tracker link or a generic prompt into an AI tool, don't do it — we can write LLM prompts or run automated tools ourselves, and that would be faster and more secure. We've recently seen an influx of agents farming our issue tracker and submitting PRs, presumably without any human intervention and only in an effort to make make their GitHub profile look more impressive. In these cases, we will reject the contribution and ban the offending account.

That said, we welcome contributors and collaborating organizations and recognize that PR reviews are a learning opportunity for all participants. Also, if you're using zavod and your contribution makes your life better, that's a great reason for us to spend some time on it. So if you're a human wanting to get involved or scratching an itch, please reach out!

## Licensing

The code within this repository is licensed under the MIT License. Data files produced by OpenSanctions are licensed under [CC BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/), and some files under `datasets/` are unmodified copies of third-party material that we cannot license at all. See [COPYRIGHT.md](COPYRIGHT.md) for which applies to what, and [our licensing page](https://www.opensanctions.org/licensing/) for commercial use.
