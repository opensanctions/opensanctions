This project contains crawlers that import source data, such as sanctions lists and other KYC/AML screening data, into the FollowTheMoney entities. It puts an emphasis on data cleaning. Much of the input data is semi-structured information published by government bodies - often rife with inconsistencies, manual data entry errors, etc. Our goal is to bring strict interpretation to these source datasets.

### Repo layout

* `zavod` contains an ETL framework for crawlers, including definitions for metadata (`zavod.meta`), entity structure (`zavod.entity.Entity`) and crawler context (`zavod.context.Context`).
    * Documentation for the entity structure (available schemata and properties in `followthemoney`) is available here: https://followthemoney.tech/explorer/schemata/ (sub paths eg. https://followthemoney.tech/explorer/schemata/Person/). Property types are documented here: https://followthemoney.tech/explorer/types/ (and eg. https://followthemoney.tech/explorer/types/name/).
    * Data cleaning functions from `rigour` are documented at: https://rigour.followthemoney.tech/
    * Write code for all zavod functions in `zavod/zavod/tests`.
    * Run tests using `cd zavod && pytest zavod/tests/`.
    * Run `cd zavod && mypy --strict --exclude zavod/tests zavod/` after each change to zavod.
* `datasets` contains crawlers. Each crawler is defined using a `.yml` file (eg. `datasets/us/ofac/us_ofac_sdn.yml`), and a code file (often `crawler.py`, but defined using the `entry_point` key of the dataset `.yml`). The dataset has a `name`, which is based on the `.yml` file name stem (e.g. `us_ofac_sdn`).
    * To run a crawler: `zavod crawl <file_path>` in the project root. Running crawl several times might re-use the same data fetched in the initial run (`context.fetch_resource`).
    * When a crawler encounters uncertainty in any of the data it is parsing, it should crash or produce an error instead of emitting ambiguous data.
    * Crawlers use `lookups` to override specific values for entity properties of a particular type. For ambiguous data, individual cases can be clarified by adding lookups.
    * After running a crawler, output data is written to `data/datasets/<dataset_name>/`. The file `issues.log` contains line-based JSON of any warnings or errors produced by the crawler. Often the source data fetched by `context.fetch_resource` is also available in that folder.
    * Crawlers commonly do `from zavod import helpers as h`. The relevant code is in `zavod/zavod/helpers`. Use this pattern over direct imports.
* `ui` contains a NextJS user interface for reviewing and verifying information from crawlers. The contained table structures need to match those in `zavod.stateful`.
* `zavod/docs` documents the zavod framework, as well as best practices, especially with regards to semantic issues like Politically Exposed Persons

## Navigating the knowledge base

Use search (grep/glob/find) to find the most relevant starting document. Once you have opened a document, follow its cross-reference links to gather related context as needed — the links are curated and encode domain relationships that keyword search misses. Avoid following links more than two hops from your starting document. If something remains unclear, fall back to search or ask.

## Content guidelines

* The documentation for how we want crawlers to be written in `zavod/docs` is authoritative.
* When best practices are identified or changed, it should be documented in `zavod/docs`, and conflicting documentation should be updated.
* Refer to the [documentation styleguide for the broader FollowTheMoney ecosystem](https://github.com/opensanctions/followthemoney/blob/main/docs/styleguide.md).
* Do not assume that existing crawlers are implemented according to current best practices. Only use patterns in existing crawlers to update the documentation when prompted to, and in that case, identify specific patterns and propose specific aspects as separate concepts.
* Claude skills should reference and search relevant documentation in `zavod/docs`.
  * When writing skills, only the skill-specific procedures and examples should be in the skill directory.
  * Any expansion or clarification of how crawlers should be written or how zavod should be used should take place in `zavod/docs`.
* Keep documentation files under 500 lines. Split documentation into more specific files at conceptual/logical boundaries at the latest when approaching this limit.
* The audience of `zavod/docs` is crawler maintainers and zavod maintainers at OpenSanctions as well as non-staff users of the zavod framework.

## Coding hints

* NEVER try to install new packages or on-board new services without being explicitly promoted to do so.
* Assume the venv you're running in has `zavod` configured.
* Write code that is specific (eg `if var is None:`, not `if var:`) and breaks with an error when encountering unexpected conditions. Distrust all input, especially from the source files.
* All zavod code needs to be fully typed, unit tested and thoroughly documented.
* When adding type hints, use the container types such as `set`, `tuple` or `dict` instead of the now-deprecated `typing.Set`, `typing.Tuple` or `typing.Dict`.
* All new crawlers should be written using typed Python. Suggest adding types to existing ones.
* We use `lxml` for parsing HTML/XML, and `context.fetch_` functions the retrieve online data. Other libraries are listed in `zavod/pyproject.toml`. NEVER use beautifulsoup, or the Python stdlib xml modules.
* For crawlers that traverse HTML using XPath expressions, use the sensibly-typed helpers in `zavod.helpers.html` if possible.
