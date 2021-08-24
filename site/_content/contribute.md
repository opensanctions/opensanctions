---
title: Contribute a data source
summary: >
    Our goal is to grow the range of sources while ensuring a high-quality data product. We're keen to see contributions of new data sources from the community. 
---

At the core of this project is a **fleet of crawler scripts** that fetch data, clean
and format it as a data source to OpenSanctions. As an open source effort, we're keen
to see anyone contribute new crawlers that help us extend the project's geographic or
topical coverage.

> If you would like to see a particular data source added but don't have the time or
> technical knowledge to do so, please consider [becoming a sponsor](/sponsor/). We will
> prioritize data sources suggested by our sponsors.

## How to contribute

Contributing a crawler is easy for anyone familiar with the basics of **web scraping in Python 3**. While OpenSanctions uses an advanced data pipeline for quality assurance, data integration and storage, the crawler API is simple and well-documented.

1. If you don't have a specific dataset in mind yet, check out the [OpenSanctions source survey](https://bit.ly/osa-sources) to find a list of candidate sources.
2. File a [GitHub issue](https://github.com/pudo/opensanctions/issues) to share your plan to contribute a data source. This grants the developer team an opportunity to give feedback regarding the [relevance](#criteria) of the suggested dataset.
3. Consult the [technical documentation](https://docs.opensanctions.org/en/latest/index.html) to install the codebase on your machine and learn how to write a crawler using the project framework.
4. Before submitting the crawler, please make sure that you have [provided descriptive metadata](https://docs.opensanctions.org/en/latest/crawlers.html#source-metadata) and added the source to any appropriate collections.
5. When your crawler is running and producing good data, submit a pull request to the [opensanctions repository](https://github.com/pudo/opensanctions).
6. Check the [dataset page](/datasets/) on OpenSanctions for your crawler regularly to see if it is producing any errors or warnings that require maintenance. You can also [inspect the nightly console output](https://github.com/pudo/opensanctions/actions/workflows/production.yml) on GitHub Actions to supervise your crawlers operation.

Thanks for getting involved!

## <a id="criteria"></a> Inclusion criteria

Adding a data source to OpenSanctions requires us to engage with this data in perpetuity. We want to prioritise data quality over quantity. The following criteria guide our interest in including a new data source in OpenSanctions:

1. **Public interest.** We want to collect data about entities that are of public interest, either because they bear significant political or economic influence, or are or have been involved in criminal activities. We're particularly interested in:
    * Economic, military and other sanctions targets from any country.
    * Companies or individuals that have been excluded/debarred from procurement.
    * Companies that are subject to exports controls and participate in arms trading.
    * Published data on wanted criminal suspects or convicted criminals. Particular focus on financial crime, money laundering, bribery and other forms of corruption.
    * People who are in senior functions in government or public administration (PEPs)
    * Directors and administrators of state-owned enterprises.
    * Systematic efforts to map out the relatives, close associates and corporate holdings of any of the above.
2. **Justification and context.** Data sources that reference a legal basis for inclusion for each subject and provide links to associated entities provide additional value.
3. **Detailed data.** A data source which merely lists `John Smith` as a person of interest does not provide enough detail to identify that individual. For people, the date of birth, nationality and some form of national identification are ideal. For other entities, the date of incorporation, registration number and jurisdiction are especially important.
4. **Legality.** We do our best to comply with the intellectual property rights of other database authors. Information published by governments and public institutions, however, is considered fair game.
