# OpenSanctions.org

This repository contains the home page for OpenSanctions.org, an open-source
repository of sanctions data, politically exposed persons, and other entities
of interest.

The scrapers are executed using [memorious](https://github.com/alephdata/memorious),
a scraping framework.

OpenSanctions.org is generated using Jekyll, and hosted on GitHub pages. Please
feel free to submit pull requests for any suggested improvements.

## Data sources

We collect data sources for this project [here](https://docs.google.com/spreadsheets/d/1ozEF7aTRM5mOK7PfusKnlGnfoYdee5AygdKbMTHkRQE/edit#gid=0). Please feel free to contribute and to add possible sources or scrapers.

## Running

1. Bring up the services:

        docker-compose up

2. Open a shell in the worker container:

        docker-compose exec worker sh

3. Run a crawler:

        memorious run un_sc_sanctions

4. Export to CSVs:

        ftm store iterate -d un_sc_sanctions | ftm export-csv


The Memorious should be available at localhost:8000. pgweb interface for the database should be available at localhost:9633.

## Pushing data into Aleph

To push crawled entities to Aleph, add `ALEPHCLIENT_HOST` and `ALEPHCLIENT_API_KEY` as environment variables to the worker container and rerun the crawlers. Scraped entities are pushed to Aleph after a crawler is finished running.

You can also push crawled entities to Aleph manually.

1. Open a shell in worker container:

       docker-compose exec worker sh

2. Iterate over scraped entities and send them to Aleph

       ftm store iterate -d un_sc_sanctions | alephclient write-entities -f un_sc_sanctions
