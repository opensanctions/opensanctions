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
