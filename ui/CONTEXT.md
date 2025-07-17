# Zavod UI

Zavod UI is a user interface intended for reviewing, and fixing when necessary, the results of automated data extraction.

## Data model

The data is in the database defined by zavod.stateful.model.review_table.

The unit of work is called a "review".


## How it works

1. OpenSanctions Crawlers call `zavod.stateful.extraction.get_accepted_data` to add the results of automated data extraction to the review database, which returns the value if a matching review entry is marked `accepted`.
2. Human reviewers visit the user interface, review any non-accepted reviews

## Tech stack

- Next.js for the reviews website
  - Server side pages directly query the database to show data and handle form posts to update data. There's no API.
- SQLAlchemy is used for Python-based crawlers to maintain review entries in the reviews table.
