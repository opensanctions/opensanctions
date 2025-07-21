# Zavod Extraction UI

This project allows users to see automated data extraction results, fix any errors in the automated extraction, and mark completed extraction results as accepted.


## Data model

The data is in the database defined by zavod.stateful.model.review_table.

The unit of work is called a "review".


## How it works

1. OpenSanctions Crawlers call `zavod.stateful.extraction.get_accepted_data` to add the results of automated data extraction to the review database, which returns the value if a matching review entry is marked `accepted`.
2. Human reviewers visit the user interface, review any non-accepted reviews


## Getting Started

For local development, you'll want to connect to your local etl db using `ZAVOD_DATABASE_URI` and disable authentication using `ZAVOD_UNSAFE_IAP_DISABLED`

```bash
npm run dev
```

Open [http://localhost:3000](http://localhost:3000) with your browser to see the result.


## Tech stack

- Next.js for the reviews website
  - Server side pages directly query the database to show data and handle form posts to update data. There's no API.
- SQLAlchemy is used for Python-based crawlers to maintain review entries in the reviews table.


## Configuration

- `ZAVOD_DATABASE_URI` - (e.g. `postgresql://user:pw@host.com/db`) - only postgres supported.
- `ZAVOD_IAP_AUDIENCE` - https://cloud.google.com/iap/docs/signed-headers-howto#iap_validate_jwt-nodejs
- `ZAVOD_UNSAFE_IAP_AUTH_DISABLED` (default `false`; e.g. `true`) - skip IAP-based authentication **DON'T ENABLE WHEN PUBLICLY ACCESSIBLE**
