# Zavod Extraction UI

This project allows users to see automated data extraction results, fix any errors in the automated extraction, and mark completed extraction results as accepted.


## Data model

The data is in the database defined by zavod.stateful.model.review_table.
This is declared for a typescript querybuilder in `lib/db.ts`.

The unit of work is called a "review".


## How it works

1. OpenSanctions Crawlers call `zavod.stateful.extraction.get_accepted_data` to add the results of automated data extraction to the review database, which returns the value if a matching review entry is marked `accepted`.
2. Human reviewers visit the user interface, review any non-accepted reviews


## Getting Started

For local development, start a postgres db, and configure the site for local development. E.g.

Run

```bash
docker compose -f ../docker-compose.yml up -d db # Bring up a dev database
```

Set environment variables, e.g. via .env.local file

```
ZAVOD_DATABASE_URI=postgresql://postgres:password@localhost:5432/dev
ZAVOD_ALLOW_UNAUTHENTICATED=true  # Unsafe if an untrusted network can reach this
NEXT_PUBLIC_BASE_URL=http://localhost:3000
```

Then run

```bash
npm run dev
```

Open [http://localhost:3000](http://localhost:3000) with your browser to see the result.


### Running tests

```bash
npm test
```

## Tech stack

- Next.js for the reviews website
  - Server side pages directly query the database to show data and handle form posts to update data. There's no API.
- SQLAlchemy is used for Python-based crawlers to maintain review entries in the reviews table.


## Configuration

- `NEXT_PUBLIC_BASE_URL` - Base URL used for absolute URLs, e.g. in dev probably `http://localhost:3000`
- `ZAVOD_DATABASE_URI` - (e.g. `postgresql://user:pw@host.com/db`) - only postgres supported.
- `ZAVOD_IAP_AUDIENCE` - https://cloud.google.com/iap/docs/signed-headers-howto#iap_validate_jwt-nodejs
- `ZAVOD_ALLOW_UNAUTHENTICATED` (default `false`; e.g. `true`) - skip IAP-based authentication **DON'T ENABLE WHEN PUBLICLY ACCESSIBLE**
