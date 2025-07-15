# Zavod Extraction UI

This project allows users to see automated data extraction results, fix any errors in the automated extraction, and mark completed extraction results as accepted.

## Getting Started

For local development, you'll want to connect to your local etl db using `ZAVOD_DATABASE_URI` and disable authentication using `ZAVOD_IAP_DISABLED`

```bash
npm run dev
```

Open [http://localhost:3000](http://localhost:3000) with your browser to see the result.


## Configuration

- `ZAVOD_DATABASE_URI` - (e.g. `postgresql://user:pw@host.com/db`) - only postgres supported.
- `ZAVOD_IAP_AUDIENCE` - https://cloud.google.com/iap/docs/signed-headers-howto#iap_validate_jwt-nodejs
- `ZAVOD_IAP_AUTH_DISABLED` (default `false`; e.g. `true`) - skip IAP-based authentication **DON'T ENABLE WHEN PUBLICLY ACCESSIBLE**
