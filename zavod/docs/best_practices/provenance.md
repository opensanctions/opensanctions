# Data provenance/lineage

## origin

We indicate the provenance of a value using the `origin` argument to `Context.emit()` and the helpers that accept this argument. This data is available to end-users in the [statement-based data](https://www.opensanctions.org/docs/statements/).

A sensible default is set automatically in some specific cases, e.g. for topics, type-based lookups, and data filled in from the dataset metadata. See `zavod/constants.py`.

A crawler could provide an `origin` value in cases where it could be meaningful:

- The data comes from a file with a meaninful name, e.g `SAM_Exclusions_Public_Extract_V2_26075.CSV`
- The data comes from an API endpoint with data specific to the entity, e.g. `https://cro.justice.cz/verejnost/api/funkcionari/1234`

## sourceUrl

When the data is sourced from a page about the person (a "deep link" to a specific person's profile), that URL is added to the `sourceUrl` property of the entity.
