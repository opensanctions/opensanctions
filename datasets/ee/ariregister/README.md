# ee_ariregister

https://avaandmed.ariregister.rik.ee/en/downloading-open-data

**Notice**

The zipped json files are downloaded first and turned into line based json to
be able to stream processing, see `Makefile`

## Todo

- refine data parsing: find at least `dissolutionDate` and make sure relations
  contain all relevant data

Open data questions:

- deduplication of officer entities is not perfect
- there can be multiple Ownership intervals between 1 officer and 1 entity with different dates and role specification:
  - "otsene osalus" (direct holding) -> comes from the bfo data source but without more context data
  - "Osanik" (translated as "associate") but with shareholding/percentage data -> comes from the officers data sources

