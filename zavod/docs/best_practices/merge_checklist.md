# Checklist when reviewing a crawler

Some things that are easy to forget but critical for new crawlers

- Metadata
    - required fields for crawlers that are often forgotten:
        - `coverage`
            - `frequency` (normally `daily`, but `weekly` for zyte and gpt crawlers, and `monthly` for company registers)
            - `start` (updated to the current day when releasing)
        - `load_db_uri: ${OPENSANCTIONS_DATABASE_URI}` for everything except company registers
        - `assertions` - see [Data Assertions](../metadata.md#data-assertions)
    - dataset `name` is clear and conforms to convention. It's fine if it's just the yaml file name and not included in the file.
    - dataset `title` is concise and meaningful
    - dataset `prefix` is short yet meaningful
    - `publisher` - see if there's another dataset with the same publisher that you can copy from. Description is important.
        - country code is correct
        - description is often skipped, but it's important
    - `ci_test` - default `true` should not be overridden to `false` unless the crawler requires some secret key or runs for more than 2 minutes.
- Crawler
    - It emits as many entities as are available at the source
    - When first added, it doesn't emit warnings or errors (except transient, e.g. network timeout/server error that can be expected to go away tomorrow)
        - **note:** code designed to warn when there are issues with the data if something changes later is very welcome!
    - all IDs are created via `Context.make_slug` or `Context.make_id` (it enforces validity)
      - QIDs validated by is_qid are an exception
    - all Persons have a `name` property. Add first and last name via `helpers.apply_name`.
