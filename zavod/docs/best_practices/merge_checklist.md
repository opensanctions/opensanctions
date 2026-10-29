# Checklist when reviewing a crawler

Some things that are easy to forget but critical for new crawlers. Scope and per-attribute completeness are governed by the [data priorities guide](priorities.md), and code conduct by the [patterns](patterns.md) — check the crawler against both before merging.

- Metadata
    - required fields for crawlers that are often forgotten:
        - `coverage`
            - `frequency` (normally `daily`, but `weekly` for gpt crawlers, and `monthly` for company registers)
            - `start` - set when the dataset is released, not at merge time. Once the dataset has passed QA and initial de-duplication, it can be [released](release.md).
        - `load_statements` - see [metadata](../metadata.md)
        - `assertions` - see [Data Assertions](../metadata.md#data-assertions)
    - dataset `name` is clear and conforms to convention. It's fine if it's just the yaml file name and not included in the file.
    - dataset `title` is concise and meaningful
    - `summary` and `description` follow [Dataset metadata](../metadata.md)
    - dataset `prefix` is short yet meaningful
    - `publisher` - see if there's another dataset with the same publisher that you can copy from.
        - country code is correct
        - description is often skipped, but it's important
    - `ci_test` - see [Continuous Integration](../metadata.md#continuous-integration)
- Crawler
    - It follows [strict interpretation](strict_interpretation.md)
    - It emits as many entities as are available at the source
    - Entities carry the identifying attributes the source offers, and nothing is marked active or risky beyond what the source says
    - Its effort is proportionate: no large amounts of code spent on details that don't matter to users, such as elaborate title cleaning (see [priorities](priorities.md))
    - It matches crawlers for similar sources, for example in its `list.*` tag and topics
    - Topics follow [topics](topics.md)
    - When first added, it doesn't emit warnings or errors (except transient, e.g. network timeout/server error that can be expected to go away tomorrow)
        - **note:** code designed to warn when there are issues with the data if something changes later is very welcome!
    - all IDs are created via `Context.make_slug` or `Context.make_id` (it enforces validity) and follow the [entity ID guide](entity_id.md)
      - QIDs validated by is_qid are an exception
    - all Persons have a `name` property. Add first and last name via `helpers.apply_name`.
    - each entity is emitted after the last helper that writes to it, such as `h.make_sanction` (see [patterns](patterns.md#emit-an-entity-after-everything-that-writes-to-it))
