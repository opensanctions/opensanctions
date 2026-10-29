# Checklist when reviewing a crawler

Some things that are easy to forget but critical for new crawlers. Scope and per-attribute completeness are governed by the [data priorities guide](priorities.md), and code conduct by the [patterns](patterns.md) — check the crawler against both before merging.

- Metadata
    - All metadata meets [Dataset metadata](../metadata.md), and every claim in it is backed by the source or otherwise justified
- Crawler
    - It follows [strict interpretation](strict_interpretation.md)
    - It reads the best data path the source offers (see [source recon](source_recon.md))
    - It emits as many entities as are available at the source, including past terms or listings the source publishes
    - Entities carry the identifying attributes the source offers, and nothing is marked active or risky beyond what the source says
    - Its effort is proportionate: no large amounts of code spent on details that don't matter to users, such as elaborate title cleaning (see [priorities](priorities.md))
    - It matches crawlers for similar sources, for example in its `list.*` tag and topics
    - Topics follow [topics](topics.md)
    - A full live run leaves `issues.log` empty
        - **note:** code designed to warn when there are issues with the data if something changes later is very welcome!
    - all IDs are created via `Context.make_slug` or `Context.make_id` (it enforces validity) and follow the [entity ID guide](entity_id.md), keyed on raw source values
      - QIDs validated by is_qid are an exception
    - all Persons have a `name` property. Add first and last name via `helpers.apply_name`.
    - each entity is emitted after the last helper that writes to it, such as `h.make_sanction` (see [patterns](patterns.md#emit-an-entity-after-everything-that-writes-to-it))
