# Entity ID generation

An entity's ID controls which records merge into one entity within a dataset and across the catalog, and must stay stable after publication. A dataset is published once it is added to a collection in `datasets/_collections/` (see [releasing a dataset](release.md)); before that point, ID schemes can change freely.

## What an entity ID is for

Two records emitted with the same ID combine their properties into a single entity. Records with different IDs produce distinct entities. This applies within a single dataset and across datasets in the catalog, where canonical IDs match equivalent entities across sources.

A changing ID is a breaking change. Downstream alerting, screening, and audit systems track entities by their canonical ID. An ID that mutates between runs makes the old entity disappear and a new one appear, breaking matched alerts and audit trails.

## Pick the right fields for the ID

**Use the fewest source fields that disambiguate, prioritizing fields that are mandatory and stable.** A natural key from the source (tax number, registration number) is best. Failing that, combine stable identity attributes (full name, jurisdiction). Avoid free-text descriptions and any field the source can drop.

**Pass raw source values.** No trimming, no case normalization, no parsing, no substitution. If the source revises its date format or country codes, that is a `rekey` event that can be explained to consumers. Internal cleaning changes are not, and they would silently mutate every previously-computed ID.

The choice has to satisfy three tensions:

- **Disambiguation** — too few fields, and two real-world entities collide on one ID.
- **Stability** — fields the source revises (display names, optional fields that appear later, programmatically-assigned row indices) mutate the ID across runs.
- **Completeness** — every record must produce a non-None ID. A sometimes-missing field breaks every previously-computed ID for the records where it later starts appearing.

## `make_id` vs `make_slug`

Default to [`context.make_id`][zavod.context.Context.make_id]. It hashes its inputs to a fixed-length opaque identifier and is stable as long as the inputs are.

Use [`context.make_slug`][zavod.context.Context.make_slug] only when the source publishes a single, clean, authority-defined identifier. Two valid forms:

- **Source-ID slug**, using the dataset prefix: `context.make_slug(profile_id)`. Use when the source's own IDs are well-managed and stable, and the slug is meaningful only inside this dataset.
- **Global-namespace slug**, using a custom prefix: `context.make_slug(lei, prefix="lei")` produces `lei-529900T8BM49AURSDO55`, which the same legal entity carries across every dataset that emits it. Use only when the source is, or strictly follows, a widely-recognized issuing authority for that identifier. A dataset can use its native namespace when it mints the identifiers. For example, `ru_egrul` legitimately owns the `ru-ogrn-` prefix because EGRUL is where OGRNs are minted.

**Never use `make_slug` with:**

- Personal information (names, dates of birth, ID numbers). Slugs are human-readable and surface in URLs, exports, and the resolver UI.
- More than one source field. Multi-part slugs compound every risk below.
- Free text or any value that needs cleaning to be readable.
- Values long enough to risk exceeding the entity ID length limit.

Three risks back the rule: PII exposure (slugs display the value), unstable slugification (the `slugify` rules can change, mutating every ID built from non-trivial text), and length explosion (slugs of free text overflow the ID length limit). `make_id` avoids all three because it hashes its inputs to a fixed-length opaque value.

**Narrow exception to the multi-part rule.** When a source reuses the same numeric ID across different entity types (Person #5 and Company #5 are distinct entities), prefix the slug with a literal type discriminator: `context.make_slug("person", source_id)` and `context.make_slug("company", source_id)`. The discriminator is a fixed string chosen by the crawler, not a source field, so it does not change across runs.

## Sub-entity IDs are computed by helpers

Helpers like [`h.make_sanction`][zavod.helpers.make_sanction], [`h.make_address`][zavod.helpers.make_address], and [`h.make_position`][zavod.helpers.make_position] build their own IDs from the linked entity's ID. Do not set `.id` on entities produced by these helpers.

Pass `key=` when a single entity has more than one of the same sub-entity (multiple sanctions across years, multiple addresses, multiple terms in office). Without it, the helper computes the same ID twice and silently merges the sub-entities.

```python
sanction_2023 = h.make_sanction(context, person, key="2023-listing")
sanction_2024 = h.make_sanction(context, person, key="2024-listing")
```

Pick a key that is unique within the entity and stable across runs: a listing date, a program code, a term number from the source. The "raw source value, no parsing" rule applies to `key=` as it does to `make_id`.

See the [helpers reference](../helpers.md) for which helpers accept `key=` and how each composes its ID.

## Changing an ID with `context.rekey`

When an ID must change, use [`context.rekey`][zavod.context.Context.rekey] to record the equivalence in the resolver. Common causes: the source revised its identifier format, a previous crawler version emitted a bad scheme, or a schema change forced the change. Downstream watchlists keep matching the canonical entity through the new ID.

```python
new_id = context.make_id(record["tax_id"])
old_id = context.make_id(record["name"], record["dob"])
context.rekey(old_id, new_id)
entity = context.make("Person")
entity.id = new_id
```

`context.rekey` is a one-shot migration, not persistent crawler state:

1. Dedupe the dataset against external sources (e.g. Wikidata) so the rekey does not drag stale matches forward.
2. Merge the PR that adds the `rekey` call alongside the new ID scheme.
3. Production runs the crawler once.
4. Follow-up PR removes the `rekey` call. The resolver entry persists; the crawler does not carry the call forever.

Every accumulated `rekey` entry stays in the resolver, so leaving calls in place is a long-term liability. Rollback is hacky (delete entries with the `zavod/rekey` user ID for a time window) and is not part of normal operation.

`context.rekey` is a no-op on the SQLite resolver used in local development; the call is logged and skipped.

## Stability matters most for matchable schemata

`Person`, `Company`, `Vessel`, `Organization`, and the other matchable schemata are referenced by downstream alerting, screening, and audit systems. Avoid changing their ID schemes. When a change is unavoidable, coordinate with the team and use `context.rekey` to preserve the resolver mapping.

Relationship entities (`Ownership`, `Family`, `Directorship`, `Occupancy`, ...) and secondary entities (`Sanction`, `Address`, `Passport`, ...) carry no stable identity in the wider ecosystem; they are reached through the matchable entity they attach to. Their IDs can be changed freely without team coordination.
