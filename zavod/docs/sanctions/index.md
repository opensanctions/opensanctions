# Developing sanctions crawlers

If this is your first crawler, start with a basic one by following the
[tutorial](../tutorial.md), then come back here. Program keys are covered in
[programs](programs.md), and short worked crawlers in [examples](examples.md).

## How a designation is modeled

Each designation is the designated entity plus a `Sanction` built by
[`h.make_sanction`][zavod.helpers.make_sanction]. Add `topics: sanction` to the
designated entity, and emit it after the `Sanction` is made (see
[patterns](../best_practices/patterns.md#emit-an-entity-after-everything-that-writes-to-it)).

An entity designated under several programs gets one `Sanction` per program, told apart
by `key=` (see [sub-entity IDs](../best_practices/entity_id.md#sub-entity-ids-are-computed-by-helpers)).

Assert `Sanction` counts alongside the `Person` and `Organization` counts (see
[data assertions](../metadata.md#data-assertions)).

## Sanction properties

[`h.make_sanction`][zavod.helpers.make_sanction] fills `entity`, `authority`, `country`
and `sourceUrl` from the dataset metadata. The crawler fills the rest from the source:

- `authorityId`: the issuing authority's own reference for the designation.
- `program` and `programId`: see [programs](programs.md).
- `provisions`: the restrictions imposed by the designation.
- `reason`: the source's statement of grounds.

### Dates

The date properties describe the designation as the authority made it, not our
processing of it:

- `listingDate`: when the designation was listed.
- `startDate`: when it took effect.
- `endDate`: when it expires, usually in the future.
- `modifiedAt`: when the authority last amended the designation.

Fill each one the source gives. When the source gives one date and the legal instrument
makes listing and effect fall on that same day, set it as both `listingDate` and
`startDate`. Otherwise set only the property the date actually is. Don't set
`createdAt`, and never use the crawl time for any of these.

Before mapping a source date to `modifiedAt`, check that the authority updates it when
it amends the designation, and only then.

When the source lists a designation whose `endDate` has passed, emit it, and set its
topics as described in [topics](../best_practices/topics.md#risk-topics-while-in-force).

## Choosing the schema

- `Person` for individuals and `Vessel` for ships.
- `LegalEntity` when the source does not say whether the target is a person or an
  organization.
- `Organization` for associations and social constructs, such as Al Qaeda, listed among
  companies. Government departments are `Organization` too; an upcast to `Company` is
  acceptable.

See also [reference entities that fit the property range](../best_practices/patterns.md#reference-entities-that-fit-the-property-range).

## Entity IDs

Use the source's own ID once you have checked that it is reasonably reliable. Otherwise,
pass `context.make_id` a sufficiently unique set of unparsed source values, such as the
name, date of birth and jurisdiction. See [entity IDs](../best_practices/entity_id.md).

## Names

New sanctions crawlers add names through `h.apply_reviewed_names`. Base the
`names.schema_rules` in the dataset YAML on what you find when studying the source's
names. See [names](../extract/names.md).

## De-listings

Drop de-listed entities. Some policies make specific exceptions to this.

## Linked parties

Emit linked parties, such as owners, parent companies or crypto wallets, without
`topics`: the analyzers compute them. Use `UnknownLink` only where the source asserts a
link without saying what kind it is.

## Lists that republish UN or EU designations

Reuse the shared parsers in `zavod.shed`:

- `zavod.shed.un_sc`: `load_un_sc` fetches the UN Security Council list, and
  `get_persons` and `get_legal_entities` filter it by `Regime`, keying entities with
  the `un_sc_sanctions` prefix so they share IDs with that dataset.
  `apply_un_name_list` adds UN-style name parts.
- `zavod.shed.fsf`: `parse_entry` parses an entry of the EU Financial Sanctions Files
  XML, after `h.remove_namespace` on the document.

## Lists without a structured source

See [curating a dataset](../best_practices/curated_datasets.md).
