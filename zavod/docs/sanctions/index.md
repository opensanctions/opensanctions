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

## Programs and measures

See [programs](programs.md) for linking a `Sanction` to a program.

Map the source's measure labels to `Sanction:provisions` through a `sanction.measure`
lookup whose values come from the `Measure` vocabulary in `zavod/stateful/programs.py`.

## Dates

- `listingDate` is when the designation was published.
- `startDate` is when it took effect.
- `endDate` is when it expires, which is usually in the future.

Fill both `listingDate` and `startDate` where the source gives them. Don't set
`createdAt`.

When the source lists a designation whose `endDate` has passed, emit it, but add
`topics: sanction` only if [`h.is_active`][zavod.helpers.is_active] is true for its
`Sanction`.

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
