# Working with addresses

Addresses are composed from structured fields with [`h.make_address`][zavod.helpers.make_address] and inlined onto the main entity with [`h.copy_address`][zavod.helpers.copy_address] as a single `address` string plus a `country` code.

For *when* to emit an address (especially for individuals), see the [data priorities guide](priorities.md).

## Addresses live on the main entity, not as separate entities

Matchers score candidates on the `address` and `country` properties of the matchable entity (`Person`, `Company`, ...) directly. Reaching values via a linked `addressEntity` requires graph traversal, which is costly in performance-sensitive matching. Emitting a separate `Address` entity adds storage and indexing overhead without contributing to matching, so the current default is to compose the address with `make_address`, then adopt its `full` and `country` statements onto the main entity with `copy_address`. The intermediate `Address` is discarded.

If address entities are reintroduced for matching, switching the call site from `copy_address` to `apply_address` re-emits the `Address` entity and re-links it through `addressEntity`. The composition step (`make_address`) doesn't change.

## The default pattern

```python
address = h.make_address(
    context,
    street=row.pop("street"),
    city=row.pop("city"),
    postal_code=row.pop("postcode"),
    country=row.pop("country"),
)
h.copy_address(entity, address)
```

What happens:

- `make_address` composes the structured parts (`street`, `city`, `postal_code`, `state`, `region`, `summary`, `po_box`) into a single `full` line using country-aware templating from [`rigour.addresses`](https://rigour.followthemoney.tech/), parses the country, and returns an `Address` entity. It doesn't emit the entity.
- `copy_address` adopts the `full` and `country` statements onto the main entity as `address` and `country`. The structured parts reach the main entity through the composed `full` text, not as separate properties.
- If `make_address` returns `None` (no usable input), `copy_address` is a safe no-op.

When the address text is in a language different from the dataset's `data.lang`, pass `lang=` to `make_address`. The tag propagates through `copy_address` to the main entity's `address` statement.

!!! warning "Mixing `full=` with parts drops the parts"

    If you pass both `full=` and individual fields like `city=` or `street=`, only `full` and `country` reach the main entity. The part fields are set on the `Address` entity that `copy_address` then discards. Pass either `full=` (when the source publishes the address as one string) or the parts (so `make_address` composes them), not both.

## Don't pre-format the address in the crawler

`make_address` knows the country-specific templating rules and assembles the `full` line consistently across crawlers. Pre-formatting in the crawler duplicates work that lives in `rigour.addresses` and produces address strings that vary by dataset. Pass the parts, and let `make_address` do the composition.

When the source publishes the address as a single string with no separate parts, pass it as `full=` and skip the structured fields.

## Country handling

`make_address` accepts the country in whichever form the source provides:

- When the source provides an ISO 2-letter code, pass `country_code=`.
- When the source provides a country name, pass `country=`. The helper parses it through the country lookup.
- When the source provides both, pass both. The helper logs a warning if they disagree.

When a source field labeled "country" sometimes contains a code (`"DE"`) and sometimes a name (`"Germany"`), `make_address` still resolves the country, but logs a warning the first time it sees a code-shaped value in the name field. Investigate persistent warnings and split the inputs in the crawler.

When neither field is set, `make_address` falls back to parsing the country out of `full=` via the country lookup.

When a source provides only a country and no address text, prefer `entity.add("country", value)` directly. The country cleaner runs through `entity.add` too, so routing through `make_address` adds no normalization. Reach for `make_address` here only when the same call site has to handle country-only rows alongside rows with structured address parts.

## Multiple addresses per entity

When the source provides several addresses for one entity (registered, mailing, operational), pass `key=` to `make_address` to disambiguate the `Address` IDs:

```python
for location in record["locations"]:
    address = h.make_address(
        context,
        full=location["full"],
        country=location["country"],
        key=location["id"],
    )
    h.copy_address(entity, address)
```

`copy_address` writes statements, so the same `address` value appears once on the main entity regardless of how many addresses were composed for it. Two source addresses that produce identical text after formatting collapse to one.

See the [entity ID guide](entity_id.md) for the rules that apply to `key=`.

## When a single text line is enough

[`h.format_address`][zavod.helpers.format_address] returns the same country-aware composed string as `make_address` but without constructing an `Address` entity. Use it when the address goes straight to the main entity's `address` property and there's no intermediate entity to build:

```python
address = h.format_address(
    city=row.pop("city"),
    state=row.pop("state"),
    country_code="us",
)
entity.add("address", address)
```

Prefer the `make_address` + `copy_address` pattern as the default. It handles the `None`-return and country normalization, and the call site can later switch to `apply_address` without rewriting the composition. Reach for `format_address` only when the composition is all you need.

## When PO Box arrives in the postcode field

Some sources stuff a PO box reference into the postcode column (`"PO Box 12345"`). [`h.postcode_pobox`][zavod.helpers.postcode_pobox] splits the input into a `(postcode, po_box)` tuple where exactly one side is populated:

```python
postcode, po_box = h.postcode_pobox(row.pop("postal_code"))
address = h.make_address(
    context,
    street=row.pop("street"),
    postal_code=postcode,
    po_box=po_box,
    country=row.pop("country"),
)
```

## Cleaning placeholder values with `type.address`

Source data often carries placeholders for unknown addresses (`"Unknown"`, `"XX"`, `"-"`, `"N/A"`). These propagate through `make_address` and reach the main entity. Drop them with a `type.address` lookup in the dataset YAML:

```yaml
lookups:
  type.address:
    lowercase: true
    options:
      - match: ["unknown", "xx", "-", "n/a"]
        value: null
```

The lookup applies whether the value reaches the entity via `copy_address` or directly via `entity.add("address", ...)`.

## When `apply_address` is the right call

[`h.apply_address`][zavod.helpers.apply_address] emits the `Address` as a separate entity, sets the `addressEntity` link on the main entity, and additionally copies the `address` text and `country` onto it. It's the legacy path the codebase is moving away from.

Use `apply_address` only when:

- The source provides delivery `remarks` that should be preserved. `remarks` is the one field `make_address` doesn't compose into `full`, so it only survives on the emitted `Address` entity.
- The `Address` entity itself needs to appear in the output, for example in test fixtures or if address entities are reintroduced for matching.

New crawlers should default to `copy_address`. Existing `apply_address` callers should be actively reviewed for switching to `copy_address` when those crawlers are next touched.
