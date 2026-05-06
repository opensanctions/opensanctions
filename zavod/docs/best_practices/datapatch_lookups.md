# Datapatch lookups

Lookups patch broken or inconsistent source values into clean ones, declared in the dataset metadata YAML and applied either automatically by `zavod` or explicitly by crawler code.

The mechanism comes from the [datapatch](https://pypi.org/project/datapatch/) library. Use it whenever the same dirty value or class of values reappears across crawls — listing the fixes in YAML keeps the crawler code free of one-off conditionals and gives reviewers a single place to inspect what has been overridden.

!!! info "Please note"
    Avoid using lookups to express something not evident in the data. For example, do not make a date more precise than it is in the data, even if you know your version to be true.


## Two ways lookups get invoked

Lookups appear under the `lookups:` key in the dataset YAML. Each named lookup is invoked in one of two ways:

- **Type lookups** — any lookup named `type.<typename>` (e.g. `type.country`, `type.email`, `type.identifier`) is invoked automatically by `zavod` whenever a property of that FollowTheMoney type is added to an entity. Crawler code does not need to call it.
- **Named lookups** — every other lookup is invoked explicitly from the crawler, via [`context.lookup_value`][zavod.context.Context.lookup_value] (returns the result's `value`) or [`context.lookup`][zavod.context.Context.lookup] (returns the full `Result` object with all attributes).

```python
# Type lookup — runs implicitly:
entity.add("email", row.pop("email"))

# Named lookup — invoked explicitly:
res = context.lookup("relationships", row.pop("link_type"))
```


## Matching: match, contains, regex

Each option in a lookup uses one of three matching modes:

- **`match`** — exact string equality, after normalization. A list of strings matches any of them. Use the list form to merge multiple inputs that should produce the same result under a single option (see [Result values](#result-values)).
- **`contains`** — substring match, after normalization.
- **`regex`** — raw Python regular expression. The input is **not** normalized before the regex runs, so write the pattern against the original string.

A single option may combine modes; if any clause matches, the option matches.

```yaml
lookups:
  type.country:
    lowercase: true
    options:
      - match: Tazmania
        value: Australia
      - contains: Syrian Arab Republic
        value: Syria
      - regex: "^USSR.*"
        value: SUHH
```

### Normalization

Three flags control how the input value and the `match`/`contains` patterns are folded before comparison. They can be set on the lookup as defaults and overridden per option:

| Flag | Default | Effect |
|---|---|---|
| `normalize` | `false` | Strip diacritics and collapse whitespace. |
| `lowercase` | `false` | Lowercase before matching. |
| `asciify` | `true` | When normalizing, transliterate non-ASCII to ASCII (Путин → Putin). Set `false` to keep non-Latin scripts intact. |

### Matching `null` inputs

To match a missing value, list `null` in the option's `match`:

```yaml
- match:
    - null
    - "Unknown"
  value: null
```

### Disambiguating overlapping options

When two options match the same input with the same priority, datapatch raises `LookupException`. Use `weight: <int>` on one option to break the tie — higher weight wins.

```yaml
- contains: Bank
  value: Generic Bank
- match: Sberbank
  weight: 10
  value: Sberbank of Russia
```


## Result values

Every option produces a result. The simplest result is a single replacement string:

```yaml
- match: tcolpetzer@mcdonoughga.org.
  value: tcolpetzer@mcdonoughga.org
```

A few rules about result values:

- **`value: X` is shorthand for `values: [X]`.** The two are interchangeable; `values:` exists for the multi-value case.
- **`value: null` drops the input.** No property is added to the entity.
- **Multiple values fan out into multiple property values:**

  ```yaml
  - match: district@repkelly.com, mike@repkelly.com
    values:
      - district@repkelly.com
      - mike@repkelly.com
  ```

- **Consolidate inputs that share a result.** When several distinct inputs should produce the same `value` (or `values`, or `value: null`), list them under one option's `match:`. Inputs that map to *different* results must remain in separate options.

  ```yaml
  # One option, three inputs that all drop:
  - match:
      - 307j@att
      - SL Jones@ballhealth.com
      - na
    value: null

  # Two options — different replacements, cannot be merged:
  - match: tcolpetzer@mcdonoughga.org.
    value: tcolpetzer@mcdonoughga.org
  - match: sensan buenaventura@capitol.hawaii.gov
    value: sensanbuenaventura@capitol.hawaii.gov
  ```

- **Arbitrary keys on the option are accessible as attributes on the result.** Named lookups use this to attach schema, role, or category information; the next two sections show how.


## Re-routing to a different property

In a `type.*` lookup, `prop:` moves the value to a different property of the same entity. This handles cases where source data labels a value as one thing but it is really another — for example an "email" column that occasionally contains a website URL.

When `values` is omitted, the original input value is preserved and only the destination property changes. To re-route **and** rewrite, set both:

```yaml
type.email:
  options:
    # Pure re-route — original URL preserved, moved to the website property
    - match: www.bloodandhonour.co.uk
      prop: website
    # Re-route with rewrite — fix the typo as well
    - match: www.surena gc.com
      prop: website
      value: www.surenagc.com
```

If the target property does not exist on the entity's schema, `zavod` logs `Invalid type lookup property re-write` and falls back to the original property.

!!! note "Same-type re-routes are the safe default"
    Re-routing keeps the value's `cleaned` flag from the original type's processing, so the value is not re-validated against the destination property's type. Re-routes within the same FtM type (e.g. `identifier` → `identifier`) are uncontroversial. Cross-type re-routes (e.g. `email` → `website`) work but the destination type's validator does not run on the value — only use them when the value is already known to be clean for the destination type.

### Curated values bypass smell checks

Any value produced by a `type.*` lookup is treated as manually reviewed and bypasses three downstream warnings:

- `Property value '<value>' is not a valid name.` (from `rigour.names.is_name`)
- `Property for <prop> looks too short for an address: <value>` (≤ 3 characters)
- `HTML/XSS suspicion in property value: <value>`

This is useful for short place names like `Zug` or for legitimate names that fail `is_name`'s heuristics — adding an identity lookup (`match: Zug` / `value: Zug`) marks the value as curated and silences the warning.


## Mapping to richer concepts

Named lookups become powerful when the result carries more than just a replacement string. Any extra YAML key on the option is accessible as an attribute on the `Result` object.

```yaml
lookups:
  relationships:
    lowercase: true
    options:
      - contains:
          - chairman of
          - director of
        schema: Directorship
        start: director
        end: organization
        link: role
      - contains:
          - shareholder of
          - owner of
        schema: Ownership
        start: owner
        end: asset
        link: role
```

The crawler reads `result.schema`, `result.start`, `result.end`, `result.link` to assemble the relation:

```python
link_type = row.pop("link_type")
res = context.lookup("relationships", link_type, warn_unmatched=True)
if res is None:
    continue
rel = context.make(res.schema)
rel.id = context.make_id(rel.schema, company.id, other_entity.id, link_type)
rel.add(res.start, entity)
rel.add(res.end, other_entity)
rel.add(res.link, link_type)
```

Pass `warn_unmatched=True` to log a warning when a value matches no option — this surfaces values that need a new lookup entry rather than silently dropping data.

For lookups where any unmatched value should halt the crawl, set `required: true` on the lookup itself. A miss then raises `LookupException`.


## Common runtime warnings and the lookup that fixes them

Several warnings emitted by `zavod` are best fixed by adding a lookup option. Each row below names the warning, what triggered it, and the lookup recipe.

| Warning | What it means | Fix |
|---|---|---|
| `Rejected property value [<prop>]: <value>` | The type cleaner could not normalize the value (an invalid date like `2020-02-31`, a country string like `France / Syria`, an unparseable phone number). | Add a `type.<type>` lookup mapping `<value>` to a corrected `value:` (or `values:` for the multi-country case). Use `value: null` to drop. |
| `Property value '<value>' is not a valid name.` | A name property on a `LegalEntity` failed [`rigour.names.is_name`](https://rigour.followthemoney.tech/) — usually because the string contains digits, punctuation patterns, or looks like an address. | `type.name` lookup with a corrected `value:`, or `value: null` to drop. The value-came-from-a-lookup check then suppresses the warning automatically. |
| `Property for <prop> looks too short for an address: <value>` | An address value is three characters or fewer. Often a parsing error; sometimes a real short place name. | If the value is a real place, add an identity lookup (`match: Zug` / `value: Zug`) to mark it as curated. Otherwise `value: null`. |
| `HTML/XSS suspicion in property value: <value>` | The value contains HTML tags or entity references — usually leftover markup from extraction. | Map the dirty value to its cleaned text via a `type.<type>` lookup. If the markup is genuinely intended (rare), add `silence_warnings: [xss-html-smell]` to the option. |
| `Property value for <prop> exceeds type length: <value>` | The value is longer than the type's `max_length`. `zavod` warns but does not truncate. | `type.<type>` lookup with a shorter `value:`. |
| `Failed to validate <format> identifier: <value>` | The value did not validate against a known identifier format (`bic`, `isin`, `lei`, `iban`, `inn`, `ogrn`, `npi`, `uei`, `qid`, `uscc`, `imo`). | `type.identifier` lookup with `match: <value>` and a corrected `value:`, or `value: null`. |

### Property name to type lookup

When an issue references a property name, the corresponding type lookup is one of these:

| Property names | Type lookup |
|---|---|
| `name`, `alias`, `previousName`, `weakAlias`, `firstName`, `lastName`, … | `type.name` |
| `address`, `full` | `type.address` |
| `country`, `jurisdiction`, `nationality`, `citizenship` | `type.country` |
| `date`, `startDate`, `endDate`, `birthDate`, `incorporationDate`, `dissolutionDate` | `type.date` |
| `registrationNumber`, `taxNumber`, `ogrnCode`, `innCode`, `npiCode`, `leiCode`, `bicCode`, `imoNumber`, … | `type.identifier` |
| `sourceUrl`, `website`, `wikipediaUrl` | `type.url` |
| `email` | `type.email` |
| `phone` | `type.phone` |
| `gender` | `type.gender` |

The full property listing is at [followthemoney.tech](https://followthemoney.tech/explorer/types/).


## Reference: configuration keys

### Lookup-level keys

Set under each named lookup (e.g. `lookups: type.country: …`):

| Key | Default | Effect |
|---|---|---|
| `options` | required | List of options. |
| `map` | — | Dict shorthand of `match: value` pairs; merged with `options`. |
| `normalize` | `false` | Strip diacritics and collapse whitespace before matching. |
| `lowercase` | `false` | Lowercase before matching. |
| `asciify` | `true` | Transliterate to ASCII when normalizing. |
| `required` | `false` | Raise `LookupException` when no option matches the input. |

### Option-level keys

Each entry in the `options:` list:

| Key | Effect |
|---|---|
| `match` | String, list of strings, or `null`; exact match after normalization. |
| `contains` | String or list; substring match after normalization. |
| `regex` | String or list; raw regex pattern, not normalized. |
| `value` / `values` | Replacement value(s). `value: null` drops the input. |
| `prop` | (`type.*` lookups only) Re-route the value to a different property. |
| `weight` | Integer to disambiguate when multiple options match the same input. |
| `normalize` / `lowercase` / `asciify` | Override the lookup-level setting. |
| `silence_warnings` | List of warning types to suppress. Currently the only recognized value is `xss-html-smell`. |
| any other key | Available as an attribute on the `Result` object (e.g. `schema`, `start`, `end`, `link`, `is_alias`, `document_schema`). |


## Recipe: translating column headers

A recurring use of named lookups is mapping non-English source column headers onto English slug keys that the crawler code references. The lookup runs once per header during table parsing.

```yaml
lookups:
  columns:
    options:
      - match: الإسم الثلاثي
        value: full_name
      - match: تاريخ الولادة ومكانها
        value: dob_place
      - match: العنوان
        value: address
      - match: الجنسية
        value: nationality
```

In the crawler, look up each header before treating it as a dictionary key:

```python
slug = context.lookup_value("columns", raw_header)
if slug is None:
    context.log.warning("Unknown column header", header=raw_header)
    continue
```

This pattern keeps the crawler code in English regardless of the source language, and any new header in the source surfaces as an explicit warning rather than silently dropped data.
