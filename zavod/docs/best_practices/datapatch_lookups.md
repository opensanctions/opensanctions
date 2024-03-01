# Datapatch lookups

We often use the [datapatch](https://pypi.org/project/datapatch/) library to help
clean up the data, as well as to map between data values and OpenSanctions concepts.

Lookups are sets of matching options which can be defined under the `lookups` key in the metadata.
They can then be used via [`context.lookup_value`][zavod.context.Context.lookup_value].

Lookups named after [FollowTheMoney](https://followthemoney.tech/explorer/#types)
types are also automatically invoked for each value when adding/setting an entity
property of that type. See below.

!!! info "Please note"
    Avoid using lookups to express something not evident in the data. E.g. don't
    make a date more precise than it is in the data, even if you know your version
    to be true.


## Fixing typos and formatting

A very common use is to fix typos and mistakes in the data:

In the metadata:

A lookup named after the FtM type prefixed with `type.`, where each option has ha `value` property.

```yaml
lookups:
  type.email:
    lowercase: true
    options:
      - match:
          - 307j@att
          - SL Jones@ballhealth.com
          - na
        value: null
      - match: tcolpetzer@mcdonoughga.org.
        value: tcolpetzer@mcdonoughga.org
      - match: sensan buenaventura@capitol.hawaii.gov
        value: sensanbuenaventura@capitol.hawaii.gov
      - match: district@repkelly.com, mike@repkelly.com
        values:
          - district@repkelly.com
          - mike@repkelly.com
  type.country: ...
```

In the crawler:

```python
entity.add("email", row.pop("email"))
```

The lookup is automatically invoked when setting the value, and the original value
is replaced by the corrected value:

- Three values where the correct value is unknown are replaced by None in Python world
- The trailing dot is dropped
- Two addresses are replaced by a list of addresses in Python world


## Mapping to OpenSanctions concepts

In the metadata:

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
          - beneficiary of
          - shareholder of
          - owner of
        schema: Ownership
        start: owner
        end: asset
        link: role
      - contains:
          - connected to
          - auditor of
        schema: UnknownLink
        start: subject
        end: object
        link: role
  type.country:
    options:
      - contains: ...
```

The lookup is named `relationships`. Each option defines four properties:
`schema`, `start`, `end`, and `link`. These are then available in the result,
if any option matched.

In the crawler:

```python
link_type = row.pop("link_type")
res = context.lookup("relationships", link_type)
if res is None:
    context.log.warning("Unknown relationship", link_type=link_type)
rel = context.make(res.schema)
rel.id = context.make_id(rel.schema, company.id, other_entity.id, link_type)
rel.add(res.start, entity)
rel.add(res.end, other_entity)
rel.add(res.link, link_type)
```

The result objects define both the schema, and the property names applicable to
the matching schema, to be able to concisely generate differet types of relationships
between two entities assumed to be created earlier - `entity`, and `other_entity`.

!!! info "Please note"
    It's usually a good idea to structure the code so that you warn if an unmatched
    value is encountered, instead of silently ignoring values and possibly excluding
    valid data from the dataset.