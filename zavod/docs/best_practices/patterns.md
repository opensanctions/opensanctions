# Usage patterns

The following are some patterns that have proved useful:

## Common crawler code structure

Our typical crawler structure consists of

1. a `crawl` function as the entrypoint which
    - fetches the data
    - converts it into an iterable of dicts, one per record
    - a loop over those records, calling...
2. a function called once per record, e.g. `crawl_item` or `crawl_person` which
    - unpacks the record dict
    - ensures the necessary cleaning takes place
    - creates one or more entities for the record (LegalEntity, Sanction, Position, relations, etc)
    - emits the created entities

We have a number of helpers to turn common formats into an iterable of dicts:

- [`h.parse_html_table()`][zavod.helpers.parse_html_table]
- [`h.parse_xlsx_sheet()`][zavod.helpers.parse_xlsx_sheet]

We typically `from zavod import helpers as h`.

When concise-enough to fit on a single line and only used once, we pop and add values on the same line:

```python
entity.add("name", row.pop("full_name"))
entity.add("birthPlace", row.pop("place_of_birth"))
```

## Detect unhandled data

If a variable number of fields can extracted automatically (e.g. from a list or table):

* Capture all fields provided by the data source in a `dict`.
* `dict_obj.pop()` individual fields when adding them to entities.
* Log warnings if there are unhandled fields remaining in the `dict` so that we notice and improve the crawler. The context method [`context.audit_data()`][zavod.context.Context.audit_data] can be used to warn about extra fields in a `dict`. It takes the `ignore` argument to explicitly list fields that are unused.

## Generating consistent unique identifiers

Make sure entity IDs are unique within the source. Avoid using only the name of the entity because there might eventually be two persons or two companies with the same name. [It is preferable](https://www.opensanctions.org/docs/identifiers) to have to deduplicate two Follow the Money entities for the same real world entity, rather than accidentally merge two entities. 

Good values to use as identifiers are:

* An ID in the source dataset, e.g. a sanction number, company registration number, personal identity number. These can be turned into a readable ID with the dataset prefix using the [`context.make_slug`][zavod.context.Context.make_slug] function.
* Some combination of consistent attributes, e.g. a person's name and normalised date of birth in a dataset that holds a relatively small proportion of the population. These attributes can be turned into a unique hash describing the entity using the [`context.make_id`][zavod.context.Context.make_slug] function.
* A combination of identifiers for the entities related by another entity, e.g. an 
  owner and a company, in the form `ownership.id = context.make_id(owner.id, "owns", company.id)`

## Capture text in its original language

Useful fields like the reason someone is sanctioned should be captured regardless of the language it is written in. Don't worry about translating fields where arbitrary text would be written. If the language is known, include the three-letter language code in the `lang` parameter to `Entity.add()`, e.g.:

```python
reason = data.pop("expunerea-a-temeiului-de-includere-in-lista-a-operatorului-economic")
sanction.add("reason", reason, lang="rom")
```

## Use datapatch lookups to clean or map values from external forms to OpenSanctions

See [Datapatches](datapatch_lookups.md)

e.g.

- Fixing typos in dates
- Translating column headings to English
- Mapping source data entity types to FollowTheMoney entity types
- Mapping relationship descriptions to FollowTheMoney relation entity types
