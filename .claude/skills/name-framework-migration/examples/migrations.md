# Real migration examples

Three patterns from production crawlers. Each shows a different variation.

---

## Pattern 1: raw capture before pop, no apply_name

**Crawler:** `datasets/us/ddtc_debarred/crawler.py`  
**Pattern:** helper function splits raw string; result goes to `entity.add`, not `h.apply_name`.

```python
# BEFORE
entity = context.make(schema)
name, aliases = split_names(row.pop(name_field))
entity.id = context.make_slug(name, date_of_birth, strict=False)
entity.add("name", name)
entity.add("alias", aliases)
```

```python
# AFTER
entity = context.make(schema)
raw_name = row.pop(name_field) # capture before cleaning
name, aliases = split_names(raw_name)
entity.id = context.make_slug(name, date_of_birth, strict=False)
entity.add("name", name)
entity.add("alias", aliases)
h.review_names(context, entity, original=h.Names(name=raw_name), default_accepted=False)
```

---


## Pattern 2: multi_split + star-unpack + reduce, no apply_name

**Crawler:** `datasets/nz/designated_terrorists/crawler.py`  
**Pattern:** `h.multi_split` with a list of delimiters, star-unpack into `name + aliases`, secondary split+reduce. Result goes to `organization.add`, not `h.apply_name`.

```python
# BEFORE
def crawl_item(input_dict: dict, context: Context):
    name, *aliases = h.multi_split(input_dict.pop("terrorist-entity")[0], ALIAS_SPLITS)
    alias_lists = [h.multi_split(alias, [", and", ","]) for alias in aliases]
    aliases = reduce(concat, alias_lists, [])

    organization = context.make("Organization")
    organization.id = context.make_slug(name)
    organization.add("topics", "sanction")
    organization.add("name", name)
    organization.add("alias", aliases)
```

```python
# AFTER
def crawl_item(input_dict: dict, context: Context):
    raw_name: str = input_dict["terrorist-entity"][0]   # capture before pop
    name, *aliases = h.multi_split(input_dict.pop("terrorist-entity")[0], ALIAS_SPLITS)
    alias_lists = [h.multi_split(alias, [", and", ","]) for alias in aliases]
    aliases = reduce(concat, alias_lists, [])

    organization = context.make("Organization")
    organization.id = context.make_slug(name)
    organization.add("topics", "sanction")
    organization.add("name", name)
    organization.add("alias", aliases)
    h.review_names(
        context,
        organization,
        original=h.Names(name=raw_name),
        default_accepted=False,
    )
```

---

## Key decisions consistent across all three

- `h.Names(name=<raw>)` — `Names` has no `middleName` field; `name` is the correct prop for unsplit source strings
- `h.Names` is re-exported via `zavod.helpers.__all__`, no extra import needed
- `default_accepted=False` — write-only mode; return value is always discarded
- `llm_cleaning` omitted — defaults to `False`, required for sanctions crawlers
- Existing hack left intact in all cases
