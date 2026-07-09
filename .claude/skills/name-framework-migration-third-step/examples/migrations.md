# Step 3 migration examples

These are taken directly from the documentation's migration example in
`zavod/docs/extract/names.md#migrating-to-the-name-cleaning-helpers`. Step 3 removes
**all** custom cleaning and the Step 1 review scaffolding, leaving one
`apply_reviewed_...` call.

The starting point below (the "Step 1 state") is the crawler as Step 1 left it. The
"Step 3" block is the whole result.

---

## Single name string

### Step 1 state (sanctions)

```python
entity = context.make("LegalEntity")
names_string = row.pop("full_name")
entity.id = context.make_id(names_string, ...)

original = h.Names(name=names_string)
suggested = h.Names()

names = h.multi_split(names_string, ["a.k.a."])
entity.add("name", names[0])
suggested.add("name", names[0])
entity.add("alias", names[1:])
for alias in names[1:]:
    suggested.add("alias", alias)

is_irregular, suggested = h.check_names_regularity(entity, suggested)
h.review_names(
    context,
    entity,
    original=original,
    suggested=suggested,
    is_irregular=is_irregular,
    default_accepted=True,
)
```

### Step 3 (sanctions)

The split, both `entity.add` calls, the `suggested`/`original` construction,
`check_names_regularity`, and `review_names` all go away:

```python
entity = context.make("LegalEntity")
names_string = row.pop("full_name")
entity.id = context.make_id(names_string, ...)

h.apply_reviewed_name_string(context, entity, string=names_string)
```

### Step 3 (non-sanctions)

Same removal, but add `llm_cleaning=True`:

```python
entity = context.make("LegalEntity")
names_string = row.pop("full_name")
entity.id = context.make_id(names_string, ...)

h.apply_reviewed_name_string(context, entity, string=names_string, llm_cleaning=True)
```

---

## Multiple name fields in the source data

When the source provides several name fields (rather than one string to split), build an
`h.Names(...)` from the raw source values and hand it to `h.apply_reviewed_names`:

```python
original = h.Names(name=item["name"], previousName=item["former_name"])
for alias in item["aliases"]:
    original.add("alias", alias["value"], lang=alias["language"])

h.apply_reviewed_names(context, entity, original=original)
```

For a non-sanctions dataset, add `llm_cleaning=True` to the `apply_reviewed_names` call.

---

## Key points

- `string=` (or the values in `h.Names(...)`) must be the **unmodified raw source
  string(s)** — never the cleaned/split value.
- Preserve any `lang=` the removed `entity.add(..., lang=...)` used by passing it to the
  `apply_reviewed_...` call.
- `h.apply_reviewed_name_string` and `h.apply_reviewed_names` are re-exported via
  `zavod.helpers` — verify they appear in `zavod/zavod/helpers/__init__.py` before
  assuming no extra import is needed.
- The `apply_reviewed_...` helpers return `None`; there is no return value to use.
