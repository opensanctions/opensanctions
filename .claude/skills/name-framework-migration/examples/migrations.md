# Real migration examples

Three patterns from production crawlers. Each shows a distinct variation of trigger pattern and raw-capture approach.

---

## Pattern 1: dataset-specific delimiter splits name into primary + aliases

**Crawler:** `datasets/pk/proscribed_persons/crawler.py`  
**Trigger:** `split("@")` — a dataset-specific delimiter embedded in the raw name string.  
**Capture approach:** rename the pop assignment from `person_name` to `raw_name`, then derive `person_name` from `name_split[0]` unconditionally.

```python
# BEFORE
def crawl_person(context: Context, row: dict):
    person_name = row.pop("Name")
    ...
    name_split = person_name.split("@")
    if len(name_split) > 1:
        person_name = name_split[0]
        entity.add("alias", name_split[1:])
    entity.add("name", person_name)
```

```python
# AFTER
def crawl_person(context: Context, row: dict):
    raw_name = row.pop("Name")
    ...
    name_split = raw_name.split("@")
    person_name = name_split[0]
    if len(name_split) > 1:
        entity.add("alias", name_split[1:])
    entity.add("name", person_name)
    h.review_names(context, entity, original=h.Names(name=raw_name))
```

---

## Pattern 2: regex splits on fka/aka/dba markers, all results dumped into `name`

**Crawler:** `datasets/_global/ebrd_ineligible/crawler.py`  
**Trigger:** `RE_NAME_SPLIT.split(name_raw)` — a regex that matches `also known as`, `f/k/a`, `formerly known as`, `doing business as`, ` or `, and several other alias/fka phrases. All split results go to `entity.add("name", ...)` without alias or previousName categorisation.  
**Capture approach:** none needed — the raw string is already assigned to `name_raw` at the pop site.

```python
NAME_SPLITS = [
    "may also be doing business as",
    "also doing business as",
    "doing business as",
    "also doing business under",
    "also known as",
    " or ",
    "f/k/a",
    "formerly known as",
    "formerly operating as",
    "formerly",
]
RE_NAME_SPLIT = re.compile("|".join(NAME_SPLITS), re.IGNORECASE)


# BEFORE
def crawl_entity(context: Context, data: Dict[str, Any]):
    name_raw = data.pop("title")
    if not name_raw:
        return
    ...
    entity.add("name", RE_NAME_SPLIT.split(name_raw))
```

```python
# AFTER
def crawl_entity(context: Context, data: Dict[str, Any]):
    name_raw = data.pop("title")
    if not name_raw:
        return
    ...
    entity.add("name", RE_NAME_SPLIT.split(name_raw))
    h.review_names(context, entity, original=h.Names(name=name_raw))
```

---

## Pattern 3: numeric prefix + bracket alias extraction + comma split for apply_name

**Crawler:** `datasets/ee/international_sanctions/crawler.py`, function `crawl_item_human_rights`  
**Trigger:** three transformations stack on the raw string:
1. Numeric prefix stripped by regex (`^\d+\.\d*\.?\s*`)
2. Alias text extracted from `(also known as ...)` brackets
3. Remaining name split on `", "` to feed `h.apply_name(first_name=..., last_name=...)`

**Capture approach:** none needed — `raw_name` is the function parameter; no mutation happens to it.

```python
# BEFORE
def crawl_item_human_rights(context: Context, source_url, raw_name: str):
    # "1. Mr. John Doe (also known as John Smith)"
    # "1.23 Mr. John Doe (also known as John Smith)"
    match = re.search(r"^\d+\.\d*\.?\s*([^(\n]+)(?:\s*\(also\s*([^)]+)\))?", raw_name)
    if match:
        name = match.group(1).strip()
        aliases = match.group(2).split("; ") if match.group(2) else []
    else:
        context.log.warning("Could not parse name", raw_name=raw_name)
        return

    last_name, first_name = name.split(", ")

    entity = context.make("Person")
    entity.id = context.make_id(name)
    h.apply_name(entity, first_name=first_name, last_name=last_name, lang="eng")
    entity.add("topics", "sanction")

    for alias in aliases:
        entity.add("alias", alias)
```

```python
# AFTER
def crawl_item_human_rights(context: Context, source_url, raw_name: str):
    # "1. Mr. John Doe (also known as John Smith)"
    # "1.23 Mr. John Doe (also known as John Smith)"
    match = re.search(r"^\d+\.\d*\.?\s*([^(\n]+)(?:\s*\(also\s*([^)]+)\))?", raw_name)
    if match:
        name = match.group(1).strip()
        aliases = match.group(2).split("; ") if match.group(2) else []
    else:
        context.log.warning("Could not parse name", raw_name=raw_name)
        return

    last_name, first_name = name.split(", ")

    entity = context.make("Person")
    entity.id = context.make_id(name)
    h.apply_name(entity, first_name=first_name, last_name=last_name, lang="eng")
    entity.add("topics", "sanction")

    for alias in aliases:
        entity.add("alias", alias)
    h.review_names(context, entity, original=h.Names(name=raw_name))
```

---

## Key decisions consistent across all three

- `h.Names(name=<raw>)` — `Names` only has `name`, `alias`, `weakAlias`, `previousName`, `abbreviation` fields; no `middleName`. Use `name` for unsplit source strings.
- `h.Names` is re-exported via `zavod.helpers` — verify it appears in `zavod/zavod/helpers/__init__.py` before assuming no extra import is needed.
- `llm_cleaning` omitted — defaults to `False`, must be `False` for all sanctions crawlers.
- Existing cleaning logic left intact in all cases — do not modify or remove it.
- The return value of `h.review_names` is always discarded — do not assign it.
