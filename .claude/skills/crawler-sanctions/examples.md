# Sanctions Crawler Examples

## Core sanctions pattern

Every sanctions crawler follows this structure: create an entity, create a linked
Sanction, mark the entity with `topics: sanction`, emit both.

```python
from lxml.etree import _Element
from rigour.mime.types import XML

from zavod import Context
from zavod import helpers as h


def crawl_entry(context: Context, node: _Element) -> None:
    # 1. Determine entity type
    entity_type = node.get("type")
    schema = context.lookup_value("type.entity", entity_type)
    if schema is None:
        context.log.warning("Unknown entity type", type=entity_type)
        return
    entity = context.make(schema)

    # 2. Set entity ID (prefer source-assigned opaque IDs)
    source_id = node.findtext("./Id")
    assert source_id is not None, node
    entity.id = context.make_slug(source_id)

    # 3. Add names
    entity.add("name", node.findtext("./Name"))
    for alias_node in node.findall("./Alias"):
        entity.add("alias", alias_node.text)

    # 4. Add dates (ALWAYS through h.apply_date)
    h.apply_date(entity, "birthDate", node.findtext("./DOB"))

    # 5. Add address
    addr = h.make_address(
        context,
        street=node.findtext("./Address/Street"),
        city=node.findtext("./Address/City"),
        country=node.findtext("./Address/Country"),
    )
    h.copy_address(entity, addr)

    # 6. Create sanction
    program = node.findtext("./Program")
    sanction = h.make_sanction(
        context,
        entity,
        program_name=program,
        source_program_key=program,
        program_key=h.lookup_sanction_program_key(context, program),
    )
    sanction.add("authorityId", source_id)
    h.apply_date(sanction, "listingDate", node.findtext("./ListingDate"))
    sanction.add("reason", node.findtext("./Reason"))

    # 7. Mark entity as sanctioned and emit
    entity.add("topics", "sanction")
    context.emit(entity)
    context.emit(sanction)


def crawl(context: Context) -> None:
    path = context.fetch_resource("source.xml", context.data_url)
    context.export_resource(path, XML, title=context.SOURCE_TITLE)
    doc = context.parse_resource_xml(path)
    h.remove_namespace(doc)  # simplifies XPath for namespace-heavy XML
    for node in doc.findall(".//Entry"):
        crawl_entry(context, node)
```

## Entity type dispatch

### From source type field

```python
TYPES = {
    "Entity": "Organization",
    "Individual": "Person",
    "Vessel": "Vessel",
    "Aircraft": "Airplane",
}

schema = TYPES.get(raw_type)
if schema is None:
    raise ValueError(f"Unknown entity type: {raw_type!r}")
entity = context.make(schema)
```

### Via yaml lookup

```python
schema = context.lookup_value("type.entity", raw_type)
if schema is None:
    return  # or raise
entity = context.make(schema)
```

### Inferred from available fields

When the source doesn't provide an explicit type field:

```python
imo_number = node.findtext("./IMONumber")
given_name = node.findtext("./GivenName")
last_name = node.findtext("./LastName")

if imo_number:
    entity = context.make("Vessel")
    entity.add("imoNumber", imo_number)
elif given_name or last_name:
    entity = context.make("Person")
else:
    entity = context.make("LegalEntity")
```

## Alias handling

### Alias types

Map source alias categories to FTM properties:

```python
ALIAS_TYPES = {
    "Name": "name",
    "A.K.A.": "alias",
    "F.K.A.": "previousName",     # formerly known as
    "N.K.A.": "name",             # now known as
}
prop = ALIAS_TYPES.get(alias_type, "alias")

# Weak aliases (low-quality names)
is_weak = quality == "low"
if is_weak:
    entity.add("weakAlias", name_value)
else:
    entity.add(prop, name_value)
```

### Splitting multi-name strings

When a single field contains multiple names separated by delimiters:

```python
NAME_SPLITS = [" (a.k.a.", " (also known as", "/"]
ALIAS_SPLITS = [",", ";", " (a.k.a.", " a.k.a.", "ALIAS:", "Hebrew:", "Arabic:"]

def split_name(name: str) -> list[str]:
    parts = h.multi_split(name, NAME_SPLITS)
    return [p.strip().rstrip(")").rstrip(";") for p in parts if p.strip()]
```

### Multi-script / multi-language names

When the source provides names in multiple scripts:

```python
# Script -> language mapping (via yaml lookup)
# script.lang:
#   options:
#     - match: [Hans, Hant]
#       value: zho
#     - match: Cyrl
#       value: rus

lang = context.lookup_value("script.lang", script_id)
h.apply_name(entity, first_name=first, last_name=last, lang=lang)
```

## Identification documents

For passports, ID numbers, registration numbers:

```python
passport = h.make_identification(
    context,
    entity,
    number=doc_number,
    doc_type=doc_type_text,          # "Passport", "National ID", etc.
    country=issuing_country,
    summary=remark,
    start_date=issue_date,
    end_date=expiry_date,
    authority=issuing_authority,
    passport=is_passport,            # True for passport-type docs
    key=doc_id,                      # disambiguator
)
if passport is not None:
    context.emit(passport)
```

For direct ID properties (no separate Identification entity needed):

```python
entity.add("passportNumber", passport_number)
entity.add("idNumber", id_number)
entity.add("registrationNumber", reg_number)
entity.add("taxNumber", tax_number)
entity.add("leiCode", lei_code)
entity.add("innCode", inn_code)
```

## Vessel-specific properties

```python
vessel = context.make("Vessel")
vessel.add("name", vessel_name)
vessel.add("imoNumber", imo_number)
vessel.add("mmsi", mmsi_number)
vessel.add("flag", flag_country)
vessel.add("callSign", call_sign)
vessel.add("type", vessel_type)
vessel.add("tonnage", tonnage)
h.apply_date(vessel, "buildDate", build_date)
```

## UnknownLink (for untyped relationships)

```python
link = context.make("UnknownLink")
link.id = context.make_id(subject.id, "link", object_.id)
link.add("subject", subject)
link.add("object", object_)
link.add("role", role_description)
context.emit(link)
```

## De-listing and modification tracking

When the source tracks modifications and de-listings:

```python
for mod in node.findall("./Modification"):
    mod_type = mod.get("type")
    effective_date = mod.get("effective-date")
    if mod_type == "de-listed":
        sanction.add("endDate", effective_date)
        # Optionally skip emitting de-listed entities entirely:
        return
    elif mod_type == "amended":
        sanction.add("modifiedAt", effective_date)
```

## LLM extraction from free-text fields

For sources with unstructured "other information" or "remarks" fields that contain
structured data (dates, nationalities, related entities), use GPT extraction with
the stateful review system:

```python
from pydantic import BaseModel, Field
from typing import Optional
from zavod.extract.llm import run_typed_text_prompt
from zavod.stateful.review import TextSourceValue, review_extraction

class ExtractedInfo(BaseModel):
    birth_place: Optional[str] = None
    nationality: Optional[str] = None
    related_entities: list[str] = Field(default_factory=list)

PROMPT = """Extract structured information from this sanctions entry's remarks field.
Return birth_place, nationality, and any related entity names mentioned."""

source_value = TextSourceValue(
    key_parts=entity_id,
    label="Other Information",
    text=raw_text,
    url=context.data_url,
)

result = run_typed_text_prompt(context, PROMPT, raw_text, ExtractedInfo)

review = review_extraction(
    context,
    source_value=source_value,
    original_extraction=result,
    origin="gpt-4o",
)

if review.accepted:
    entity.add("birthPlace", review.extracted_data.birth_place)
    entity.add("nationality", review.extracted_data.nationality)
    # ... emit related entities
```

Note: `ci_test: false` is required when the crawler uses LLM extraction (no API key in CI).
For large crawlers using LLM, flush periodically: `context.flush()` every ~100 entries.
