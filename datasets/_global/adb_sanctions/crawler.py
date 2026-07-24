import json
import re

from pydantic import BaseModel, JsonValue
from rigour.names import contains_split_phrase

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.extract.llm import run_typed_text_prompt
from zavod.stateful.review import (
    JSONSourceValue,
    review_extraction,
    assert_all_accepted,
)

LLM_MODEL_VERSION = "gpt-5.4"
EXTRACT_PROMPT = """Extract structured entity data from an entry from a list
of debarments from a development bank.

Input: JSON with "name" and "other_name" fields. These fields may contain:
- One or more distinct legal entity names (e.g. "Company A; Company B")
- Aliases, abbreviations, former names alongside a primary name
- Embedded registration numbers, tax IDs, USC codes, national ID numbers

For EACH distinct legal entity found, extract:
  name               - primary legal name(s)
  alias              - alternative or also-known-as names
  weakAlias          - abbreviations, acronyms, short forms
  previousName       - former legal names
  abbreviation       - official abbreviations
  registrationNumber - company / business registration numbers
  uscCode            - Unified Social Credit Codes (China 统一社会信用代码)
  taxNumber          - tax IDs (TIN, VAT, BIN, e-TIN, IRC, etc.)
  idNumber           - national ID, passport, CNIC, or other personal IDs

Rules:
- Preserve original text exactly (no spelling corrections, no title-casing).
- Do not invent or expand names not present in the input.
- If there is only one entity, return a list with one item.
- Exclude the numeric/alphanumeric identifier strings from name/alias fields;
  put them only in the relevant identifier field.
"""

PATTERN_IRREGULAR = r"[;()\\/:]|Reg\b|\bNo\b|Registration|Register|Number|operating|also|\baka\b|CNPJ|known"
REGEX_IRREGULAR = re.compile(PATTERN_IRREGULAR, re.IGNORECASE)
REGEX_INTERNAL_URL = re.compile(
    r"http://([\w-]+\.)+azurecontainerapps.io:80/published-list"
)


class EntityData(BaseModel):
    name: list[str] = []
    alias: list[str] = []
    weakAlias: list[str] = []
    previousName: list[str] = []
    abbreviation: list[str] = []
    registrationNumber: list[str] = []
    uscCode: list[str] = []
    taxNumber: list[str] = []
    idNumber: list[str] = []


class EntityExtractionResult(BaseModel):
    entities: list[EntityData]


def apply_entity_data(entity: Entity, data: EntityData) -> None:
    for prop in EntityData.model_fields:
        for val in getattr(data, prop):
            entity.add(prop, val)


def crawl_row(context: Context, row: dict[str, str | None]) -> None:
    full_name = row.pop("name") or ""
    other_name = (row.pop("otherName") or "").replace("\\", "")
    country = row.pop("nationality") or ""
    country = country.replace("Non ADB Member Country", "")
    country = country.replace("Rep. of", "").strip()
    country = country.replace("*2", "").strip()

    grounds = row.pop("grounds")
    sanction_type = row.pop("sanctionType")
    addresses = (row.pop("address") or "").split(";")
    start_date = row.pop("effectiveDateOfSanction")
    end_date = row.pop("lapseDateOfSanction")
    modified_at = row.pop("changesMadeOn")

    if (
        REGEX_IRREGULAR.search(full_name)
        or REGEX_IRREGULAR.search(other_name)
        or contains_split_phrase(full_name)
        or contains_split_phrase(other_name)
    ):
        source_data: JsonValue = {"name": full_name, "other_name": other_name}
        source_value = JSONSourceValue(
            key_parts=[full_name, "other_name", other_name],
            label="entity extraction",
            data=source_data,
        )
        result = run_typed_text_prompt(
            context=context,
            prompt=EXTRACT_PROMPT,
            string=json.dumps(source_data, ensure_ascii=False),
            response_type=EntityExtractionResult,
            model=LLM_MODEL_VERSION,
        )
        review = review_extraction(
            context=context,
            source_value=source_value,
            original_extraction=result,
            origin=LLM_MODEL_VERSION,
        )
        if review.accepted:
            entities_data = review.extracted_data.entities
        else:
            entities_data = []  # we loop over this later regardless
    else:
        alias = [other_name] if other_name.strip() else []
        entities_data = [EntityData(name=[full_name], alias=alias)]

    first_org = None
    for entity_data in entities_data:
        entity = context.make("LegalEntity")
        entity.id = context.make_id(entity_data.name[0], country)

        apply_entity_data(entity, entity_data)
        entity.add("country", country)
        entity.add("address", addresses)

        sanction = h.make_sanction(context, entity)
        sanction.add("reason", grounds)
        sanction.add("status", sanction_type)
        h.apply_date(sanction, "startDate", start_date)
        h.apply_date(sanction, "endDate", end_date)
        h.apply_date(sanction, "modifiedAt", modified_at)

        if h.is_active(sanction):
            entity.add("topics", "debarment")

        # create relation to preserve link
        # when there is more than one org per row
        if first_org is None:
            first_org = entity
        else:
            rel = context.make("UnknownLink")
            rel.id = context.make_id(
                first_org.first("name"), entity_data.name[0], country
            )
            rel.add("subject", first_org)
            rel.add("object", entity)
            context.emit(rel)

        context.emit(entity)
        context.emit(sanction)

    context.audit_data(row)


def crawl(context: Context) -> None:
    next_url = context.data_url + "?sortField=Name&isAscending=true"
    pages = 0
    while next_url:
        response = context.fetch_json(next_url)

        next_url = response["links"]["next"]
        if next_url is not None:
            next_url = REGEX_INTERNAL_URL.sub(context.data_url, next_url)

        for item in response["data"]:
            crawl_row(context, item["attributes"])

        pages += 1
        assert pages <= 500, "More pages than expected."

    assert_all_accepted(context)
