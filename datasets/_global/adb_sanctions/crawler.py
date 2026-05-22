import json
import re
from typing import Dict

from pydantic import BaseModel
from pydantic import JsonValue

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.extract.llm import run_typed_text_prompt
from zavod.extract.names.clean import LLM_MODEL_VERSION
from zavod.stateful.review import (
    JSONSourceValue,
    review_extraction,
    assert_all_accepted,
)

EXTRACT_PROMPT = """Extract structured entity data from a sanctions list entry.

Input: JSON with "name" and "other_name" fields. These fields may contain:
- One or more distinct legal entity names (e.g. "Company A; Company B")
- Aliases, abbreviations, former names alongside a primary name
- Embedded registration numbers, tax IDs, USC codes, national ID numbers

For EACH distinct legal entity found, extract:
  name              – primary legal name(s)
  alias             – alternative or also-known-as names
  weakAlias         – abbreviations, acronyms, short forms
  previousName      – former legal names
  abbreviation      – official abbreviations
  registrationNumber – company / business registration numbers
  uscCode           – Unified Social Credit Codes (China 统一社会信用代码)
  taxNumber         – tax IDs (TIN, VAT, BIN, e-TIN, IRC, etc.)
  idNumber          – national ID, passport, CNIC, or other personal IDs

Rules:
- Preserve original text exactly (no spelling corrections, no title-casing).
- Do not invent or expand names not present in the input.
- If there is only one entity, return a list with one item.
- Exclude the numeric/alphanumeric identifier strings from name/alias fields;
  put them only in the relevant identifier field.
"""

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


def crawl_row(context: Context, row: Dict[str, str | None]) -> None:
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

    # A probe entity is needed to call h.is_name_irregular, which consults
    # the dataset's names spec (reject_chars, reject_strings, contains_split_phrase).
    # It is reused as the emitted entity for single-entity rows.
    base_entity = context.make("LegalEntity")
    base_entity.id = context.make_id(full_name, country)

    # Trigger LLM extraction when the framework detects irregular name content
    # (split phrases, parentheses, reject_strings) or when other_name contains
    # digits — a reliable signal for embedded identifiers (TINs, reg numbers,
    # USC codes) that have no split-phrase marker.
    if (
        h.is_name_irregular(base_entity, full_name)
        or h.is_name_irregular(base_entity, other_name)
        or bool(other_name.strip() and re.search(r"\d", other_name))
    ):
        source_data: JsonValue = {"name": full_name, "other_name": other_name}
        source_value = JSONSourceValue(
            key_parts=[context.dataset.name, full_name, other_name],
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
        # if the review is accepted return entities
        # otherwise return
        if review.accepted:
            entities_data = review.extracted_data.entities
        else:
            entities_data = []  # we loop over this later regardless
    else:
        alias = [other_name] if other_name.strip() else []
        entities_data = [EntityData(name=[full_name], alias=alias)]

    for entity_data in entities_data:
        entity = context.make("LegalEntity")
        entity.id = context.make_id(full_name, country)

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
