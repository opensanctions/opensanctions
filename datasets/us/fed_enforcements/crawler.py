import csv
from hashlib import sha1
from normality import slugify
from typing import Dict, List
from urllib.parse import urljoin, urlparse

from rigour.mime.types import CSV
from pydantic import BaseModel, Field

from zavod import Context
from zavod import helpers as h
from zavod.shed.gpt import run_typed_text_prompt, DEFAULT_MODEL
from zavod.stateful.review import assert_all_accepted, get_review, request_review


ORG_PARSE_PROMPT = """Extract the entities in the following attached string.
Only included information in the provided string. Leave missing fields as null."""
PROGRAM_NAME = "US Federal Reserve Enforcement Actions"

MODEL_VERSION = 1
MIN_MODEL_VERSION = 1


class BankOrgEntity(BaseModel):
    name: str = Field(
        description="The name of the company, exactly as spelled in the text."
    )
    locality: str | None = Field(
        description="The city, state and country of the company, only if indicated in the text.",
        default=None,
    )
    country: str | None = Field(
        description="The two-letter ISO code of the country reflected by the `locality`.",
        default=None,
    )


class BankOrgsResult(BaseModel):
    entities: list[BankOrgEntity]


def review_key(string: str) -> str:
    slug = slugify(string)
    if len(string) <= 255:
        return string
    else:
        hash = sha1(string.encode("utf-8")).hexdigest()[:10]
        return f"{slug[:80]}-{hash}"


def crawl_item(context: Context, original_filename: str, input_dict: Dict[str, str]):
    origin = None
    if input_dict["Individual"]:
        schema = "Person"
        party_name = input_dict.pop("Individual")

        names = [party_name]
        result = context.lookup("individual_name", party_name)
        if result:
            names = result.values
            origin = "hand-extracted"
        else:
            origin = original_filename
            if len(party_name) > 50:
                context.log.warn("Name too long", name=party_name)
        affiliation = input_dict.pop("Individual Affiliation")
        entities = [BankOrgEntity(name=name) for name in names]
    else:
        origin = DEFAULT_MODEL
        schema = "Company"
        affiliation = None
        party_name = input_dict.pop("Banking Organization")

        review = get_review(
            context,
            BankOrgsResult,
            party_name,
            MIN_MODEL_VERSION,
        )
        if review is None:
            prompt_result = run_typed_text_prompt(
                context,
                prompt=ORG_PARSE_PROMPT,
                string=party_name,
                response_type=BankOrgsResult,
            )
            review = request_review(
                context=context,
                key=review_key(party_name),
                source_value=party_name,
                source_mime_type="text/plain",
                source_label="Banking Organization field in CSV",
                source_url=None,
                orig_extraction_data=prompt_result,
                model_version=MODEL_VERSION,
            )
        entities = review.extracted_data.entities if review.accepted else []

    effective_date = input_dict.pop("Effective Date")
    termination_date = input_dict.pop("Termination Date")
    provisions = input_dict.pop("Action")
    sanction_description = input_dict.pop("Note")
    url = input_dict.pop("URL", None)
    for ent in entities:
        entity = context.make(schema)
        entity.id = context.make_id(party_name, ent.name, affiliation, ent.locality)
        entity.add("name", ent.name, origin=origin)

        if ent.locality:
            entity.add("address", ent.locality, origin=origin)
        entity.add("country", ent.country, original_value=ent.locality, origin=origin)

        if schema == "Company":
            entity.add("topics", "fin.bank")

        sanction = h.make_sanction(
            context,
            entity,
            key=[effective_date],
            program_name=PROGRAM_NAME,
            program_key=h.lookup_sanction_program_key(context, PROGRAM_NAME),
        )
        h.apply_date(sanction, "startDate", effective_date)
        sanction.add("provisions", provisions)
        sanction.add("description", sanction_description)
        sanction.add("sourceUrl", url)

        h.apply_date(sanction, "endDate", termination_date)
        is_active = h.is_active(sanction)
        if is_active:
            entity.add("topics", "reg.action")

        context.emit(entity)
        context.emit(sanction)

    # Name = the string that appears in the url column
    context.audit_data(input_dict, ignore=["Name"])


def crawl(context: Context):
    # Load up the previously-accepted reviews
    for option in context.get_lookup("bank_orgs").options:
        source_value = option.config["match"]
        key = review_key(source_value)
        review = get_review(context, BankOrgsResult, key, MIN_MODEL_VERSION)
        if review is not None:
            continue
        entities: List[BankOrgEntity] = []

        for entity_config in option.config["entities"]:
            entity = BankOrgEntity(**entity_config)
            entities.append(entity)

        hand_extracted = BankOrgsResult(entities=entities)

        request_review(
            context=context,
            key=key,
            source_value=source_value,
            source_mime_type="text/plain",
            source_label="Banking Organization field in CSV",
            source_url=None,
            orig_extraction_data=hand_extracted,
            model_version=MODEL_VERSION,
            default_accepted=True,
        )

    original_filename = urlparse(context.data_url).path.split("/")[-1]
    path = context.fetch_resource("source.csv", context.data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)

    with open(path, "r", encoding="utf-8-sig") as fh:
        for item in csv.DictReader(fh):
            url = item.pop("URL")
            if url != "DNE":
                item["URL"] = urljoin(context.data_url, url)
            crawl_item(context, original_filename, item)

    assert_all_accepted(context, raise_on_unaccepted=False)
