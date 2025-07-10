import csv
from normality import slugify
import yaml  # noqa
from typing import Dict
from urllib.parse import urljoin

from rigour.mime.types import CSV

from zavod import Context
from zavod import helpers as h
from zavod.shed.gpt import run_typed_text_prompt
from zavod.stateful.extraction import extract_items
from pydantic import BaseModel


ORG_PARSE_PROMPT = """From the following list of organisations or companies, please extract an array of
        JSON objects (named `entities`) for each separate entity with the following structure: `name`,
        containing the  name of the company, exactly as spelled in the text, and `locality`, containing
        the city, state and country of the company. Include a field called `country` with the two-letter
        ISO code of the country reflected by the `locality`. Do not include any other information and
        include the entity name exactly as stated, without any additions. If only one entity is listed,
        make it the sole item in the JSON array `entities`."""
PROGRAM_NAME = "US Federal Reserve Enforcement Actions"


class BankOrgEntity(BaseModel):
    name: str
    locality: str | None = None
    country: str | None = None


class BankOrgsResult(BaseModel):
    entities: list[BankOrgEntity]


def crawl_item(input_dict: Dict[str, str], context: Context):
    url = input_dict.get("URL", None)
    if input_dict["Individual"]:
        schema = "Person"
        party_name = input_dict.pop("Individual")

        names = [party_name]
        result = context.lookup("individual_name", party_name)
        if result:
            names = result.values
        elif len(party_name) > 50:
            context.log.warn("Name too long", name=party_name)
        affiliation = input_dict.pop("Individual Affiliation")
        entities = [
            BankOrgEntity(name=name, locality=None, country=None) for name in names
        ]
    else:
        schema = "Company"
        affiliation = None
        party_name = input_dict.pop("Banking Organization")

        prompt_result = run_typed_text_prompt(
            context,
            prompt=ORG_PARSE_PROMPT,
            string=party_name,
            response_type=BankOrgsResult,
        )
        accepted_result = extract_items(
            context,
            key=slugify(party_name),
            source_value=party_name,
            source_content_type="text/plain",
            source_label="Banking Organization",
            orig_extraction_data=prompt_result,
            source_url=url or "",
        )
        entities = accepted_result.entities if accepted_result else []

    effective_date = input_dict.pop("Effective Date")
    termination_date = input_dict.pop("Termination Date")
    provisions = input_dict.pop("Action")
    sanction_description = input_dict.pop("Note")
    url = input_dict.pop("URL", None)
    for ent in entities:
        entity = context.make(schema)
        entity.id = context.make_id(party_name, ent.name, affiliation, ent.locality)
        entity.add("name", ent.name)

        if ent.locality:
            entity.add("address", ent.locality)
        entity.add("country", ent.country, original_value=ent.locality)

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
    path = context.fetch_resource("source.csv", context.data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)

    with open(path, "r", encoding="utf-8-sig") as fh:
        for item in csv.DictReader(fh):
            url = item.pop("URL")
            if url != "DNE":
                item["URL"] = urljoin(context.data_url, url)
            crawl_item(item, context)

    # sections = [{"match": k, "entities": v} for k, v in NEW_BANK_ORGS.items()]
    # print(yaml.dump({"options": sections}, sort_keys=False))
