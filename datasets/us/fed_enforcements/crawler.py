import csv
from typing import Dict, Optional
from urllib.parse import urljoin, urlparse

from pydantic import BaseModel, Field
from rigour.mime.types import CSV
from zavod.entity import Entity
from zavod.shed.gpt import DEFAULT_MODEL, run_typed_text_prompt
from zavod.stateful.review import (
    TextSourceValue,
    assert_all_accepted,
    review_extraction,
)

from zavod import Context
from zavod import helpers as h

PROGRAM_KEY = "US-FED-ENF"

CRAWLER_VERSION = 1


class BankOrgEntity(BaseModel):
    name: str = Field(
        description="The name of the company, exactly as spelled in the text."
    )
    locality: str | None = Field(
        description="The city, state and country of the company, if explicitly stated in the text.",
        default=None,
    )
    country: str | None = Field(
        description="The two-letter ISO code of the country reflected by the `locality`.",
        default=None,
    )


class BankOrgsResult(BaseModel):
    entities: list[BankOrgEntity]


ORG_PARSE_PROMPT = f"""
Extract the entities in the attached string.

NEVER infer, assume, or generate values that are not directly stated in the source text.

Instructions for specific fields:

 - name: {BankOrgEntity.model_fields["name"].description}
 - locality: {BankOrgEntity.model_fields["locality"].description}
 - country: {BankOrgEntity.model_fields["country"].description}
"""


def crawl_article(context: Context, url: Optional[str]) -> Optional[Entity]:
    if not url or not url.strip():
        return None
    if url.endswith(".pdf") or url.endswith(".csv") or "boarddocs" in url:
        # Create the Documentation but don't try and fetch and extract the title and date.
        title = [None]
        published_at = [None]
    else:
        doc = context.fetch_html(str(url), cache_days=90)
        title = doc.xpath(".//h3[@class='title']/text()")
        published_at = doc.xpath(".//p[@class='article__time']/text()")
    article = h.make_article(context, url, title=title, published_at=published_at[0])
    context.emit(article)
    return article


def crawl_item(
    context: Context, original_filename: str, input_dict: Dict[str, Optional[str]]
):
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
        schema = "Company"
        affiliation = None
        party_name = input_dict.pop("Banking Organization")
        source_value = TextSourceValue(
            key_parts=party_name,
            label="Banking Organization field in CSV",
            text=party_name,
        )
        prompt_result = run_typed_text_prompt(
            context,
            prompt=ORG_PARSE_PROMPT,
            string=source_value.value_string,
            response_type=BankOrgsResult,
        )
        review = review_extraction(
            context=context,
            crawler_version=CRAWLER_VERSION,
            source_value=source_value,
            original_extraction=prompt_result,
            origin=DEFAULT_MODEL,
        )
        origin = review.origin
        entities = review.extracted_data.entities if review.accepted else []

    effective_date = input_dict.pop("Effective Date")
    termination_date = input_dict.pop("Termination Date")
    provisions = input_dict.pop("Action")
    sanction_description = input_dict.pop("Note")
    url = input_dict.pop("URL", None)
    article = crawl_article(context, url)
    for ent in entities:
        entity = context.make(schema)
        entity.id = context.make_id(party_name, ent.name, affiliation, ent.locality)
        entity.add("name", ent.name, origin=origin)
        entity.add("sourceUrl", url)

        if ent.locality:
            entity.add("address", ent.locality, origin=origin)
        entity.add("country", ent.country, original_value=ent.locality, origin=origin)

        if schema == "Company":
            entity.add("topics", "fin.bank")

        sanction = h.make_sanction(
            context, entity, key=[effective_date], program_key=PROGRAM_KEY
        )
        h.apply_date(sanction, "startDate", effective_date)
        sanction.add("provisions", provisions)
        sanction.add("description", sanction_description)
        sanction.set("sourceUrl", url)

        h.apply_date(sanction, "endDate", termination_date)
        is_active = h.is_active(sanction)
        if is_active:
            entity.add("topics", "reg.action")

        if article:
            documentation = h.make_documentation(context, entity, article)
            context.emit(documentation)

        context.emit(entity)
        context.emit(sanction)

    # Name = the string that appears in the url column
    context.audit_data(input_dict, ignore=["Name"])


def crawl(context: Context):
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
