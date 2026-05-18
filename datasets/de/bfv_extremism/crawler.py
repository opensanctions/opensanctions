from pydantic import BaseModel
from lxml.etree import _Element

from zavod import Context, helpers as h
from zavod.stateful.review import (
    TextSourceValue,
    review_extraction,
    assert_all_accepted,
)


class Organization(BaseModel):
    name: str
    aliases: list[str] = []
    abbreviations: list[str] = []
    previous_names: list[str] = []


class Organizations(BaseModel):
    organizations: list[Organization]
    start_date: str | None = None
    reason: str | None = None


def crawl_row(context: Context, row: dict[str, _Element]) -> None:
    str_row = h.cells_to_str(row)
    name = str_row.pop("organisation")
    ban_info = str_row.pop("verbot_verbotsgrunde_auszug")
    assert name is not None
    assert ban_info is not None

    # send every row for review
    source_value = TextSourceValue(
        key_parts=name,
        label="Organization and Ban Info",
        text=f"{name}\n\n{ban_info}",
        url=context.data_url,
    )
    # basic split and let reviewers do the rest
    name, *aliases = h.multi_split(name, ["auch agierend unter"])
    organization = [Organization(name=name, aliases=aliases)]
    original_extraction = Organizations(organizations=organization, reason=ban_info)

    review = review_extraction(
        context,
        source_value=source_value,
        original_extraction=original_extraction,
        origin="heuristic",
    )

    if not review.accepted:
        extracted_data = original_extraction
    else:
        extracted_data = review.extracted_data

    first_entity = None
    for item in extracted_data.organizations:
        entity = context.make("Organization")
        entity.id = context.make_id(item.name)

        entity.add("name", item.name)
        entity.add("alias", item.aliases)
        entity.add("previousName", item.previous_names)
        entity.add("abbreviation", item.abbreviations)
        entity.add("topics", "sanction")
        entity.add("country", "de")

        sanction = h.make_sanction(context, entity)
        h.apply_date(sanction, "listingDate", extracted_data.start_date)
        sanction.add("reason", extracted_data.reason)

        if first_entity is None:
            first_entity = entity
        else:
            link = context.make("UnknownLink")
            link.add("subject", first_entity)
            link.add("object", entity)
            context.emit(link)
            print(link)

        context.emit(entity)
        context.emit(sanction)

    context.audit_data(str_row, ignore=["verbotene_symbole_und_kennzeichen"])


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url)
    table = h.xpath_element(doc, ".//table")

    for row in h.parse_html_table(table):
        crawl_row(context, row)

    assert_all_accepted(context, raise_on_unaccepted=False)
