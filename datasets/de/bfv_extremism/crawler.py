from pydantic import BaseModel
from lxml.etree import _Element

from zavod import Context, helpers as h
from zavod.stateful.review import (
    HtmlSourceValue,
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

    cells = h.xpath_elements(row, ".//td")
    assert len(cells) == 3
    name = h.element_text(cells[0])
    ban_info = h.element_text(cells[1])
    assert name is not None
    assert ban_info is not None
    
    # send every row for review
    source_value = HtmlSourceValue(
        key_parts=name,
        label="Organization and Ban Info",
        element=row,
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
        return
    extracted_data = review.extracted_data

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

        context.emit(entity)
        context.emit(sanction)



def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url)
    table = h.xpath_element(doc, ".//table")

    headings = h.xpath_elements(table, ".//tr[th]/th")
    assert len(headings) == 3
    assert h.element_text(headings[0]) == "Organisation", h.element_text(headings[0])
    assert h.element_text(headings[1]) == "Verbotene Symbole und Kennzeichen", h.element_text(headings[1])
    assert h.element_text(headings[2]) == "Verbot / Verbotsgründe (Auszug)", h.element_text(headings[2])

    for row in h.xpath_elements(table, ".//tr[td]"):
        crawl_row(context, row)

    assert_all_accepted(context, raise_on_unaccepted=False)
