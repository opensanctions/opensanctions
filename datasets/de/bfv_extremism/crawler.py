import re
from pydantic import BaseModel
from lxml.etree import _Element

from zavod import Context, helpers as h
from rigour.names.split_phrases import contains_split_phrase
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


def crawl_row(context: Context, row: dict[str, _Element]) -> None:
    str_row = h.cells_to_str(row)
    name = str_row.pop("organisation")
    assert name is not None

    if contains_split_phrase(name):
        source_value = TextSourceValue(
            key_parts=name,
            label="Organization Name",
            text=name,
            url=context.data_url,
        )
        # basic split and let reviewers do the rest
        name, *aliases = h.multi_split(name, ["auch agierend unter"])
        organization = [Organization(name=name, aliases=aliases)]
        original_extraction = Organizations(organizations=organization)

        review = review_extraction(
            context,
            source_value=source_value,
            original_extraction=original_extraction,
            origin="heuristic",
        )

        if not review.accepted:
            return
        extracted_data = review.extracted_data
    else:
        extracted_data = Organizations(organizations=[Organization(name=name)])

    for item in extracted_data.organizations:
        entity = context.make("Organization")
        entity.id = context.make_id(item.name)

        entity.add("name", item.name)
        entity.add("alias", item.aliases)
        entity.add("previousName", item.previous_names)
        entity.add("abbreviation", item.abbreviations)
        entity.add("topics", "sanction")
        entity.add("country", "de")

        ban_info = str_row.pop("verbot_verbotsgrunde_auszug")
        assert ban_info is not None
        sanction = h.make_sanction(context, entity)

        if "Vollzug des Verbots" in ban_info:
            ban_info = ban_info.split("Vollzug des Verbots: ")[1]
            date, reason = re.split(r"Verbotsgr[uü]nd\w*:\s*", ban_info, maxsplit=1)
            h.apply_date(sanction, "listingDate", date.strip())
            sanction.add("reason", reason.strip())
        elif "Inkrafttreten des Kennzeichenverbots" in ban_info:
            ban_info = ban_info.split("Inkrafttreten des Kennzeichenverbots: ")[1]
            pattern = r"\d{1,2}\.\s+(?:Januar|Februar|März|April|Mai|Juni|Juli|August|September|Oktober|November|Dezember)\s+(?:19|20)\d{2}"
            match = re.search(pattern, ban_info)
            assert match is not None
            date = match.group().strip()
            reason = ban_info[match.end() :].strip()
            h.apply_date(sanction, "listingDate", date.strip())
            sanction.add("reason", reason.strip())
        else:
            context.log.warning(f"Unexpected ban info format: {ban_info}")

        context.emit(entity)
        context.emit(sanction)

    context.audit_data(str_row, ignore=["verbotene_symbole_und_kennzeichen"])


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url)
    table = h.xpath_element(doc, ".//table")

    for row in h.parse_html_table(table):
        crawl_row(context, row)

    assert_all_accepted(context, raise_on_unaccepted=False)
