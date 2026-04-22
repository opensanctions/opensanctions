from lxml.etree import _Element
from pydantic import BaseModel

from zavod import Context, helpers as h
from rigour.names.split_phrases import contains_split_phrase
from zavod.shed import enforcements
from zavod.stateful.review import (
    TextSourceValue,
    review_extraction,
    assert_all_accepted,
)


MAX_AGE_DAYS = 365 * 10  # keep 10 years of history


class Respondent(BaseModel):
    name: str
    aliases: list[str] = []
    abbreviations: list[str] = []
    previous_names: list[str] = []


class Respondents(BaseModel):
    respondents: list[Respondent]


IRREGULARITIES = [" and ", "["]
SPLITS = ["d/b/a", "a/k/a", "f/k/a", " and "]


def crawl_row(context: Context, row: dict[str, _Element]) -> None:
    # fetch case's url
    url_el = row.get("enforcement_action")
    assert url_el is not None
    detail_url = h.xpath_string(url_el, ".//a/@href")

    # process row
    str_row = h.cells_to_str(row)
    case_name = str_row.pop("enforcement_action")
    sanction_date = str_row.pop("date_sort_ascending")
    matter_number = str_row.pop("matter_number")
    assert sanction_date is not None
    if not enforcements.within_max_age(context, sanction_date, MAX_AGE_DAYS):
        return

    assert case_name is not None
    case_name = case_name.replace("In the Matter of", "")
    assert "in the matter of" not in case_name.lower()

    needs_review = contains_split_phrase(case_name) or any(
        irregularity in case_name for irregularity in IRREGULARITIES
    )

    if needs_review:
        source_value = TextSourceValue(
            key_parts=case_name,
            label="Case Name",
            text=case_name,
            url=context.data_url,
        )

        # There are so few, it isn't worth maintaining a prompt. Just basic splits
        # and let reviewers split into multiple respondents if needed.
        name, *aliases = h.multi_split(case_name, SPLITS)
        respondents = [Respondent(name=name, aliases=aliases)]
        original_extraction = Respondents(respondents=respondents)

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
        extracted_data = Respondents(respondents=[Respondent(name=case_name)])

    financial_institution = str_row.pop("financial_institution")

    for item in extracted_data.respondents:
        entity = context.make("LegalEntity")
        entity.id = context.make_id(item.name, matter_number)

        entity.add("name", item.name)
        entity.add("alias", item.aliases)
        entity.add("previousName", item.previous_names)
        entity.add("abbreviation", item.abbreviations)
        entity.add("sector", financial_institution)
        entity.add("topics", "reg.action")
        entity.add("country", "us")

        sanction = h.make_sanction(context, entity, key=matter_number)
        sanction.add("authorityId", matter_number)
        sanction.set("sourceUrl", detail_url)
        h.apply_date(sanction, "listingDate", sanction_date)

        context.emit(entity)
        context.emit(sanction)
    context.audit_data(str_row)


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, cache_days=1, absolute_links=True)
    table = h.xpath_element(doc, ".//table")

    for row in h.parse_html_table(table):
        crawl_row(context, row)

    assert_all_accepted(context, raise_on_unaccepted=False)
