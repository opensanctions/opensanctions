from lxml.etree import _Element
from datetime import date
from typing import List
from pydantic import BaseModel

from zavod import Context, helpers as h
from rigour.names.split_phrases import contains_split_phrase
from zavod.shed import enforcements
from zavod.extract.llm import run_typed_text_prompt, DEFAULT_MODEL
from zavod.stateful.review import review_extraction, HtmlSourceValue

start = date.today()
end = date(start.year + 10, start.month, start.day)
MAX_AGE_DAYS = (end - start).days


class Respondent(BaseModel):
    name: str
    aliases: List[str] = []


class Respondents(BaseModel):
    respondents: List[Respondent]


PROMPT = """
Extract the entity name(s) from the string and its aliases if any, following the schema provided.
ONLY include names mentioned in the input string.

- name: The name of the entity precisely as expressed in the text.
- aliases: ONLY extract aliases that follow an explicit indication of an _alternative_ name, such as "also known as", "alias", "formerly", "aka", "fka". Otherwise the aliases field should just be an empty array.
"""


def crawl_row(context: Context, row: dict[str, _Element]) -> None:
    # fetch case's url
    url_el = row.get("enforcement_action")
    assert url_el is not None
    url = h.xpath_strings(url_el, ".//a/@href")

    # process row
    str_row = h.cells_to_str(row)
    case_name = str_row.pop("enforcement_action")
    sanction_date = str_row.pop("date_sort_ascending")
    if sanction_date and not enforcements.within_max_age(
        context, sanction_date, MAX_AGE_DAYS
    ):
        return

    assert case_name is not None
    if "in the matter of" in case_name.lower():
        case_name = case_name.split("In the Matter of")[1].strip()

    entity = context.make("LegalEntity")
    entity.id = context.make_id(case_name, str_row.pop("matter_number"))

    # send to cleaning if a string contrains split entities and aliases
    if contains_split_phrase(case_name) or " and " in case_name:
        source_value = HtmlSourceValue(
            key_parts=url,
            label="FinCEN Enforcement Action",
            element=url_el,
            url=url[0],
        )
        prompt_result = run_typed_text_prompt(
            context,
            prompt=PROMPT,
            string=case_name,
            response_type=Respondents,
        )
        review = review_extraction(
            context,
            source_value=source_value,
            original_extraction=prompt_result,
            origin=DEFAULT_MODEL,
        )
        if not review.accepted:
            return

        for item in review.extracted_data.respondents:
            entity.add("name", item.name, origin=review.origin)
            entity.add("alias", item.aliases, origin=review.origin)
        # original = h.Names(name=case_name)
        # h.review_names(context, entity, original=original, llm_cleaning=True)
    else:
        entity.add("name", case_name)

    entity.add("sourceUrl", url)
    entity.add("sector", str_row.pop("financial_institution"))
    entity.add("topics", "reg.action")
    entity.add("country", "us")

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "listingDate", sanction_date)

    context.emit(entity)
    context.emit(sanction)
    context.audit_data(str_row)


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, cache_days=1, absolute_links=True)
    table = h.xpath_element(doc, ".//table")

    for row in h.parse_html_table(table):
        crawl_row(context, row)

    # assert_all_accepted(context, raise_on_unaccepted=False)
