from lxml.etree import _Element
from typing import List
from pydantic import BaseModel

from zavod import Context, helpers as h
from rigour.names.split_phrases import contains_split_phrase
from zavod.shed import enforcements
from zavod.extract.llm import run_typed_text_prompt, DEFAULT_MODEL
from zavod.stateful.review import (
    review_extraction,
    HtmlSourceValue,
    assert_all_accepted,
)


MAX_AGE_DAYS = 365 * 10  # keep 10 years of history


class Respondent(BaseModel):
    name: str
    aliases: List[str] = []
    previous_names: List[str] = []


class Respondents(BaseModel):
    respondents: List[Respondent]


PROMPT = """
Extract the entity name(s) from the string and its aliases if any, following the schema provided.
ONLY include names mentioned in the input string. A string might contain multiple entities,
record each entity with its data in a separate {Respondent} class, so that if a string contains multiple entities,
they are listed as separate objects in {Respondents}.

- name: The name of the entity precisely as expressed in the text.
- aliases: ONLY extract aliases that follow an explicit indication of an _alternative_ name, such as "also known as", "alias", "formerly", "aka", "fka". Otherwise the aliases field should just be an empty array.
- previous_names: ONLY extract previous names that follow an explicit indication of a _former_ name, such as "formerly", "fka". Otherwise the previous_names field should just be an empty array.
"""


def crawl_row(context: Context, row: dict[str, _Element]) -> None:
    # fetch case's url
    url_el = row.get("enforcement_action")
    assert url_el is not None
    detail_url = h.xpath_strings(url_el, ".//a/@href")

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

    needs_review = contains_split_phrase(case_name) or " and " in case_name

    if needs_review:
        source_value = HtmlSourceValue(
            key_parts=case_name,
            label="Case Name",
            element=url_el,
            url=context.data_url,
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
        extrated_data = review.extracted_data
        origin = review.origin
    else:
        extrated_data = Respondents(respondents=[Respondent(name=case_name)])
        origin = None

    for item in extrated_data.respondents:
        entity = context.make("LegalEntity")
        entity.id = context.make_id(item.name, matter_number)

        # check name irregularity (e.g. [UPDATED...]) and send to review
        original = h.Names(name=item.name)
        is_irregular, suggested = h.check_names_regularity(entity, original)

        # another review will be created if standard heuristics suggest the name is irregular,
        # or if there is a custom suggestion that differs from the original categorisation.
        h.review_names(
            context,
            entity,
            original=original,
            suggested=suggested,
            is_irregular=is_irregular,
        )
        # TODO: once we're done with reviews -- change to apply_reviewed_names()

        entity.add("name", item.name, origin=origin)
        entity.add("alias", item.aliases, origin=origin)
        entity.add("previous_name", item.previous_names, origin=origin)
        entity.add("sourceUrl", detail_url)
        entity.add("sector", str_row.pop("financial_institution"))
        entity.add("topics", "reg.action")
        entity.add("country", "us")

        sanction = h.make_sanction(context, entity, key=matter_number)
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
