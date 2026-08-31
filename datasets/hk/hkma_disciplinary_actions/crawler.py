from typing import Literal

from pydantic import BaseModel, Field

from zavod import Context
from zavod import helpers as h
from zavod.extract.llm import DEFAULT_MODEL, run_typed_text_prompt
from zavod.stateful.review import (
    TextSourceValue,
    assert_all_accepted,
    review_extraction,
)
from zavod.util import Element

name_field = Field(
    description=(
        "The full name of the institution as the source writes it, minus the "
        "abbreviation. Keep parentheses that belong to the registered name, e.g. "
        "'Fubon Bank (Hong Kong) Limited'. Drop trailing punctuation left over "
        "from the source's formatting."
    )
)
alias_field = Field(
    description=(
        "Any other complete name the source gives for the same institution, such "
        "as a former name or a name in another language. Empty when the source "
        "gives only one name."
    )
)
weak_alias_field = Field(
    description=(
        "Name forms contained in the listed name that are too generic to identify "
        "this institution on their own. When the name is a branch, include the "
        "institution it is a branch of, e.g. 'Commerzbank AG' for 'Commerzbank AG, "
        "Hong Kong Branch'. Otherwise empty. Never shorten a name that is not a "
        "branch."
    )
)
abbreviation_field = Field(
    description=(
        "The abbreviation the press release uses for this institution, which the "
        "source appends to the name in a trailing bracket, e.g. 'IOBHK' in "
        "'Indian Overseas Bank, Hong Kong Branch (IOBHK)'. Null when the source "
        "gives no abbreviation."
    )
)


class Institution(BaseModel):
    name: str = name_field
    alias: list[str] = alias_field
    weak_alias: list[str] = weak_alias_field
    abbreviation: str | None = abbreviation_field


class Institutions(BaseModel):
    institutions: list[Institution]


PROMPT = f"""
Extract the institutions disciplined by the Hong Kong Monetary Authority from the
attached listing. Each line names exactly one institution. Do not merge, split,
invent or omit any of them. Only use names that appear in the listing itself.

Instructions for specific fields:

  - name: {name_field.description}
  - alias: {alias_field.description}
  - weak_alias: {weak_alias_field.description}
  - abbreviation: {abbreviation_field.description}
"""


def parse_item(context: Context, item: Element) -> None:
    date_el = h.xpath_element(item, ".//div[@class='related-information-date']")
    date = h.element_text(date_el)
    if not h.within_max_age(context, date):
        return
    link = h.xpath_element(item, ".//div[@class='related-information-text']/a")
    url = link.get("href")
    assert url is not None, h.element_text(item)
    # All institutions covered by one press release are packed into the link title,
    # separated by literal <br /> tags.
    title = link.get("title")
    assert title is not None, url

    # The trailing path segment is the HKMA's press release number, which is more
    # stable than the full URL if the site is ever reorganised.
    notice_id = url.rstrip("/").rsplit("/", 1)[-1]
    source_value = TextSourceValue(
        key_parts=notice_id,
        label="Institutions named in the disciplinary action",
        text=title,
        url=url,
    )
    extraction = run_typed_text_prompt(
        context=context,
        prompt=PROMPT,
        string=title,
        response_type=Institutions,
        model=DEFAULT_MODEL,
    )
    review = review_extraction(
        context=context,
        source_value=source_value,
        original_extraction=extraction,
        origin=DEFAULT_MODEL,
    )
    if not review.accepted:
        return

    doc = context.fetch_html(url, cache_days=7)
    headline_el = h.xpath_element(doc, ".//h3[@class='press-release-title']")
    headline = h.element_text(headline_el)
    article = h.make_article(context, url, title=headline, published_at=date)
    context.emit(article)

    for institution in review.extracted_data.institutions:
        entity = context.make("Company")
        entity.id = context.make_id(institution.name)
        entity.add("name", institution.name, origin=review.origin)
        entity.add("alias", institution.alias, origin=review.origin)
        # The press release abbreviation is a name form, but only ever a weak one.
        entity.add("weakAlias", institution.weak_alias, origin=review.origin)
        entity.add("abbreviation", institution.abbreviation, origin=review.origin)
        entity.add("topics", "reg.action")
        review.link_entity(context, entity)

        sanction = h.make_sanction(context, entity, key=url)
        sanction.add("description", headline)
        sanction.add("sourceUrl", url)
        h.apply_date(sanction, "listingDate", date)

        context.emit(entity)
        context.emit(sanction)
        context.emit(h.make_documentation(context, entity, article))


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, absolute_links=True, cache_days=1)
    section = h.xpath_element(
        doc,
        ".//div[contains(@class, 'content-wrapper')]"
        "[.//div[text()='Disciplinary Actions']]",
    )
    items = h.xpath_elements(
        section, ".//li[contains(@class, 'related-information-item')]"
    )
    for item in items:
        parse_item(context, item)

    assert_all_accepted(context, raise_on_unaccepted=False)
