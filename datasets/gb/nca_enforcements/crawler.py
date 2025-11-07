from typing import List, Literal

from pydantic import BaseModel, Field
from zavod.shed.gpt import DEFAULT_MODEL, run_typed_text_prompt
from zavod.stateful.review import (
    HtmlSourceValue,
    assert_all_accepted,
    review_extraction,
)

from zavod import Context
from zavod import helpers as h

NAME_XPATH: str = "//h1[@itemprop='headline']/text()"
CONTENT_XPATH: str = "//div[@itemprop='articleBody']"
DATE_XPATH: str = "//div[@itemprop='articleBody']/p[strong][last()]/strong/text()"
TOPIC_XPATH: str = "//li[@itemprop='keywords']/a/text()"

Schema = Literal["Person", "Company", "LegalEntity"]

schema_field = Field(
    description="Use LegalEntity if it isn't clear whether the entity is a person or a company."
)


class Offender(BaseModel):
    entity_schema: Schema = schema_field
    name: str
    address: List[str] = []
    country: List[str] = []


class Offenders(BaseModel):
    offenders: List[Offender]


PROMPT = f"""
Extract the offenders or entities subject to UK National Crime Agency (NCA) enforcement actions mentioned in the article.
NEVER include victims, witnesses, investigators, or enforcement officers.
NEVER infer, assume, or generate values that are not directly stated in the source text.
If the name refers to an individual person, use Person as the entity_schema.

Specific fields:

- name: The name of the entity precisely as expressed in the text.
- entity_schema: {schema_field.description}
- address: The locations associated with the offender, such as their home address, town, city, or region."
- country: Any countries the entity is indicated to reside, operate, or have been born or registered in. Leave empty if not explicitly stated.
"""


def crawl_enforcement_action(context: Context, url: str) -> None:
    article_doc = context.fetch_html(url, cache_days=7, absolute_links=True)
    if article_doc is None:
        return
    article_name = article_doc.xpath(NAME_XPATH)[0]
    assert article_name, "Article name not found"
    article_content = article_doc.xpath(CONTENT_XPATH)
    assert len(article_content) == 1
    article_element = article_content[0]
    date = article_doc.xpath(DATE_XPATH)
    # The date is absent in some articles
    if date != []:
        date = date[0]
    # Extract topics and look them up
    raw_topics: List[str] = article_doc.xpath(TOPIC_XPATH)
    resolved_topics: List[str] = []
    for topic in raw_topics:
        topic_clean = topic.strip()
        res = context.lookup("topics", topic_clean)
        if res is not None:
            resolved_topics.append(res.value)
        else:
            context.log.warning("Topic lookup not found", topic=topic_clean)

    source_value = HtmlSourceValue(
        key_parts=url,
        label="Press Release",
        element=article_element,
        url=url,
    )
    prompt_result = run_typed_text_prompt(
        context, PROMPT, source_value.value_string, Offenders
    )
    review = review_extraction(
        context,
        source_value=source_value,
        original_extraction=prompt_result,
        origin=DEFAULT_MODEL,
    )

    if not review.accepted:
        return

    for item in review.extracted_data.offenders:
        entity = context.make(item.entity_schema)
        entity.id = context.make_id(item.name, str(item.address), str(item.country))
        entity.add("name", item.name, origin=review.origin)
        if item.address != item.country:
            entity.add("address", item.address, origin=review.origin)
        entity.add("country", item.country, origin=review.origin)
        for topic in resolved_topics:
            entity.add("topics", topic)

        # We use the date as a key to make sure notices about separate actions are separate sanction entities
        sanction = h.make_sanction(context, entity, date)
        h.apply_date(sanction, "date", date)
        sanction.set("sourceUrl", url)

        article = h.make_article(context, url, title=article_name, published_at=date)
        documentation = h.make_documentation(context, entity, article)

        context.emit(entity)
        context.emit(sanction)
        context.emit(article)
        context.emit(documentation)


def crawl(context: Context):
    page: int = 0
    base_url: str = "https://www.nationalcrimeagency.gov.uk/news?start={offset}"

    while True:
        index_url = base_url.format(offset=page * 16)
        doc = context.fetch_html(index_url, absolute_links=True, cache_days=5)
        links = doc.xpath(
            "//div[@class='blog news-page']/div[@class='row-fluid']//div[@class='page-header']//a/@href"
        )
        if links == [] or len(links) == 0:
            context.log.info("No more links found, stopping crawl")
            break
        for link in links:
            crawl_enforcement_action(context, link)
        if page >= 200:  # Limit to 200 pages to avoid infinite loops
            break
        page += 1

    assert_all_accepted(context)
