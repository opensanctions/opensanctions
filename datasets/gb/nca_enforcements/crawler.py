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

NAME_XPATH = "//h1[@itemprop='headline']/text()"
CONTENT_XPATH = "//div[@itemprop='articleBody']"
DATE_XPATH = "//div[@itemprop='articleBody']/p[strong][last()]/strong/text()"
TOPIC_XPATH = "//li[@itemprop='keywords']/a/text()"

Schema = Literal["Person", "Company", "LegalEntity"]

schema_field = Field(
    description="Use LegalEntity if it isn't clear whether the entity is a person or a company."
)
address_field = Field(
    default=[],
    description=("The addresses or even just the districts/states of the defendant."),
)


class Offender(BaseModel):
    entity_schema: Schema = schema_field
    name: str
    address: List[str] = address_field
    country: List[str] = []


class Offenders(BaseModel):
    defendants: List[Offender]


PROMPT = f"""
Extract the offenders or entities subject to UK National Crime Agency (NCA) enforcement actions mentioned in the attached article.
NEVER include victims, witnesses, investigators, or enforcement officers.
NEVER infer, assume, or generate values that are not directly stated in the source text.
If the name refers to an individual person, use Person as the entity_schema.

Specific fields:

- name: The name of the entity precisely as expressed in the text.
- entity_schema: {schema_field.description}
- address: {address_field.description}
- country: Any countries the entity is indicated to reside, operate, or have been born or registered in. Leave empty if not explicitly stated.
"""


def crawl_enforcement_action(context: Context, url: str) -> None:
    article = context.fetch_html(url, cache_days=7, absolute_links=True)
    if article is None:
        return
    article_name = article.xpath(NAME_XPATH)[0]
    article_content = article.xpath(CONTENT_XPATH)
    assert len(article_content) == 1
    article_element = article_content[0]
    date = article.xpath(DATE_XPATH)
    topic = article.xpath(TOPIC_XPATH)
    if topic != []:
        for t in topic:
            res = context.lookup("topics", t.strip())
            if res is not None:
                topic = res.value
            else:
                context.log.warning("Lookup not found", topic=t.strip())
    if date == []:
        context.log.warning("Date not found in article", url=url)
        return
    date = date[0]

    source_value = HtmlSourceValue(
        key_parts=url,
        label="Enforcement Action Notice",
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

    for item in review.extracted_data.defendants:
        entity = context.make(item.entity_schema)
        entity.id = context.make_id(item.name, item.address, item.country)
        entity.add("name", item.name, origin=review.origin)
        if item.address != item.country:
            entity.add("address", item.address, origin=review.origin)
        entity.add("country", item.country, origin=review.origin)
        entity.add("topics", "reg.action")

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
    page = 0
    while page < 20:
        base_url = f"https://www.nationalcrimeagency.gov.uk/news?start={page * 16}"
        context.log.info("Crawling index page", url=base_url)
        doc = context.fetch_html(base_url, absolute_links=True, cache_days=5)
        links = doc.xpath(
            "//div[@class='blog news-page']/div[@class='row-fluid']//div[@class='page-header']//a/@href"
        )
        if not links:
            break
        assert links is not None, "No links found on the index page"
        for link in links:
            crawl_enforcement_action(context, link)
        page += 1

    assert_all_accepted(context)
