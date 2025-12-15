from typing import List, Literal

from pydantic import BaseModel, Field
from zavod.shed import enforcements
from zavod.extract.llm import DEFAULT_MODEL, run_typed_text_prompt
from zavod.stateful.review import (
    HtmlSourceValue,
    assert_all_accepted,
    review_extraction,
)

from zavod import Context
from zavod import helpers as h
from zavod.util import Element

Schema = Literal["Person", "Company", "LegalEntity"]

schema_field = Field(
    description="Use LegalEntity if it isn't clear whether the entity is a person or a company."
)


class Offender(BaseModel):
    entity_schema: Schema = schema_field
    name: str
    aliases: List[str] = []
    address: List[str] = []
    country: List[str] = []


class Offenders(BaseModel):
    offenders: List[Offender]


PROMPT = f"""
Extract the offenders or entities subject to UK National Crime Agency (NCA) enforcement actions mentioned in the article.
NEVER  include any entries where the name is not explicitly provided (e.g., "a man", "an individual", "two people", etc.).
NEVER include victims, witnesses, investigators, or enforcement officers.
NEVER infer, assume, or generate values that are not directly stated in the source text.
If the name refers to an individual person, use Person as the entity_schema.

Specific fields:

- entity_schema: {schema_field.description}
- name: The name of the entity precisely as expressed in the text.
- aliases: ONLY extract aliases that follow an explicit indication of an _alternative_ name, such as "also known as", "alias", "formerly", "aka", "fka". Otherwise the aliases field should just be an empty array.
- address: The full address or location details as they appear in the article (do not split into components; capture the complete expression such as “123 Main Street, Birmingham”). If no address is given for a specific entity, leave this field as an empty array.
- country: Any countries the entity is indicated to reside, operate, or have been born or registered in. Leave empty if not explicitly stated.
"""


def get_date(context: Context, url: str, article_doc: Element) -> str | None:
    # The last <p><strong> in the article body usually contains the date,
    # but some articles don't have a date at all.
    dates = article_doc.xpath(
        "//div[@itemprop='articleBody']/p[strong][last()]/strong[last()]/text()"
    )
    assert len(dates) <= 1
    if dates != []:
        raw_date = str(dates[0])
        # 03 October 2025
        if len(raw_date.split(" ")) == 3:
            # it looks like a date
            return raw_date
        else:
            context.log.info("Doesn't look like a date", url=url, raw_date=raw_date)
    return None


def crawl_enforcement_action(context: Context, url: str) -> None:
    article_doc = context.fetch_html(url, cache_days=7, absolute_links=True)
    if article_doc is None:
        return
    article_name = article_doc.xpath("//h1[@itemprop='headline']/text()")[0]
    assert article_name, "Article name not found"
    article_content = article_doc.xpath("//div[@itemprop='articleBody']")
    assert len(article_content) == 1
    article_element = article_content[0]

    # Extract topics and look them up
    raw_topics: List[str] = article_doc.xpath("//li[@itemprop='keywords']/a/text()")
    topics: List[str] = []
    for topic in raw_topics:
        res = context.lookup("topics", topic, warn_unmatched=True)
        if res is not None:
            topics.append(res.value)

    # Topics mapped to null will cause articles to be skipped during processing.
    # It's a way to filter out articles that are not relevant to OpenSanctions' scope.
    if not topics:
        return

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
        entity.add("alias", item.aliases, origin=review.origin)
        if item.address != item.country:
            entity.add("address", item.address, origin=review.origin)
        entity.add("country", item.country, origin=review.origin)
        for topic in topics:
            entity.add("topics", topic)

        raw_date = get_date(context, url, article_doc)
        if raw_date and not enforcements.within_max_age(context, raw_date):
            continue

        # We use the date as a key to make sure notices about separate actions are separate sanction entities
        sanction = h.make_sanction(context, entity, raw_date)
        h.apply_date(sanction, "date", raw_date)
        sanction.set("sourceUrl", url)

        article = h.make_article(
            context, url, title=article_name, published_at=raw_date
        )
        documentation = h.make_documentation(context, entity, article)

        context.emit(entity)
        context.emit(sanction)
        context.emit(article)
        context.emit(documentation)


def crawl(context: Context):
    index_url = context.data_url
    seen: set[str] = set()
    while index_url:
        # Avoid infinite loops in case of pagination issues
        if index_url in seen:
            context.log.warning(f"Pagination loop detected at {index_url}, stopping.")
            break
        seen.add(index_url)
        doc = context.fetch_html(index_url, absolute_links=True)
        links = doc.xpath(
            "//div[@class='blog news-page']/div[@class='row-fluid']//div[@class='page-header']//a/@href"
        )
        for link in links:
            crawl_enforcement_action(context, link)
        # Find the link to the next page (pagination)
        next_links = doc.xpath("//a[@aria-label='Go to next > page']/@href")
        assert len(next_links) <= 1
        index_url = next_links[0] if next_links else None

    assert_all_accepted(context)
