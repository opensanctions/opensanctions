from pydantic import BaseModel, Field
import re
from typing import Optional, List, Literal
from zavod.entity import Entity
from zavod.shed import enforcements

from lxml.html import HtmlElement, fromstring, tostring

from zavod import Context
from zavod import helpers as h
from zavod.shed.gpt import DEFAULT_MODEL, run_typed_text_prompt
from zavod.stateful.review import (
    Review,
    assert_all_accepted,
    request_review,
    get_review,
    model_hash,
    html_to_text_hash,
)

Schema = Literal["Person", "Company", "LegalEntity"]
Status = Literal[
    "Filed",
    "Dismissed",
    "Settled",
    "Default judgement",
    "Final judgement",
    "Supplemental consent order",
    "Other",
]

MODEL_VERSION = 1
MIN_MODEL_VERSION = 1

REGEX_RELEASE_ID = re.compile(r"(\w{2,8}-\w{2,4}[\w #-]*)$")

something_changed = False

schema_field = Field(
    description="Use LegalEntity if it isn't clear whether the entity is a person or a company."
)
address_field = Field(
    default=[],
    description=("The addresses or even just the districts/states of the defendant."),
)
status_field = Field(
    description=(
        "The status of the enforcement action notice."
        " If `Other`, add the text used as the status in the source to `notes`."
    )
)
notes_field = Field(default=None, description=("Only used if `status` is `Other`."))
original_press_release_number_field = Field(
    description=(
        "The original press release number of the enforcement action notice."
        " When announcing charges, this is the press release number of the"
        " announcement. When announcing court orders or dropped charges,"
        " this is the reference to the original press release."
    )
)


class RelatedCompany(BaseModel):
    name: str
    relationship: str


class Defendant(BaseModel):
    entity_schema: Schema = schema_field
    name: str
    aliases: List[str] = []
    address: str | List[str] | None = address_field
    country: str | List[str] | None = []
    status: Status = status_field
    notes: Optional[str] = notes_field
    original_press_release_number: Optional[str] = original_press_release_number_field
    related_companies: List[RelatedCompany] = []


class Defendants(BaseModel):
    defendants: List[Defendant]


PROMPT = f"""
Extract the defendants or entities subject to OFAC enforcement actions in the attached article.
NEVER include relief defendants.
NEVER infer, assume, or generate values that are not directly stated in the source text.

Trading/D.B.A. names which follow a person name but look like company can just be
aliases of the person. If the name is a person name, use `Person` as the entity_schema.

Specific fields:

- entity_schema: {schema_field.description}
- address: {address_field.description}
- country: Any countries explicitly associated with the defendant in the text. Leave empty if not explicitly stated.
- status: {status_field.description}
- notes: {notes_field.description}
- original_press_release_number: {original_press_release_number_field.description}
- related_companies: If the defendant is a person and a related company is mentioned in the source text, add it here.
    - relationship: Use text verbatim from the source. If it's ambiguous, e.g. "agents and owners", use that text exactly as it is, plural and all.
"""


def make_related_company(context: Context, name: str) -> Entity:
    entity = context.make("Company")
    entity.id = context.make_id(name)
    entity.add("name", name)
    return entity


def make_company_link(
    context: Context, entity: Entity, related_company: Entity, relationship: str
) -> Entity:
    link = context.make("UnknownLink")
    link.id = context.make_id("Related company", entity.id, related_company.id)
    link.add("subject", entity)
    link.add("object", related_company)
    link.add("role", relationship)
    return link


def source_changed(review: Review, article_element: HtmlElement) -> bool:
    """
    The key exists but the current source data looks different from the existing version
    in spite of heavy normalisation.
    """
    seen_element = fromstring(review.source_value)
    return html_to_text_hash(seen_element) != html_to_text_hash(article_element)


def check_something_changed(
    context: Context,
    review: Review,
    article_html: str,
    article_element: HtmlElement,
) -> bool:
    """
    Returns True if the source content has changed.

    In that case it also reprompts to log whether the extracted data has changed.
    """
    if source_changed(review, article_element):
        prompt_result = run_typed_text_prompt(context, PROMPT, article_html, Defendants)
        if model_hash(prompt_result) == model_hash(review.orig_extraction_data):
            context.log.warning(
                "The source content has changed but the extracted data has not",
                url=review.source_url,
                seen_source_value=review.source_value,
                new_source_value=article_html,
            )
        else:
            # A new extraction result looks different from the known original extraction
            context.log.warning(
                "The extracted data has changed",
                url=review.source_url,
                orig_extracted_data=review.orig_extraction_data.model_dump(),
                prompt_result=prompt_result.model_dump(),
            )
        return True
    else:
        return False


def crawl_enforcement_action(context: Context, date: str, url: str) -> None:
    article = context.fetch_html(url, cache_days=1)
    if article is None:
        return
    article_html = tostring(article, pretty_print=True).decode("utf-8")
    name = article.xpath("//span[@class='treas-page-title']/text()")
    article_text = article.xpath(
        "//div[@id='block-ofac-content']//div[@class='field__item']/p"
    )
    source_pdf = article.xpath(
        "//div[@id='block-ofac-content']//a[@data-entity-type='media']/@href"
    )
    assert all([name, article_text, source_pdf]), "One or more fields are empty"

    review = get_review(context, Defendants, name, MIN_MODEL_VERSION)
    if review is None:
        prompt_result = run_typed_text_prompt(
            context, PROMPT, article_text[0], Defendants
        )
        review = request_review(
            context,
            name[0],
            article_text[0],
            "text",
            "Enforcement Action Notice",
            url,
            prompt_result,
            MODEL_VERSION,
        )

    if check_something_changed(context, review, article_html, article):
        # In the first iteration, we're being super conservative and rejecting
        # export if the source content has changed regardless of whether the
        # extraction result has changed. If we see this happening and we see that
        # the extraction result reliably identifies real data changes, we can
        # relax this to only reject if the extraction result has changed.

        # Similarly if we see that broad markup changes don't trigger massive
        # re-reviews but legitimate changes are reliably detected, we can allow
        # it to automatically request re-reviews upon extraction changes.
        global something_changed
        something_changed = True
        return

    if not review.accepted:
        return

    for item in review.extracted_data.defendants:
        entity = context.make(item.entity_schema)
        entity.id = context.make_id(item.name, item.address, item.country)
        entity.add("name", item.name, origin=DEFAULT_MODEL)
        if item.address != item.country:
            entity.add("address", item.address, origin=DEFAULT_MODEL)
        entity.add("country", item.country, origin=DEFAULT_MODEL)
        entity.add("alias", item.aliases, origin=DEFAULT_MODEL)
        entity.add("topics", "reg.action")

        # We try to link press releases that refer to an original press release number
        # back to the original press release by using that in the sanction key.

        # In practice often the entity ID and thus sanction ID differs because of
        # different levels of address details in the press releases.
        sanction = h.make_sanction(
            context,
            entity,
            key=item.original_press_release_number,
        )
        h.apply_date(sanction, "date", date)
        sanction.set("sourceUrl", url)
        sanction.add("status", item.status, origin=DEFAULT_MODEL)
        sanction.add("summary", item.notes, origin=DEFAULT_MODEL)
        sanction.add(
            "authorityId", item.original_press_release_number, origin=DEFAULT_MODEL
        )

        for related_company in item.related_companies:
            related_company_entity = make_related_company(context, related_company.name)
            link = make_company_link(
                context, entity, related_company_entity, related_company.relationship
            )
            context.emit(related_company_entity)
            context.emit(link)

        # article = h.make_article(
        #     context, url, title=get_title(article_element), published_at=date
        # )
        # documentation = h.make_documentation(context, entity, article, date=date)

        context.emit(entity)
        context.emit(sanction)
        # context.emit(article)
        # context.emit(documentation)


def crawl_index_page(context: Context, link: str):
    doc = context.fetch_html(link, cache_days=1)
    name = doc.xpath("//span[@class='treas-page-title']/text()")
    enforcement_date = doc.xpath(
        "//div[@id='block-ofac-content']//div[@class='field__item']/text()"
    )
    text = doc.xpath(
        "//div[@id='block-ofac-content']//div[@class='field__item']/p/text()"
    )
    source_pdf = doc.xpath(
        "//div[@id='block-ofac-content']//a[@data-entity-type='media']/@href"
    )
    assert all(
        [name, enforcement_date, text, source_pdf]
    ), "One or more fields are empty"

    if not enforcements.within_max_age(context, enforcement_date[0]):
        return False

    crawl_enforcement_action(context, enforcement_date[0], link)
    return True


def crawl(context: Context):
    doc = context.fetch_html(context.data_url, cache_days=1)
    doc.make_links_absolute(context.data_url)
    links = doc.xpath(
        "//div[@class='view-content']//a[contains(@href, 'recent-actions') and not(contains(@href, 'enforcement-actions'))]/@href"
    )
    for link in links:
        if not crawl_index_page(context, link):
            break

    # assert_all_accepted(context)
