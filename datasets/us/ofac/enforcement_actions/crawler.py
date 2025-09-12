from datetime import date
from lxml.html import HtmlElement, fromstring, tostring
from pydantic import BaseModel, Field
from rigour.mime.types import HTML
from typing import Optional, List, Literal

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.shed import enforcements
from zavod.shed.gpt import DEFAULT_MODEL, run_typed_text_prompt
from zavod.stateful.review import (
    Review,
    assert_all_accepted,
    request_review,
    get_review,
    model_hash,
)

NAME_XPATH = "//span[@class='treas-page-title']/text()"
CONTENT_XPATH = "//div[@id='block-ofac-content']//div[@class='content']"
DATE_XPATH = "//div[@id='block-ofac-content']//div[@class='field__item']/text()"
# Notices issued before 5 July 2016 are PDFs
MAX_AGE_DAYS = (date.today() - date(2016, 7, 8)).days

Schema = Literal["Person", "Company", "LegalEntity"]
Status = Literal[
    "Settled",
    "Filed",
    "Dismissed",
    "Final judgement",
    "Other",
]

# Possible variations of enforcement action types found in source data:
#
# Settlement Agreement
# Penalty Notice to an Individual
# Finding of Violation
# Sanctions Compliance Guidance
# Issuance of Sanctions Regulations
# Civil Monetary Penalty
# Release of OFAC Enforcement Information
# Release​ of Civil Penalties Information
# Recent OFAC Actions

MODEL_VERSION = 1
MIN_MODEL_VERSION = 1

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


class RelatedCompany(BaseModel):
    name: str
    relationship: str


class Defendant(BaseModel):
    entity_schema: Schema = schema_field
    name: str
    aliases: List[str] = []
    address: List[str] = address_field
    country: List[str] = []
    status: Status = status_field
    notes: Optional[str] = notes_field
    related_companies: List[RelatedCompany] = []
    pdf_url: List[str] = []


class Defendants(BaseModel):
    defendants: List[Defendant]


PROMPT = f"""
Extract the defendants or entities subject to OFAC enforcement actions in the attached article.
NEVER include relief defendants.
NEVER infer, assume, or generate values that are not directly stated in the source text.
If the name is a person name, use `Person` as the entity_schema.

Specific fields:

- name: The name of the entity precisely as expressed in the text. If an acronym follows the name in parentheses, include it as an alias and not as part of the name.
- entity_schema: {schema_field.description}
- address: {address_field.description}
- country: Any countries the entity is indicated to reside, operate, or have been born or registered in. Leave empty if not explicitly stated.
- status: {status_field.description}
- notes: {notes_field.description}
- related_companies: If the defendant has an ownership or controlling relationship with the related entity, add it here.
- relationship: Use text verbatim from the source. If it's ambiguous, e.g. "agents and owners", use that text exactly as it is, plural and all.
- pdf_url: The PDF URL exactly as written in the source text.
  • If multiple PDFs are present, associate each with the correct entity if the source makes that link explicit.
  • If no PDF is mentioned for an entity, leave this field empty.
  • Never invent or infer a URL.
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
    return h.element_text_hash(seen_element) != h.element_text_hash(article_element)


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


def crawl_enforcement_action(context: Context, url: str) -> None:
    article = context.fetch_html(url, cache_days=7)
    article.make_links_absolute(context.data_url)
    if article is None:
        return
    article_name = article.xpath(NAME_XPATH)[0]
    article_content = article.xpath(CONTENT_XPATH)
    assert len(article_content) == 1
    article_element = article_content[0]
    date = article.xpath(DATE_XPATH)[0]
    article_html = tostring(article_element, pretty_print=True, encoding="unicode")
    assert all([article_name, article_html, date]), "One or more fields are empty"

    review = get_review(context, Defendants, url, MIN_MODEL_VERSION)
    if review is None:
        prompt_result = run_typed_text_prompt(context, PROMPT, article_html, Defendants)
        review = request_review(
            context,
            url,
            article_html,
            HTML,
            "Enforcement Action Notice",
            url,
            prompt_result,
            MODEL_VERSION,
        )
    if check_something_changed(context, review, article_html, article_element):
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

        # We use the date as a key to make sure notices about separate actions are separate sanction entities
        sanction = h.make_sanction(context, entity, date)
        h.apply_date(sanction, "date", date)
        sanction.set("sourceUrl", url)
        sanction.add("sourceUrl", item.pdf_url, origin=DEFAULT_MODEL)
        sanction.add("status", item.status, origin=DEFAULT_MODEL)
        sanction.add("summary", item.notes, origin=DEFAULT_MODEL)

        for related_company in item.related_companies:
            related_company_entity = make_related_company(context, related_company.name)
            link = make_company_link(
                context, entity, related_company_entity, related_company.relationship
            )
            context.emit(related_company_entity)
            context.emit(link)

        article = h.make_article(context, url, title=article_name, published_at=date)
        documentation = h.make_documentation(context, entity, article)

        context.emit(entity)
        context.emit(sanction)
        context.emit(article)
        context.emit(documentation)


def crawl(context: Context):
    page = 0
    within_age_limit = True
    while within_age_limit:
        base_url = (
            f"https://ofac.treasury.gov/recent-actions/enforcement-actions?page={page}"
        )
        context.log.info("Crawling index page", url=base_url)
        doc = context.fetch_html(base_url, absolute_links=True)
        links = doc.xpath(
            "//div[@class='view-content']//a[contains(@href, 'recent-actions') and not(contains(@href, 'enforcement-actions'))]/@href"
        )
        if not links:
            break
        search_results = doc.findall(".//div[contains(@class, 'search-result')]")
        for result in search_results:
            enforcement_date = result.xpath(
                ".//div[contains(@class,'margin-top-1') and contains(., 'Enforcement Actions')]/text()[normalize-space()]"
            )
            assert len(enforcement_date) == 1, "Expected exactly one enforcement date"
            clean_date = enforcement_date[0].strip().removesuffix(" -")
            if not enforcements.within_max_age(context, clean_date, MAX_AGE_DAYS):
                within_age_limit = False
                break
            link = result.xpath(
                ".//a[contains(@href, 'recent-actions') or contains(@href, 'recent-issues') and not(contains(@href, 'enforcement-actions'))]/@href"
            )[0]
            # Extract the first link to the enforcement action (or "recent-issues").
            # Skip one known duplicate case of "https://ofac.treasury.gov/recent-actions/20181127_33" under /recent-issues.
            # For any other "recent-issues" links, log a warning so we can review them later.
            if link == "https://ofac.treasury.gov/recent-issues/20181127_33":
                continue
            if "recent-issues" in link:
                context.log.warn(
                    f"Double check recent-issues link for possible duplicates: {link}"
                )
            crawl_enforcement_action(context, link)
        page += 1

    assert_all_accepted(context)
    global something_changed
    assert not something_changed, (
        "See what changed to determine whether to trigger re-review."
    )
