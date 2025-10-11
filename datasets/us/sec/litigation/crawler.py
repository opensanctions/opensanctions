import re
from time import sleep
from typing import List, Literal, Optional

from lxml.html import HtmlElement, fromstring, tostring
from pydantic import BaseModel, Field
from rigour.mime.types import HTML
from zavod.context import Context
from zavod.shed import enforcements
from zavod.shed.gpt import DEFAULT_MODEL, run_typed_text_prompt
from zavod.stateful.review import (
    HtmlSourceValue,
    assert_all_accepted,
    review_extraction,
)

from zavod import helpers as h

# Ensure never more than 10 requests per second
# https://www.sec.gov/about/privacy-information#security
SLEEP = 0.1
HEADERS = {
    "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
    "cache-control": "no-cache",
    "pragma": "no-cache",
    "priority": "u=0, i",
    "sec-ch-ua": '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "none",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": "OpenSanctions tech@opensanctions.org",
}


CRAWLER_VERSION = 1
# e.g. lr-25757
# Sometimes without the first dash
# Sometimes with a letter at the end
# sometimes with -0 etc at the end
REGEX_RELEASE_ID = re.compile(r"^lr-?(\d{4,}\w*(?:-\d+)?)$")


Schema = Literal["Person", "Company", "LegalEntity"]
Status = Literal[
    "Filed",
    "Dismissed",
    "Settled",
    "Default judgement",
    "Final judgement",
    "Final consent judgement",
    "Consent judgement",
    "Supplemental consent order",
    "Other",
]

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


class Defendant(BaseModel):
    entity_schema: Schema = schema_field
    name: str
    aliases: List[str] = []
    address: List[str] = address_field
    country: List[str] = []
    status: Status = status_field
    notes: Optional[str] = notes_field


class Defendants(BaseModel):
    defendants: List[Defendant]


PROMPT = f"""
Extract the defendants subject to litigation announced in the attached article.
NEVER include relief defendants.
NEVER infer, assume, or generate values that are not directly stated in the source text.

Specific fields:

- entity_schema: {schema_field.description}
- address: {address_field.description}
- country: Any countries the entity is indicated to reside, operate, or have been born or registered in.
- status: {status_field.description}
- notes: {notes_field.description}
"""


def strip_non_body_content(article_element: HtmlElement) -> None:
    for el in article_element.xpath(
        ".//*[text()='U.S. SECURITIES AND EXCHANGE COMMISSION']"
    ):
        el.getparent().remove(el)


def get_title(url: str, article_element: HtmlElement) -> str:
    titles = article_element.xpath(".//h1[contains(@class, 'page-title__heading')]")
    assert len(titles) == 1, (len(titles), url)
    return titles[0].text_content()


def get_case_numbers(article_element: HtmlElement) -> List[str]:
    case_numbers = []
    for h3 in article_element.xpath(".//h3"):
        h3_text = h3.text_content().strip()
        if "No." in h3_text and "v." in h3_text:
            case_numbers.append(h3_text)
    return case_numbers


def get_release_id(url: str) -> str:
    path_suffix = url.split("/")[-1]
    match = REGEX_RELEASE_ID.search(path_suffix)
    assert match, f"Invalid release ID: {path_suffix}"
    return match.group(1)


def crawl_release(
    context: Context, date: str, url: str, see_also_urls: List[str]
) -> None:
    sleep(SLEEP)
    doc = context.fetch_html(url, headers=HEADERS, cache_days=15, absolute_links=True)
    article_xpath = (
        ".//div[contains(@class, 'node-details-layout__main-region__content')]"
    )
    article_element = doc.xpath(article_xpath)[0]
    case_numbers = get_case_numbers(article_element)
    strip_non_body_content(article_element)
    article_html = tostring(article_element, pretty_print=True, encoding="unicode")
    release_id = get_release_id(url)
    source_value = HtmlSourceValue(
        key_parts=release_id,
        label="Litigation Release",
        element=article_element,
        url=url,
    )
    prompt_result = run_typed_text_prompt(context, PROMPT, article_html, Defendants)
    review = review_extraction(
        context,
        crawler_version=CRAWLER_VERSION,
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
        entity.add("alias", item.aliases, origin=review.origin)
        entity.add("topics", "reg.action")

        # Distinct actions should be distinct sanctions, while different releases
        # about the same action should end up in the same sanction. We use the list of
        # case numbers for this. There is normally only one case number per release,
        # but sometimes more, in which case sanction id might not be consistent between releases.
        # The id might also differ if the entity id differs based on differing address details
        # between releases.
        sanction = h.make_sanction(context, entity, key=case_numbers)
        h.apply_date(sanction, "date", date)
        sanction.add("sourceUrl", url)
        sanction.add("sourceUrl", see_also_urls)
        sanction.add("status", item.status, origin=review.origin)
        sanction.add("summary", item.notes, origin=review.origin)
        sanction.add("authorityId", release_id)

        article = h.make_article(
            context, url, title=get_title(url, doc), published_at=date
        )
        documentation = h.make_documentation(context, entity, article)

        context.emit(entity)
        context.emit(sanction)
        context.emit(article)
        context.emit(documentation)


def crawl_index_page(context: Context, doc) -> bool:
    """Returns false if we should stop crawling."""
    table_xpath = ".//table[contains(@class, 'views-view-table')]"
    tables = doc.xpath(table_xpath)
    assert len(tables) == 1
    for row in h.parse_html_table(tables[0]):
        enforcement_date = row["date_sort_descending"].text_content().strip()
        if not enforcements.within_max_age(context, enforcement_date):
            return False
        action_cell = row["respondents"]
        action_link_xpath = (
            ".//div[contains(@class, 'release-view__respondents')]//a/@href"
        )
        urls = action_cell.xpath(action_link_xpath)
        assert len(urls) == 1
        url = urls[0]
        if "see also" in action_cell.text_content().lower():
            see_also_xpath = ".//span[contains(@class, 'media--view-mode-release-see-also-files')]//a/@href"
            see_also_urls = action_cell.xpath(see_also_xpath)
            assert len(see_also_urls) > 0
        else:
            see_also_urls = []
        crawl_release(context, enforcement_date, url, see_also_urls)
    return True


def crawl(context: Context) -> None:
    next_url: Optional[str] = context.data_url
    while next_url:
        context.log.info("Crawling index page", url=next_url)
        sleep(SLEEP)
        doc = context.fetch_html(next_url, headers=HEADERS, absolute_links=True)
        next_urls = doc.xpath(
            ".//a[contains(@class, 'usa-pagination__next-page')]/@href"
        )
        assert len(next_urls) <= 1
        if next_urls:
            next_url = next_urls[0]
        else:
            next_url = None
        if not crawl_index_page(context, doc):
            break

    assert_all_accepted(context)
