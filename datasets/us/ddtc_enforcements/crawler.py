import re
import json
from typing import Any
from lxml import html
from urllib.parse import urljoin

from zavod import Context
from zavod import helpers as h


REQUEST_TEMPLATE: dict[str, Any] = {
    "sys_id": "384b968adb3cd30044f9ff621f961941",
    "preventViewAll": True,
    "workflow_state": "published",
    "field_list": "year,company,description,charging_letter,consent_agreement,order",
    "table": "x_usd10_ddtc_publi_ddtc_public_penalty_and_oversight",
    "order": 1,
    "sys_view_count": "0",
    "d": "DESC",
    "active": True,
    "fields_array": [
        "year",
        "company",
        "description",
        "charging_letter",
        "consent_agreement",
        "order",
    ],
    "kb_knowledge_page": "ddtc_kb_article_page",
    "o": "year",
    "sp_widget_dv": "PWS DDTC Public Portal KB",
    "kb_knowledge_base": "34a3eed6db2f0300d0a370131f961983",
    "category": "ba6b12cadb3cd30044f9ff621f961981",
    "sp_column": "b8037ac41b0ec950055b9796bc4bcb86",
    "sys_class_name": "sp_instance",
    "sp_widget": "dbd0af311b58b450055b9796bc4bcb2d",
}
PROGRAM_KEY = "US-DDTC-ENFORCEMENT"


def get_link_href(base_url: str, link: str | None) -> str | None:
    if not link:
        return None
    anchor = html.fromstring(link)
    return urljoin(base_url, anchor.get("href"))


def crawl_row(context: Context, row: dict[str, Any]) -> None:
    entity = context.make("LegalEntity")
    name = row.pop("company")["value"]
    entity.id = context.make_slug(name)
    entity.add("name", name)
    entity.add("country", "us")
    entity.add("topics", "crime.traffick")

    description = row.pop("description")["value"]
    sanction = h.make_sanction(context, entity, description, program_key=PROGRAM_KEY)
    sanction.add("reason", description)
    sanction.add("listingDate", row.pop("year")["value"])

    base_url = context.dataset.url
    assert base_url is not None
    sanction.add(
        "sourceUrl",
        get_link_href(base_url, row.pop("charging_letter")["value"]),
    )

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row, ignore=["sys_id", "consent_agreement", "order"])


def crawl(context: Context) -> None:
    # ServiceNow table widget requires X-UserToken which is set as global variable
    # using a script tag in the page, and JSESSIONID cookie which is set in response
    # to page request.
    # BigIP cookie is also important since session seems to be origin-specific.

    context.log.info("Fetching table page")
    # Set cookies and fetch token
    url = context.dataset.url
    assert url is not None
    doc = context.fetch_html(url)
    page_data = h.xpath_element(
        doc,
        ".//script[@data-description='NOW vars set by properties, custom events, g_* globals']",
    )
    javascript_vars = h.element_text(page_data)
    token_match = re.search(r"window\.g_ck = '(\w+)';", javascript_vars)
    assert token_match is not None, "Could not find g_ck token in page"
    token: str = token_match.group(1)

    request_data = REQUEST_TEMPLATE

    headers: dict[str, str] = {"X-UserToken": token}
    num_pages = None

    data_url = context.dataset.data
    assert data_url is not None
    page_num = 1
    while num_pages is None or page_num <= num_pages:
        context.log.info(f"Fetching table data page {page_num}")
        request_data["p"] = page_num
        res = context.http.post(
            data_url.url,
            data=json.dumps(request_data),
            headers=headers,
        )
        response_data = res.json()
        num_pages = response_data["result"]["data"]["num_pages"]
        for row in response_data["result"]["data"]["list"]:
            crawl_row(context, row)
        page_num += 1
