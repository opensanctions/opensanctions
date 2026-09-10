import json
import re
from typing import Any
from urllib.parse import parse_qs, urljoin, urlsplit
from datetime import datetime, timedelta
from dataclasses import dataclass
from lxml import html

from zavod import Context, settings
from zavod import helpers as h

BASE_URL = "https://www.nfa.futures.org"

@dataclass
class Respondent:
    id: str
    url: str
    name_raw: str

# Cutoff is later than earliest available year
def first_year() -> str:
    """Calculate min year for enforcements"""
    now = datetime.now()
    cutoff = timedelta(days = h.dates.MAX_ENFORCEMENT_DAYS)
    delta = now - cutoff
    return delta.strftime("%Y")


def fetch_csrf_token(context: Context):
    url = context.dataset.url
    doc = context.fetch_html(url)
    return h.xpath_string(doc, ".//meta[@name='csrf-token']/@content")


def fetch_rows(context: Context) -> list[dict[str, Any]]:
    """Fetch the full listing from the JSON-RPC endpoint.

    The endpoint only answers requests that carry the session cookie and CSRF token
    handed out by the listing page, and answers everything else with a redirect to an
    error page rather than an error status.
    """
    token = fetch_csrf_token(context)
    
    data = context.fetch_json(
        context.data_url,
        method="POST",
        data=json.dumps(
            {
                "method": "getEnforcementRegs",
                "params": [first_year(), settings.RUN_TIME.year],
            }
        ),
        headers={
            "Content-Type": "application/json",
            "Referer": context.dataset.url,
            "x-csrf-token": token,
            "x-requested-with": "XMLHttpRequest",
        },
    )
    rows = data["Result"]
    assert isinstance(rows, list) and len(rows), data.get("Message")
    return rows


def parse_respondents(context: Context, headline: str) -> list(Respondent):
    """Return the (respondent key, detail URL, caption) of each link in a headline."""
    respondents = list()
    fragment = html.fragment_fromstring(headline, create_parent="div")
    for anchor in h.xpath_elements(fragment, ".//a"):
        # Some hrefs are written root-relative with a Windows separator, e.g.
        # "\BasicNet/basic-reg-actions-details.aspx?...".
        href = (anchor.get("href") or "").replace("\\", "/")
        respondents.append(
            Respondent(
                id = parse_qs(urlsplit(href).query)["nfaid"][0],
                url = BASE_URL + href,
                name_raw = h.multi_split(
                    h.element_text(anchor), 
                    ["et al."]
                )[0],
            ))
    return respondents


def crawl_row(context: Context, row: dict[str, str]) -> None:
    case_id = row.pop("CASE_ID")
    category = row.pop("ACTION_CATEGORY_CODE")

    # Complaints are excluded. A complaint is the charging document that opens a disciplinary
    # case and states allegations that have not been adjudicated; respondents who were only
    # ever named in a complaint are therefore not in this dataset. Sanctions imposed —
    # fines, bars, suspensions, withdrawals — are published only on the per-case detail
    # pages, which NFA excludes from crawling, so this dataset records that an action was
    # taken but not what it imposed.
    if category == "COMPLAINTS":
        return

    date = row.pop("content_date_sort")
    respondents = parse_respondents(context, row.pop("HEADLINE_TEXT"))
    for respondent in respondents:
        entity = context.make("LegalEntity")
        entity.id = context.make_id(respondent.id, respondent.name_raw)
        h.apply_name(entity, full=respondent.name_raw)
        entity.add("topics", "reg.action")
        entity.add("sourceUrl", respondent.url)

        sanction = h.make_sanction(context, entity, key=case_id)
        sanction.add("authorityId", case_id)
        sanction.add("program", category)
        sanction.add("sourceUrl", respondent.url)
        h.apply_dates(
            sanction,
            "startDate",
            [date,],
        )

        context.emit(entity)
        context.emit(sanction)


def crawl(context: Context) -> None:
    for row in fetch_rows(context):
        crawl_row(context, row)

        # context.audit_data(
        #     row,
        #     ignore=[
        #         "CONTENT_DATE",
        #         "SORTORDER",
        #         "RULE_ID",
        #         "RULE_SECTION_ID",
        #         "RULE_SECTION_NAME",
        #     ],
        # )
