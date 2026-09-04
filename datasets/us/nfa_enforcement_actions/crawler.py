import json
import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import parse_qs, urljoin, urlsplit

from lxml import html

from zavod import Context, settings
from zavod import helpers as h

# Lower bound of the year slider on the source page; the earliest action is from 1996.
FIRST_YEAR = 1981
# The link params carrying a respondent's key, in the two forms the source has used.
KEY_PARAMS = ("entityid", "nfaid")
# A caption naming more than one respondent, e.g. "Acme Futures, et al. (Jane Roe)".
AMBIGUOUS_CAPTION = re.compile(r"\bet\.?\s*al\b|[()]", re.IGNORECASE)
# Names often contain an et al and parenthetical names, all post-nominal info that 
# we can ignore.
PATTERN_IRREGULAR = r"[()]|et al"
REGEX_IRREGULAR = re.compile(PATTERN_IRREGULAR, re.IGNORECASE)


def listing_url(context: Context) -> str:
    """The page the respondent links are relative to, and the API's expected referer."""
    url = context.dataset.url
    assert url is not None, "Dataset metadata is missing `url`"
    return url


def fetch_rows(context: Context) -> list[dict[str, Any]]:
    """Fetch the full listing from the JSON-RPC endpoint.

    The endpoint only answers requests that carry the session cookie and CSRF token
    handed out by the listing page, and answers everything else with a redirect to an
    error page rather than an error status.
    """
    page_url = listing_url(context)
    doc = context.fetch_html(page_url)
    token = h.xpath_string(doc, ".//meta[@name='csrf-token']/@content")
    text = context.fetch_text(
        context.data_url,
        method="POST",
        data=json.dumps(
            {
                "method": "getEnforcementRegs",
                "params": [FIRST_YEAR, settings.RUN_TIME.year],
            }
        ),
        headers={
            "Content-Type": "application/json",
            "Referer": page_url,
            "x-csrf-token": token,
            "x-requested-with": "XMLHttpRequest",
        },
    )
    if text is None or not text.startswith("{"):
        raise RuntimeError("Listing API returned no JSON; the CSRF handshake failed.")
    data = json.loads(text)
    if data.get("Success") is not True:
        raise RuntimeError(f"Listing API reported failure: {data.get('Message')!r}")
    rows = data["Result"]
    assert isinstance(rows, list) and len(rows), data.get("Message")
    return rows


def normalise_key(param: str, value: str) -> str:
    """Reduce a respondent's link parameter to a single form per respondent.

    Numeric `entityid` values are published both zero-padded to seven digits and
    unpadded, for about 90 respondents in both forms, so they are padded here. One
    legacy id is not numeric and is left alone.
    """
    value = value.strip()
    if param == "entityid" and value.isdigit():
        return value.zfill(7)
    return value


def parse_respondents(
    context: Context, headline: str, case_id: str
) -> list[tuple[str, str, str]]:
    """Return the (respondent key, detail URL, caption) of each link in a headline."""
    fragment = html.fragment_fromstring(headline, create_parent="div")
    respondents: list[tuple[str, str, str]] = []
    for anchor in h.xpath_elements(fragment, ".//a"):
        caption = h.element_text(anchor)
        # Some hrefs are written root-relative with a Windows separator, e.g.
        # "\BasicNet/basic-reg-actions-details.aspx?...".
        href = (anchor.get("href") or "").replace("\\", "/")
        query = parse_qs(urlsplit(href).query)
        keys = [
            f"{p}:{normalise_key(p, query[p][0])}" for p in KEY_PARAMS if p in query
        ]
        if len(keys) != 1:
            # An unclosed <a/> tag in a 1998 headline leaves a stray, empty anchor.
            if caption:
                context.log.warning(
                    "Cannot identify respondent from link",
                    case_id=case_id,
                    caption=caption,
                    href=href,
                )
            continue
        respondents.append((keys[0], urljoin(listing_url(context), href), caption))
    return respondents


def clean_name(name: str) -> str:
    """
    Names often contain a post-nominal add on that can vary, even though
    the entityid (from the NFA website) is the same for that name.

    ex. 
    American Futures Group, Inc., et al. (Bill Hockemeyer)
    should be
    American Futures Group, Inc.
    """
    PATTERN_ETAL = r"\s*,?\s*\bet\.?\s+al\b\.?"
    REGEX_ETAL = re.compile(PATTERN_ETAL, re.IGNORECASE)
    name_without_etal = REGEX_ETAL.split(name, 1)[0]
    PATTERN_PARENTHETICAL = r"\(([^()]+)\)"
    REGEX_PARENTHETICAL = re.compile(PATTERN_PARENTHETICAL, re.IGNORECASE)
    name_without_parentheticals = re.sub(REGEX_PARENTHETICAL, "", name_without_etal)
    return name_without_parentheticals


def parse_row(context: Context, row) -> None:
    case_id = row.pop("CASE_ID")
    category = row.pop("ACTION_CATEGORY_CODE")
    date = row.pop("content_date_sort")
    respondents = parse_respondents(context, row.pop("HEADLINE_TEXT"), case_id)
    
    for respondent in respondents:
        entity_id = respondent[0]
        source_url = respondent[1]
        name_raw = respondent[2]
        
        entity = context.make("LegalEntity")
        entity.id = context.make_id(entity_id, name_raw)
        h.apply_name(entity, full=clean_name(name_raw))
        entity.add("topics", "reg.action")
        entity.add("sourceUrl", source_url)
        

        sanction = h.make_sanction(context, entity, key=case_id)
        sanction.add("authorityId", case_id)
        sanction.add("program", category)
        sanction.add("sourceUrl", source_url)
        h.apply_dates(sanction, "startDate", [date,])

        context.emit(entity)
        context.emit(sanction)


def crawl(context: Context) -> None:
    irregular_count = 0
    for row in fetch_rows(context):
        parse_row(context, row)

        context.audit_data(
            row,
            ignore=[
                "CONTENT_DATE",
                "SORTORDER",
                "RULE_ID",
                "RULE_SECTION_ID",
                "RULE_SECTION_NAME",
            ],
        )