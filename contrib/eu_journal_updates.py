import logging
import os
import re
import sys
from pathlib import Path
from typing import Optional
from urllib.parse import urlencode

import click
import requests
from banal import hash_data
from lxml import etree
from lxml.etree import _Element
from zeep import Client
from zeep.settings import Settings
from zeep.wsse.username import UsernameToken

import zavod.helpers as h
from zavod import Context
from zavod.logs import configure_logging
from zavod.meta import load_dataset_from_path

DATASET_PATH = Path("datasets/eu/journal_sanctions/eu_journal_sanctions.yml")
REGIME_URL = "https://www.sanctionsmap.eu/api/v1/regime"

EURLEX_WS_URL = "https://eur-lex.europa.eu/EURLexWebService"
EURLEX_WS_USERNAME = os.environ.get("EURLEX_WS_USERNAME")
EURLEX_WS_PASSWORD = os.environ.get("EURLEX_WS_PASSWORD")
HEARTBEAT_URL = os.environ.get("EU_JOURNAL_HEARTBEAT_URL", "")
PAGE_SIZE = 100

SEEN_PATH = Path(os.environ["EU_JOURNAL_SEEN_PATH"])

# This selects the channel and includes a secret authorising sending messages.
SLACK_WEBHOOK_URL = os.environ["SLACK_WEBHOOK_URL"]

SLACK_ESCAPES = [
    ("&", "&amp;"),
    ("<", "&lt;"),
    (">", "&gt;"),
]

REGEX_CORRIG = re.compile(r"R\(\d+\)$")


def slack_escape(string):
    """
    https://api.slack.com/reference/surfaces/formatting#escaping
    > You shouldn't HTML entity-encode the entire text, as only the specific
    > characters shown above will be decoded for display in Slack.
    """
    for char, escape in SLACK_ESCAPES:
        string = string.replace(char, escape)
    return string


def expert_query(
    context: Context,
    client,
    query: str,
    page_num: int,
    cache_days: Optional[int] = None,
) -> _Element:
    """ "
    Equivalent to Expert Search on the eur-lex website.

    https://eur-lex.europa.eu/expert-search-form.html
    """
    args = [query, page_num, PAGE_SIZE, "en"]
    # Our search language is English. That means we don't get titles for most
    # documents that don't have an English version. I have seen german
    # and french titles for these series. That might be due to user language
    # preferences.
    #
    # The only documents I've seen this happen on were corrections -
    # I guess to that language version of the notice in the journal.
    # e.g. 32022D0266R(01), 32022D0266R(02), 32022D0266R(03), ...
    key = hash_data(args)
    response_text = context.cache.get(key, max_age=cache_days) if cache_days else None
    if response_text is None:
        response = client.service.doQuery(*args)
        response.raise_for_status()
        response_text = response.text
        root = etree.fromstring(response_text.encode("utf-8"))
        # only set if we could parse xml
        context.cache.set(key, response_text)
        context.flush()
        context.log.debug("Cache MISS", args=args)
        return h.remove_namespace(root)
    root = etree.fromstring(response_text.encode("utf-8"))
    context.log.debug("Cache HIT", args=args)
    return h.remove_namespace(root)


def get_original_celex(context: Context, url: str) -> Optional[str]:
    """
    Gets the CELEX number of the original act, when given what could be a URL
    to a consolidated version of the act, an amendment, or the original.
    """
    url = url.replace("/TXT/", "/ALL/")
    doc = context.fetch_html(url, cache_days=90)

    # Consolidated versions
    for link in doc.findall('.//div[@class="consLegLinks"]//a'):
        if "initial legal act" in link.text:
            celex = link.get("data-celex")
            if celex:
                return celex

    # Amendments
    tables = doc.xpath(".//table[@id='relatedDocsTbMS']")
    if tables:
        assert len(tables) == 1, (len(tables), url)
        rows = [h.cells_to_str(row) for row in h.parse_html_table(tables[0])]
        for row in rows:
            if row["relation"] in {"Extended validity", "Modifies"}:
                return row["act"]

    # Originals
    local_ids = doc.xpath(".//meta[@property='eli:id_local']/@content")
    if local_ids:
        assert len(local_ids) == 1, (local_ids, url)
        return local_ids[0]

    context.log.error(f"Could not extract CELEX number from {url}")
    return None


def query_celex(
    context: Context,
    client: Client,
    celex: str,
    page_num=1,
    cache_days: Optional[int] = None,
):
    # Expert query gives 500s when querying corrigendum CELEXes.
    celex = REGEX_CORRIG.sub("", celex)
    query = f"MS={celex} OR EA={celex} OR LB={celex} ORDER BY XC DESC"
    context.log.info(f"Querying CELEX {celex}", page=page_num, query=query)
    try:
        soap_response = expert_query(
            context, client, query, page_num=page_num, cache_days=cache_days
        )
    except requests.exceptions.HTTPError as e:
        context.log.error("Error querying EUR-Lex", error=e, response=e.response.text)
        return
    total_hits = int(soap_response.find(".//totalhits").text)
    num_hits = int(soap_response.find(".//numhits").text)
    context.log.debug(
        f"Page: {page_num}, Total hits: {total_hits}, num hits: {num_hits}"
    )

    for result in soap_response.xpath(".//result"):
        # OJ numbers seem to be in multiple possible places so this excludes too many.
        # Compare acts to corrigenda.
        # journal_publication = result.findall(
        #    ".//RESOURCE_LEGAL_PUBLISHED_IN_OFFICIAL-JOURNAL"
        # )
        # if len(journal_publication) == 0:
        #    continue

        # Skipping not in_force documents means missing corrigendum notices
        # in_force = result.xpath(".//RESOURCE_LEGAL_IN-FORCE/VALUE")
        # if in_force == [] or in_force[0].text != "true":
        #    continue

        titles = result.xpath(".//EXPRESSION_TITLE/VALUE/text()")
        assert len(titles) <= 1, titles
        # title appears to only be missing for documents without a version in english,
        # german or french.
        if not titles:
            continue

        result_celex = result.xpath(".//ID_CELEX/VALUE/text()")
        assert len(result_celex) == 1, result_celex
        celex_url = "https://eur-lex.europa.eu/legal-content/EN/TXT/?" + urlencode(
            {"uri": f"CELEX:{result_celex[0]}"}
        )
        # One document may have multiple dates - in one example the enactment date
        # was the latest of the dates.
        # document_dates = result.xpath(".//WORK_DATE_DOCUMENT/VALUE/text()")
        # oj_pub_dates = result.xpath(
        #    ".//RESOURCE_LEGAL_PUBLISHED_IN_OFFICIAL-JOURNAL//DATE_PUBLICATION/VALUE/text()"
        # )

        # one EUR-Lex entry may be in multiple journals - perhaps when translations are published?
        # oj_refs = []
        # for journal in journal_publication:
        #    oj_class = journal.find(".//OFFICIAL-JOURNAL_CLASS/VALUE").text
        #    oj_number = journal.find(".//OFFICIAL-JOURNAL_NUMBER/VALUE").text
        #    oj_year = journal.find(".//OFFICIAL-JOURNAL_YEAR/VALUE").text
        # OJ C 326/2015
        # oj_refs.append(f"{oj_class} {oj_number}/{oj_year}")

        yield {
            "title": titles[0],
            "celex": result_celex[0],
            "celex_url": celex_url,
            # "oj_refs": oj_refs,
        }
    if page_num * PAGE_SIZE < total_hits:
        yield from query_celex(
            context, client, celex, page_num=page_num + 1, cache_days=cache_days
        )


def crawl_updates(context: Context, cache_days: Optional[int] = None):
    """
    Get all the EUR-Lex entries about all regimes that are in the official journal
    and are in force.

    Entries may be returned more than once.
    """
    client = Client(
        "https://eur-lex.europa.eu/EURLexWebService?wsdl",
        wsse=UsernameToken(EURLEX_WS_USERNAME, EURLEX_WS_PASSWORD),
        settings=Settings(raw_response=True),
    )
    regimes = context.fetch_json(REGIME_URL)
    seen_this_run = set()
    for regime in regimes["data"]:
        regime_url = f"{REGIME_URL}/{regime['id']}"
        context.log.info(
            "Crawling regime", title=regime["specification"], url=regime_url
        )
        regime_json = context.fetch_json(regime_url, cache_days=cache_days)
        legal_acts = regime_json.pop("data").pop("legal_acts", None)

        for act in legal_acts["data"]:
            url: str = act.pop("url")
            if "eur-lex.europa.eu" not in url:
                continue

            celex = get_original_celex(context, url)
            if celex in seen_this_run:
                context.log.info(f"Skipping {celex} as already seen in this run.")
                continue
            seen_this_run.add(celex)

            for item in query_celex(
                context, client, celex, page_num=1, cache_days=cache_days
            ):
                item["regime"] = regime["specification"]
                yield item


def item_message(item):
    return f"""New document on EUR-Lex regarding '{item["regime"]}':
<{item["celex_url"]}|{slack_escape(item["title"])}>
"""


def send_message(context, message):
    response = context.http.post(SLACK_WEBHOOK_URL, json={"text": message})
    if response.status_code == 200:
        return None
    else:
        return f"Error {response.status_code}\nMessage: {message}\nResponse: {response.text}"


def exit_with_error(context: Context, message: str):
    if HEARTBEAT_URL:
        context.http.post(HEARTBEAT_URL + "/fail", data=message.encode("utf-8"))
    sys.exit(1)


@click.command()
@click.option("--debug", is_flag=True, default=False)
@click.option("--slack", is_flag=True, default=False)
@click.option("--update-seen", is_flag=True, default=False)
@click.option("--cache-days", type=int, default=None)
def main(
    debug=False, slack=False, update_seen=False, cache_days: Optional[int] = None
) -> None:
    """Check what new legislation is available in OJEU that concerns sanctions."""
    configure_logging(level=logging.DEBUG if debug else logging.INFO)
    dataset = load_dataset_from_path(DATASET_PATH)
    context = Context(dataset)

    context.log.info("Loading seen items", path=SEEN_PATH)
    with open(SEEN_PATH.as_posix(), "r") as fh:
        seen = set(i.strip() for i in fh.readlines())
    context.log.info(f"Seen {len(seen)} items.")

    # Get all the new items
    new = dict()
    some_updates = False
    for item in crawl_updates(context, cache_days=cache_days):
        some_updates = True
        if item["celex"] in seen:
            continue
        new[item["celex"]] = item
    context.log.info(f"Found {len(new)} new items.")

    # If we didn't find any updates, something's probably wrong.
    if not some_updates:
        error_message = "No legislation or amendments found at all. Something's wrong."
        context.log.error(error_message)
        exit_with_error(context, error_message)

    # Prepare the messages in advance to reduce the chance of partial failure
    # and subsequent duplicate messages.
    messages = [item_message(i) for i in new.values()]

    # Announce the new files
    errors = []
    for message in messages:
        if slack:
            error = send_message(context, message)
        else:
            error = None
            context.log.info(f"Message: {message}")
        if error:
            errors.append(error)

    # Add to seen file so we don't process them again
    if new and update_seen:
        with open(SEEN_PATH.as_posix(), "a") as fh:
            for celex in new.keys():
                fh.write(celex + "\n")
        context.log.info("Updated seen file with new items.")

    # If there were sending errors, log them and exit nonzero to alert us.
    if errors:
        for error in errors:
            context.log.error(error)
        error_message = "\n".join(errors)
        exit_with_error(context, error_message)


if __name__ == "__main__":
    main()
