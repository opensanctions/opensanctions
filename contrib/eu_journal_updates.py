import logging
from pathlib import Path
import sys
from typing import Optional
import os
import click
from zeep import Client
from zeep.wsse.username import UsernameToken
from zeep.settings import Settings
from lxml import etree
from lxml.etree import _Element
from banal import hash_data

from zavod import Context
import zavod.helpers as h
from zavod.logs import configure_logging
from zavod.meta import load_dataset_from_path


DATASET_PATH = Path("datasets/eu/journal_sanctions/eu_journal_sanctions.yml")
REGIME_URL = "https://www.sanctionsmap.eu/api/v1/regime"

EURLEX_WS_URL = "https://eur-lex.europa.eu/EURLexWebService"
EURLEX_WS_USERNAME = os.environ.get("EURLEX_WS_USERNAME")
EURLEX_WS_PASSWORD = os.environ.get("EURLEX_WS_PASSWORD")
PAGE_SIZE = 100

SEEN_PATH = Path(os.environ["EU_JOURNAL_SEEN_PATH"])

# This selects the channel and includes a secret authorising sending messages.
SLACK_WEBHOOK_URL = os.environ["SLACK_WEBHOOK_URL"]


SLACK_ESCAPES = [
    ("&", "&amp;"),
    ("<", "&lt;"),
    (">", "&gt;"),
]


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
    args = [query, page_num, PAGE_SIZE, "en"]
    key = hash_data(args)
    response_text = context.cache.get(key, max_age=cache_days)
    if response_text is None:
        response = client.service.doQuery(*args)
        response_text = response.text
        root = etree.fromstring(response_text.encode("utf-8"))
        # only set if we could parse xml
        context.cache.set(key, response_text)
        context.log.debug("Cache MISS", query)
        return h.remove_namespace(root)
    root = etree.fromstring(response_text.encode("utf-8"))
    context.log.debug("Cache HIT", query)
    return h.remove_namespace(root)


def get_original_celex(context: Context, url: str) -> Optional[str]:
    """
    Gets the CELEX number of the original act, when given what could be a URL
    to a consolidated version of the act.
    """
    doc = context.fetch_html(url, cache_days=90)
    for link in doc.findall('.//div[@class="consLegLinks"]//a'):
        if "legal act" in link.text:
            celex = link.get("data-celex")
            if celex:
                return celex

    page_title = doc.find('.//div[@class="PageTitle"]')
    if page_title is not None:
        title = page_title.xpath("string()").strip()
        if title.startswith("Document"):
            number = title.split(" ")[-1]
            number = title.split("\xa0")[-1]
            number = number.split("-", 1)[0]
            return number
    context.log.error(f"Could not extract CELEX number from URL: {url}")
    return None


def query_celex(
    context: Context,
    client: Client,
    celex: str,
    page_num=1,
    cache_days: Optional[int] = None,
):
    query = f"MS={celex} OR EA={celex} OR LB={celex} ORDER BY XC DESC"
    context.log.info(f"Querying CELEX {celex}", page=page_num, query=query)
    soap_response = expert_query(
        context, client, query, page_num=page_num, cache_days=cache_days
    )
    total_hits = int(soap_response.find(".//totalhits").text)
    num_hits = int(soap_response.find(".//numhits").text)
    context.log.debug(
        f"Page: {page_num}, Total hits: {total_hits}, num hits: {num_hits}"
    )

    for result in soap_response.xpath(".//result"):
        journal_publication = result.findall(
            ".//RESOURCE_LEGAL_PUBLISHED_IN_OFFICIAL-JOURNAL"
        )
        if len(journal_publication) == 0:
            continue

        in_force = result.xpath(".//RESOURCE_LEGAL_IN-FORCE/VALUE")
        if in_force == [] or in_force[0].text != "true":
            continue

        titles = result.xpath(".//EXPRESSION_TITLE/VALUE/text()")
        assert len(titles) == 1
        result_celex = result.xpath(".//ID_CELEX/VALUE/text()")
        assert len(result_celex) == 1, result_celex
        celex_url = f"https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:{result_celex[0]}"
        # One document may have multiple dates - in one example the enactment date was the latest of the dates.
        # document_dates = result.xpath(".//WORK_DATE_DOCUMENT/VALUE/text()")

        # one EUR-Lex entry may be in multiple journals - perhaps when translations are published?
        oj_refs = []
        for journal in journal_publication:
            oj_class = journal.find(".//OFFICIAL-JOURNAL_CLASS/VALUE").text
            oj_number = journal.find(".//OFFICIAL-JOURNAL_NUMBER/VALUE").text
            oj_year = journal.find(".//OFFICIAL-JOURNAL_YEAR/VALUE").text
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
            for item in query_celex(
                context, client, celex, page_num=1, cache_days=cache_days
            ):
                item["regime"] = regime["specification"]
                yield item


def item_message(item):
    return f"""New Official Journal of the EU notice about {item["regime"]}:
<{item["celex_url"]}|{slack_escape(item["title"])}>
"""


def send_message(context, message):
    response = context.http.post(SLACK_WEBHOOK_URL, json={"text": message})
    if response.status_code == 200:
        return None
    else:
        return f"Error {response.status_code}\nMessage: {message}\nResponse: {response.text}"


@click.command()
@click.option("--debug", is_flag=True, default=False)
@click.option("--dry-run", is_flag=True, default=False)
@click.option("--cache_days", type=int, default=None)
def main(debug=False, dry_run=False, cache_days: Optional[int] = None) -> None:
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
    for item in crawl_updates(context, cache_days=cache_days):
        if item["celex"] in seen:
            continue
        new[item["celex"]] = item
    context.log.info(f"Found {len(new)} new items.")

    # Prepare the messages in advance to reduce the chance of partial failure
    messages = [item_message(i) for i in new.values()]

    # Announce the new files
    errors = []
    for message in messages:
        if dry_run:
            error = None
            context.log.info(f"Dry run - would send message: {message}")
        else:
            error = send_message(context, message)
        if error:
            errors.append(error)

    # Add to seen file so we don't process them again
    if new and not dry_run:
        with open(SEEN_PATH.as_posix(), "a") as fh:
            for celex in new.keys():
                fh.write(celex + "\n")
        context.log.info(f"Updated seen file with new items.")

    # If there were sending errors, log them and exit nonzero to alert us.
    if errors:
        for error in errors:
            context.log.error(error)
        sys.exit(1)


if __name__ == "__main__":
    main()
