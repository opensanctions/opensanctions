import logging
from pathlib import Path
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

SEEN_PATH = Path(os.environ["EU_JOURNAL_SEEN_PATH"])


def expert_query(context: Context, client, expert_query: str) -> _Element:
    args = [expert_query, 1, 100, "en"]
    key = hash_data(args)
    response_text = context.cache.get(key, max_age=7)
    if response_text is None:
        response = client.service.doQuery(*args)
        response_text = response.text
        root = etree.fromstring(response_text.encode("utf-8"))
        # only set if we could parse xml
        context.cache.set(key, response_text)
        context.log.debug("Cache MISS", expert_query)
        return h.remove_namespace(root)
    root = etree.fromstring(response_text.encode("utf-8"))
    context.log.debug("Cache HIT", expert_query)
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


def crawl_updates(context: Context):
    client = Client(
        "https://eur-lex.europa.eu/EURLexWebService?wsdl",
        wsse=UsernameToken(EURLEX_WS_USERNAME, EURLEX_WS_PASSWORD),
        settings=Settings(raw_response=True),
    )
    regimes = context.fetch_json(REGIME_URL)
    in_force_count = 0
    in_oj_count = 0
    for item in regimes["data"]:
        regime_url = f"{REGIME_URL}/{item['id']}"
        print(item["specification"])
        print(regime_url)
        regime_json = context.fetch_json(regime_url, cache_days=1)
        legal_acts = regime_json.pop("data").pop("legal_acts", None)

        for act in legal_acts["data"]:
            url: str = act.pop("url")
            if "eur-lex.europa.eu" not in url:
                continue
            number = get_original_celex(context, url)
            query = f"MS={number} OR EA={number} OR LB={number} ORDER BY XC DESC"
            soap_response = expert_query(context, client, query)
            if number == "32022R0263":
                print(etree.tostring(soap_response).decode("utf-8"))
            for result in soap_response.xpath(".//result"):
                in_oj = result.find(".//RESOURCE_LEGAL_PUBLISHED_IN_OFFICIAL-JOURNAL")
                if in_oj is None:
                    continue
                in_oj_count += 1

                in_force = result.xpath(".//RESOURCE_LEGAL_IN-FORCE/VALUE")
                if in_force == [] or in_force[0].text != "true":
                    continue
                in_force_count += 1

                titles = result.xpath(".//EXPRESSION_TITLE/VALUE/text()")
                assert len(titles) == 1
                yield {
                    "title": titles[0],
                    # date
                }
    assert in_oj_count > 2000, in_oj_count
    assert in_force_count > 1000, in_force_count


@click.command()
@click.option("--debug", is_flag=True, default=False)
@click.option("--cache_days", default=None)
def main(debug=False, cache_days: Optional[int] = None) -> None:
    """Check what new legislation is available in OJEU that concerns sanctions."""
    configure_logging(level=logging.DEBUG if debug else logging.INFO)
    dataset = load_dataset_from_path(DATASET_PATH)
    context = Context(dataset)
    list(crawl_updates(context))


if __name__ == "__main__":
    main()
