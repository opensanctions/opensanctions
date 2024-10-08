import csv
from typing import Dict, List, Optional, Set, Tuple
import os
from zeep import Client
from zeep.wsse.username import UsernameToken
from zeep.settings import Settings
from lxml import etree
from banal import hash_data

from zavod import Context
import zavod.helpers as h

REGIME_URL = "https://www.sanctionsmap.eu/api/v1/regime"

EURLEX_WS_URL = "https://eur-lex.europa.eu/EURLexWebService"
EURLEX_WS_USERNAME = os.environ.get("EURLEX_WS_USERNAME")
EURLEX_WS_PASSWORD = os.environ.get("EURLEX_WS_PASSWORD")
client = Client(
    "https://eur-lex.europa.eu/EURLexWebService?wsdl",
    wsse=UsernameToken(EURLEX_WS_USERNAME, EURLEX_WS_PASSWORD),
    settings=Settings(raw_response=True),
)


def crawl_row(context: Context, row: Dict[str, str]):
    """Process one row of the CSV data"""
    row_id = row.pop("List ID").strip(" \t.")
    entity_type = row.pop("Type").strip()
    name = row.pop("Name").strip()
    country = row.pop("Country").strip()

    # context.log.info(f"Processing row ID {row_id}: {name}")
    entity = context.make(entity_type)
    entity.id = context.make_id(row_id, name, country)
    context.log.debug(f"Unique ID {entity.id}")
    entity.add("topics", "sanction")
    entity.add("country", country)
    entity.add("sourceUrl", row.pop("Source URL", None))
    entity.add("birthDate", row.pop("DOB", None))
    h.apply_name(entity, name)
    alias = row.pop("Alias").strip()
    if alias:
        h.apply_name(entity, alias, alias=True)
    context.audit_data(row)
    sanction = h.make_sanction(context, entity)
    context.emit(entity, target=True)
    context.emit(sanction)


def crawl_csv(context: Context):
    """Process the CSV data"""
    path = context.fetch_resource("reg_2878_database.csv", context.data_url)
    with open(path, "rt") as infh:
        reader = csv.DictReader(infh)
        for row in reader:
            crawl_row(context, row)


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


def expert_query(context: Context, expert_query: str) -> etree.Element:
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


def crawl_ojeu(context: Context) -> None:
    """Check what new legislation is available in OJEU that concerns sanctions."""
    known_urls: List[str] = context.dataset.config.get("ojeu_urls", [])
    regime = context.fetch_json(REGIME_URL)
    old_numbers: Set[str] = set()
    new_numbers: Set[Tuple[str, str]] = set()
    for item in regime["data"]:
        regime_url = f"{REGIME_URL}/{item['id']}"
        print(regime_url)
        regime_json = context.fetch_json(regime_url, cache_days=1)
        legal_acts = regime_json.pop("data").pop("legal_acts", None)

        for act in legal_acts["data"]:
            url: str = act.pop("url")
            if "eur-lex.europa.eu" not in url:
                continue
            number = get_original_celex(context, url)
            print("   ", number, url)
            query = f"MS={number} OR EA={number} OR LB={number} ORDER BY XC DESC"
            print("   ", query)
            soap_response = expert_query(context, query)
            #if number == "32022R0263":
            #    print(etree.tostring(soap_response).decode("utf-8"))
            for result in soap_response.xpath(".//result"):
                for title in result.xpath(".//EXPRESSION_TITLE/VALUE/text()"):
                    print("       ", title)
                for eli in result.xpath(".//RESOURCE_LEGAL_ELI/VALUE/text()"):
                    print("         ", eli)
                for in_force in result.xpath(".//RESOURCE_LEGAL_IN-FORCE/VALUE/text()"):
                    print(f"         in_force={in_force}")
                print(f"         in_oj={result.find('.//RESOURCE_LEGAL_PUBLISHED_IN_OFFICIAL-JOURNAL') is not None}")
                


def crawl(context: Context):
    """Crawl the OHCHR database as converted to CSV"""
    crawl_ojeu(context)
    # crawl_csv(context)
