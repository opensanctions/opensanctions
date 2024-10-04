import csv
from typing import Dict, List, Optional, Set

from zavod import Context
import zavod.helpers as h

REGIME_URL = "https://www.sanctionsmap.eu/api/v1/regime"


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


def get_ojeu_number(context: Context, url: str) -> Optional[str]:
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
    context.log.error(f"Could not extract OJ number from URL: {url}")
    return None


def crawl_ojeu(context: Context) -> None:
    """Check what new legislation is available in OJEU that concerns sanctions."""
    known_urls: List[str] = context.dataset.config.get("ojeu_urls", [])
    regime = context.fetch_json(REGIME_URL)
    numbers: Set[str] = set()
    new_numbers: Set[str] = set()
    for item in regime["data"]:
        regime_url = f"{REGIME_URL}/{item['id']}"
        regime_json = context.fetch_json(regime_url, cache_days=1)
        legal_acts = regime_json.pop("data").pop("legal_acts", None)

        for act in legal_acts["data"]:
            url: str = act.pop("url")
            if "eur-lex.europa.eu" not in url:
                continue
            number = get_ojeu_number(context, url)
            if number is None:
                continue
            if url not in known_urls:
                context.log.warning(
                    "New OJEU URL found",
                    url=url,
                    number=number,
                    title=act.get("title"),
                )
                new_numbers.add(number)
            numbers.add(number)

    if len(new_numbers):
        context.log.info("New OJEU numbers", numbers=new_numbers)

    numbers.discard("31992R3541")
    numbers.discard("31993R3275")
    numbers.discard("32014R0833")
    numbers.discard("32014R0269")

    ascending = sorted(numbers)
    for num in ascending:
        query = f"MS={num} OR EA={num} OR LB={num} ORDER BY XC DESC"
        name = f"OJEU-TRACK-{num}"
        # context.log.info("Query for EUR-Lex", query=query, name=name)
        print(f"[{name}] {query}")


def crawl(context: Context):
    """Crawl the OHCHR database as converted to CSV"""
    crawl_ojeu(context)
    # crawl_csv(context)
