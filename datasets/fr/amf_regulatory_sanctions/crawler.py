from typing import Any, Generator, Optional, Tuple, Union

from normality import collapse_spaces
from zavod import Context
import re
from html import unescape
from urllib.parse import urljoin


CLEAN_ENTITY = re.compile(r"(<br />\r\n| et |;)", re.IGNORECASE)
BASE_URL = "https://www.amf-france.org"


def parse_json(context: Context) -> Generator[dict, None, None]:
    response = context.fetch_json(context.data_url)
    if "data" not in response:
        context.log.info("No data available.")
        return

    context.log.info(
        f"Fetched {len(response['data'])} results."
    )  # Log the number of results
    for item in response["data"]:
        yield item


def get_value(
    data: dict, keys: Tuple[str, ...], default: Optional[Any] = None
) -> Union[Optional[Any], Any]:
    for key in keys:
        val = data.pop(key, None)
        if val is not None:
            return val
    return default


PREFIXES = [
    "Sociétés",
    "Société",
    "société",
    "Banque",
    "la société",
    "Caisse",
    "cabinet",
    "La sociétés",
    "les CABINETS",
    "les sociétés",
    "LA",
    "de gestion",
    "cabinets",
    "SCS",  # Société en commandite simple
    r"M\.?",
    "Mme",
    r"MM\.?",
    "Madame",
    "Monsieur",
    "Monsieur",
    "Melle",
]


comma_prefix_anon = "(,|et) +(" + "|".join(PREFIXES) + r")[\. \b]+[A-Z]\b"
print("comma_prefix_anon", comma_prefix_anon)
prefix_anon = "(" + "|".join(PREFIXES) + r")[\. \b]+[A-Z]\b"
print("prefix_anon", prefix_anon)
comma_anon = r"(,|et) +[A-Z]\b"
print("comma_anon", comma_anon)


def clean_names_str(names_str: str) -> Optional[str]:
    names_str = re.sub(comma_prefix_anon, "", names_str)
    names_str = re.sub(prefix_anon, "", names_str)
    names_str = re.sub(comma_anon, "", names_str)
    names_str = collapse_spaces(names_str)
    return names_str


def process_entity(
    context: Context,
    title: str,
    listing_date: str,
    name: str,
    theme: str,
    link: Optional[str],
    item: dict,
) -> None:
    """Process a legal entity entry."""
    entity = context.make("LegalEntity")
    entity.id = context.make_id(title, listing_date, name)

    # Add name directly without cleaning
    entity.add("name", name)

    # Add additional information
    entity.add("notes", theme)
    entity.add("notes", title)

    if link and isinstance(link, str):
        full_link = urljoin(BASE_URL, link)
        entity.add("sourceUrl", full_link)

    # Check and add download URL if available
    if "download" in item:
        download_links = item["download"].get("sanction", {}).get("links", {})
        if download_links:
            download_url = download_links.get("url")
            if download_url and isinstance(download_url, str):
                full_download_url = urljoin(BASE_URL, download_url)
                entity.add("sourceUrl", full_download_url)

    context.emit(entity, target=True)


def crawl(context: Context) -> None:
    # General data
    for data in parse_json(context):
        # print("data", data)

        # Extract relevant fields from each item
        theme = unescape(str(data.get("theme")))
        item = data.get("infos", {})
        title = item.get("title")
        names_str = unescape(item.get("text_egard", ""))
        # print("original", names_str)
        names_str = clean_names_str(names_str)
        # print("cleaned", names_str)
        if not names_str:
            continue

        entities_res = context.lookup("entities", names_str)
        # No mapping yet
        if entities_res is None:
            context.log.warning("No entity mapping for %r" % names_str)
            continue

        # No entities for remaining names str
        if not entities_res.entities:
            continue

        penalty = item.pop("text")
        # print("penalty", penalty)
        # html.fromstring(penalty)
        # Todo: figure out if we can assume cases like all/none exonorated and what topic we should give.
        # Perhaps we should just give all reg.action?

        listing_date = item.get("date", "")
        link = item.get("link", "")

        for entity in entities_res.entities:
            # make entities here
            process_entity(context, title, listing_date, entity, theme, link, item)
            # print("   ", entity)

        # Make it so you don't need a blank relations field in each entry
        if not entities_res.relationships:
            continue
        for rel in entities_res.relationships:
            # make relations here
            print("   ", rel)
