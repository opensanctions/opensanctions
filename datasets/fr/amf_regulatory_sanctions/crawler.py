from typing import Any, Optional, cast

from normality import squash_spaces
from zavod import Context, helpers as h
import re
from html import unescape
from urllib.parse import urljoin


# Base URL for sourceUrl links
BASE_URL = "https://www.amf-france.org"
PREFIXES = [
    "Sociétés",
    "Société",
    "société",
    "Banque",
    "la société",
    "La société",
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
prefix_anon = "(" + "|".join(PREFIXES) + r")[\. \b]+[A-Z]\b"
comma_anon = r"(,|et) +[A-Z]\b"


def clean_names_str(names_str: str) -> Optional[str]:
    names_str = re.sub(comma_prefix_anon, "", names_str)
    names_str = re.sub(prefix_anon, "", names_str)
    names_str = re.sub(comma_anon, "", names_str)
    names_str = squash_spaces(names_str)
    return names_str


def entity_id(context: Context, name: str, listing_date: Optional[str]) -> str:
    id_ = context.make_id(name, listing_date)
    assert id_ is not None
    return id_


def crawl_entity(
    context: Context,
    *,
    name: str,
    reason: str,
    listing_date: Optional[str],
    source_urls: list[str],
    title: Optional[str],
) -> None:
    """Process a legal entity entry."""
    entity = context.make("LegalEntity")
    entity.id = entity_id(context, name, listing_date)

    entity.add("name", name)
    entity.add("notes", title)
    entity.add("topics", "reg.action")
    entity.add("sourceUrl", source_urls)

    sanction = h.make_sanction(context, entity)
    sanction.add("reason", reason)
    sanction.add("listingDate", listing_date)
    context.emit(sanction)
    context.emit(entity)


def crawl(context: Context) -> None:
    response = context.fetch_json(context.data_url)
    if "data" not in response:
        context.log.info("No data available.")
        return

    context.log.info(f"Fetched {len(response['data'])} results.")

    # General data
    for data_entry in response["data"]:
        reason = unescape(str(data_entry.get("theme")))
        infos_dict = data_entry.get("infos", {})

        names_str = unescape(infos_dict.pop("text_egard", ""))
        names_str = clean_names_str(names_str)
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

        # The information from infos_dict is used multiple times (once per name)
        # so we pop it here and pass it to the crawl_entity function instead of
        # doing it in crawl_entity.
        listing_date = infos_dict.pop("date", None)
        title = infos_dict.pop("title", None)

        source_urls = []
        link: Any = infos_dict.pop("link", None)
        if link is not None:
            if isinstance(link, str):
                full_link = urljoin(BASE_URL, link)
                source_urls.append(full_link)
            elif isinstance(link, dict) and "url" in link:
                url_link = cast(str, link.get("url"))
                source_urls.append(url_link)

        # Check and add download URL if available
        download: Any = infos_dict.pop("download", None)
        if download is not None:
            if download_links := download.get("sanction", {}).get("links", {}):
                download_url = download_links.get("url")
                if download_url and isinstance(download_url, str):
                    full_download_url = urljoin(BASE_URL, download_url)
                    source_urls.append(full_download_url)

        context.audit_data(
            infos_dict,
            ignore=[
                "text",  # penalty, how much they had to pay
                "recours",  # literally "appeal", something about whether the decision was/can be appealed?
            ],
        )

        for entity_name in entities_res.entities:
            crawl_entity(
                context,
                name=entity_name,
                reason=reason,
                listing_date=listing_date,
                source_urls=source_urls,
                title=title,
            )
            # Make it so you don't need a blank relations field in each entry
            if not entities_res.relationships:
                continue

            for rel in entities_res.relationships:
                # Create or get the existing relation target entity
                # rel_from_id = context.make_id(rel["from"], listing_date)
                rel_from_id = entity_id(context, rel["from"], listing_date)
                rel_to_id = entity_id(context, rel["to"], listing_date)
                # rel_to_id = context.make_id(rel["to"], listing_date)

                relation = context.make(rel["schema"])
                relation.id = context.make_id(rel_from_id, rel_to_id)
                relation.add(rel["from_prop"], rel_from_id)
                relation.add(rel["to_prop"], rel_to_id)
                relation.add("role", rel.get("role"))
                context.emit(relation)
