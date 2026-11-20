import re
from datetime import datetime
from html import unescape
from typing import Any, Literal
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

from datapatch import Result
from normality import squash_spaces
from pydantic import BaseModel
from zavod import Context, helpers as h
from zavod.stateful.review import TextSourceValue, review_extraction

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


# A placeholder letter must not run on into the rest of a real name, such as
# "M Capital Partners", "A Plus Finance", "X-Trade" or "B*CAPITAL".
placeholder = r"[A-Z](?![\w*-]|\s+[A-Z][a-zà-ÿ])"
comma_prefix_anon = "(,|et) +(" + "|".join(PREFIXES) + r")[\. \b]+" + placeholder
prefix_anon = "(" + "|".join(PREFIXES) + r")[\. \b]+" + placeholder
comma_anon = r"(,|et) +" + placeholder


class Respondent(BaseModel):
    entity_schema: Literal["Person", "LegalEntity"]
    name: str
    previous_names: list[str] = []


class Succession(BaseModel):
    predecessor: str
    successor: str


class Respondents(BaseModel):
    respondents: list[Respondent]
    successions: list[Succession] = []


def clean_names_str(names_str: str) -> str | None:
    names_str = re.sub(comma_prefix_anon, "", names_str)
    names_str = re.sub(prefix_anon, "", names_str)
    names_str = re.sub(comma_anon, "", names_str)
    names_str = squash_spaces(names_str)
    return names_str


def respondent_id(context: Context, respondent: Respondent) -> str:
    # Lookup-era IDs hashed a list of current and previous names as its str() form.
    if respondent.previous_names:
        id_ = context.make_id(str([respondent.name, *respondent.previous_names]))
    else:
        id_ = context.make_id(respondent.name)
    assert id_ is not None
    return id_


def is_person(raw: str, name: str) -> bool:
    """A name directly after M./Mme/..., or in a list after MM. without a company
    prefix in between, names a person."""
    escaped = re.escape(name)
    if re.search(r"(?<!\w)(M\.?|Mme\.?|Monsieur|Madame|Melle|Mlle)\s+" + escaped, raw):
        return True
    match = re.search(r"(?<!\w)MM\.?\s+([^<]*?)" + escaped, raw)
    if match is None:
        return False
    return re.search(r"soci[ée]t[ée]|cabinet|banque", match.group(1), re.I) is None


def respondents_from_lookup(raw: str, res: Result) -> Respondents:
    respondents: list[Respondent] = []
    for value in res.entities or []:
        if isinstance(value, list):
            name, *previous_names = value
            respondents.append(
                Respondent(
                    entity_schema="LegalEntity",
                    name=name,
                    previous_names=previous_names,
                )
            )
            continue
        assert isinstance(value, str), value
        schema: Literal["Person", "LegalEntity"] = (
            "Person" if is_person(raw, value) else "LegalEntity"
        )
        respondents.append(Respondent(entity_schema=schema, name=value))

    successions: list[Succession] = []
    for rel in res.relationships or []:
        assert rel["schema"] == "Succession", rel
        assert rel["from_prop"] == "predecessor", rel
        assert rel["to_prop"] == "successor", rel
        assert rel["role"] == "venant aux droits", rel
        # A list names one entity by its current and previous names.
        predecessor = rel["from"][0] if isinstance(rel["from"], list) else rel["from"]
        successor = rel["to"][0] if isinstance(rel["to"], list) else rel["to"]
        successions.append(Succession(predecessor=predecessor, successor=successor))
    return Respondents(respondents=respondents, successions=successions)


def parse_listing_date(timestamp: str) -> str:
    # The AMF publishes from Paris; UTC would shift late-evening entries a day back.
    published = datetime.fromtimestamp(int(timestamp), ZoneInfo("Europe/Paris"))
    return published.date().isoformat()


def emit_respondents(
    context: Context,
    respondents: Respondents,
    *,
    origin: str,
    reason: str,
    listing_date: str,
    title: str,
    source_urls: list[str],
) -> None:
    ids: dict[str, str] = {}
    for respondent in respondents.respondents:
        # Names and schemata stay as published until the switch to reviewed
        # extraction, so the backfill doesn't change them.
        entity = context.make("LegalEntity")
        entity.id = respondent_id(context, respondent)
        ids[respondent.name] = entity.id
        entity.add("name", [respondent.name, *respondent.previous_names], origin=origin)
        entity.add("topics", "reg.action")
        entity.add("sourceUrl", source_urls)

        sanction = h.make_sanction(context, entity, key=title)
        sanction.add("reason", reason)
        sanction.add("authorityId", title)
        sanction.add("listingDate", listing_date)
        sanction.add("sourceUrl", source_urls)
        context.emit(sanction)
        context.emit(entity)

    for succession in respondents.successions:
        predecessor_id = ids[succession.predecessor]
        successor_id = ids[succession.successor]
        relation = context.make("Succession")
        relation.id = context.make_id(predecessor_id, successor_id)
        relation.add("predecessor", predecessor_id)
        relation.add("successor", successor_id)
        relation.add("role", "venant aux droits")
        context.emit(relation)


def crawl(context: Context) -> None:
    # TODO: remove after the first production run. Fixing the placeholder regex
    # restored the leading letter of the first two names, and Natixis Securities
    # now lists its current name first. Both changed their IDs.
    for old_key, new_key in [
        ("Capital Partners", "M Capital Partners"),
        ("Plus Finance", "A Plus Finance"),
        (
            str(["NATEXIS BLEICHROEDER", "NATIXIS SECURITIES"]),
            str(["NATIXIS SECURITIES", "NATEXIS BLEICHROEDER"]),
        ),
    ]:
        context.rekey(context.make_id(old_key), context.make_id(new_key))

    response = context.fetch_json(context.data_url)
    if "data" not in response:
        context.log.info("No data available.")
        return

    context.log.info(f"Fetched {len(response['data'])} results.")

    for data_entry in response["data"]:
        reason = unescape(data_entry.pop("theme"))
        listing_date = parse_listing_date(data_entry.pop("date"))
        infos_dict = data_entry.pop("infos")
        context.audit_data(data_entry)

        raw_names = unescape(infos_dict.pop("text_egard"))
        names_str = clean_names_str(raw_names)
        if not names_str:
            continue

        entities_res = context.lookup("entities", names_str)
        # No mapping yet
        if entities_res is None:
            context.log.warning(f"No entity mapping for {names_str!r}")
            continue

        respondents = respondents_from_lookup(raw_names, entities_res)
        # Backfill the lookup as accepted reviews, so the move to reviewed
        # extraction keeps the names and IDs we already publish.
        review_extraction(
            context,
            source_value=TextSourceValue(
                key_parts=raw_names, label="Respondents (text_egard)", text=raw_names
            ),
            original_extraction=respondents,
            origin="lookups",
            default_accepted=True,
        )
        if not respondents.respondents:
            continue

        title = infos_dict.pop("title")
        assert title, infos_dict

        source_urls = []
        link: Any = infos_dict.pop("link", None)
        if link is not None:
            if isinstance(link, str):
                source_urls.append(urljoin(BASE_URL, link))
            elif isinstance(link, dict) and "url" in link:
                source_urls.append(link["url"])

        download: Any = infos_dict.pop("download", None)
        if download is not None:
            if download_links := download.get("sanction", {}).get("links", {}):
                download_url = download_links.get("url")
                if download_url and isinstance(download_url, str):
                    source_urls.append(urljoin(BASE_URL, download_url))

        context.audit_data(
            infos_dict,
            ignore=[
                "text",  # penalty, how much they had to pay
                "recours",  # literally "appeal", something about whether the decision was/can be appealed?
            ],
        )

        emit_respondents(
            context,
            respondents,
            origin="lookups",
            reason=reason,
            listing_date=listing_date,
            title=title,
            source_urls=source_urls,
        )
