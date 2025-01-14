# Imiona i nazwisk
# Pseudonim
# Data urodzenia
# Miejsce urodzenia
# Rodzaj i nr dokumentu stwierdzającego tożsamość
# Inne informacje
# Uzasadnienie wpisu na listę
# Data umieszczenia na liście
#
# Names and surnames
# Pseudonym
# Date of birth
# Place of birth
# Type and number of the identity document
# Other informations
# Justification for entry on the list
# Date of listing

from normality import collapse_spaces
from rigour.mime.types import CSV
from typing import Dict
import csv

from zavod import Context
from zavod import helpers as h
from zavod.shed.un_sc import get_legal_entities, get_persons, Regime, load_un_sc

PDF_URL = "https://www.gov.pl/attachment/2fc03b3b-a5f6-4d08-80d1-728cdb71d2d6"
POLAND_PROGRAM = "art. 118 ustawy z dnia 1 marca 2018 r. o przeciwdziałaniu praniu pieniędzy i finansowaniu terroryzmu"
UN_SC_PREFIXES = [Regime.TALIBAN, Regime.DAESH_AL_QAIDA]

KNOWN_HASHES = {
    "https://www.gov.pl/attachment/2fc03b3b-a5f6-4d08-80d1-728cdb71d2d6": "94c0607177fec8a07ca3e7d82c3d61be36ea20ee",
    "https://www.gov.pl/attachment/56238b34-8a26-4431-a05a-e1d039f0defa": "3b8c0419879991e4dfd663aeed7b2df3c7472c55",
}


def crawl_row(context: Context, row: Dict[str, str]):
    entity = context.make("Person")
    name = row.pop("Imiona i nazwisk")
    birthplace = row.pop("Miejsce urodzenia")
    entity.id = context.make_id(birthplace, name)
    entity.add("name", name)
    entity.add("alias", row.pop("Pseudonim").split("\n"))
    birth_country = birthplace.split(",")[-1]
    entity.add("birthPlace", birthplace, lang="pol")
    entity.add("birthCountry", birth_country, lang="pol")
    h.apply_date(entity, "birthDate", row.pop("Data urodzenia"))
    entity.add("address", row.pop("location full") or None)
    entity.add("country", row.pop("location country") or None)

    entity.add("nationality", row.pop("narodowość"))
    entity.add("topics", "sanction")

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "listingDate", row.pop("Data umieszczenia na liście"))
    sanction.add(
        "reason", collapse_spaces(row.pop("Uzasadnienie wpisu na listę")), lang="pol"
    )
    sanction.add("program", POLAND_PROGRAM, "pol")

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row)


def check_updates(context: Context):
    doc = context.fetch_html(context.dataset.url)
    doc.make_links_absolute(context.dataset.url)
    materials = doc.findall(".//a[@class='file-download']")

    # Process the materials
    if len(materials) == 0:
        context.log.warning("No materials downloads found")
    else:
        for material in materials:
            url = material.get("href")
            if url in KNOWN_HASHES:
                h.assert_url_hash(context, url, KNOWN_HASHES[url])
            else:
                context.log.warning(
                    "Unknown materials download URL. Check if we want to scrape it.",
                    url=url,
                )

    # Assert the hash of the page content for <article class="article-area__article ">
    article = doc.find(".//article[@class='article-area__article ']")
    expected_page_hash = "726c2ff5c7f2964161b4a3529733b0d9ae812644"
    h.assert_dom_hash(article, expected_page_hash, raise_exc=True)


def crawl(context: Context):
    check_updates(context)

    path = context.fetch_resource("source.csv", context.data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            crawl_row(context, row)

    # UN Security Council stubs
    un_sc, doc = load_un_sc(context)

    for _node, entity in get_persons(context, un_sc.prefix, doc, UN_SC_PREFIXES):
        context.emit(entity)

    for _node, entity in get_legal_entities(context, un_sc.prefix, doc, UN_SC_PREFIXES):
        context.emit(entity)
