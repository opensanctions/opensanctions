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

from typing import Dict

from normality import collapse_spaces
from openpyxl import load_workbook

from rigour.mime.types import XLSX
from zavod import Context
from zavod import helpers as h
from zavod.shed.un_sc import get_legal_entities, get_persons, Regime, load_un_sc

POLAND_PROGRAM = "art. 118 ustawy z dnia 1 marca 2018 r. o przeciwdziałaniu praniu pieniędzy i finansowaniu terroryzmu"
UN_SC_PREFIXES = [Regime.TALIBAN, Regime.DAESH_AL_QAIDA]

PSEUDONYM_SPLITS = ["a) ", "b) ", "c) "]


def crawl_row(context: Context, row: Dict[str, str]):
    entity = context.make("Person")
    name = row.pop("imiona_i_nazwiska")
    birthplace = row.pop("miejsce_urodzenia")
    entity.id = context.make_id(birthplace, name)
    entity.add("name", name)
    entity.add("alias", h.multi_split(row.pop("pseudonim"), PSEUDONYM_SPLITS))
    birth_country = birthplace.split(",")[-1]
    entity.add("birthPlace", birthplace, lang="pol")
    entity.add("birthCountry", birth_country, lang="pol")
    h.apply_date(entity, "birthDate", row.pop("data_urodzenia"))

    other_information = row.pop("inne_informacje")
    res = context.lookup("other_information", other_information)
    if not res:
        context.log.warning("No extra information lookup found", row=row)
    else:
        entity.add(
            "nationality",
            res.properties.get("nationality"),
            lang="pol",
            original_value=other_information,
        )
        entity.add(
            "country",
            res.properties.get("country"),
            lang="pol",
            original_value=other_information,
        )
        entity.add(
            "address",
            res.properties.get("address"),
            lang="pol",
            original_value=other_information,
        )

    entity.add("topics", "sanction")
    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "listingDate", row.pop("data_umieszczenia_na_liscie"))
    sanction.add(
        "reason",
        collapse_spaces(row.pop("uzasadnienie_wpisu_na_liste")),
        lang="pol",
    )
    sanction.add(
        "sourceUrl",
        row.pop("uzasadnienie_wpisu_na_liste_url"),
        lang="pol",
    )
    sanction.add("program", POLAND_PROGRAM, "pol")

    context.emit(entity, target=True)
    context.emit(sanction)

    context.audit_data(
        row,
        ignore=[
            "lp",  # ID, we don't trust this to be stable
        ],
    )


def crawl(context: Context):
    doc = context.fetch_html(context.dataset.url)
    doc.make_links_absolute(context.dataset.url)

    xlsx_link_element = doc.xpath(
        ".//a[@class='file-download' and contains(text(), 'art. 118 ust. 1 pkt 2 (wersja xlsx)')]"
    )
    if len(xlsx_link_element) != 1:
        raise RuntimeError("Could not find XLSX link element")

    xlsx_url = xlsx_link_element[0].get("href")

    # Assert the hash of the page content for <article class="article-area__article ">
    article = doc.find(".//article[@class='article-area__article ']")
    expected_page_hash = "30aca6ba4b245649db4bee16e0798d661080bd9a"
    if not h.assert_dom_hash(article, expected_page_hash, text_only=True):
        context.log.warning(
            "Page hash has changed. Verify that the lists referenced on the page are the 118 ust. 1 pkt 2, the UNSC Taliban list (resolution 1988 (2011)), and UNSC Al Qaida list (resolution 2253 (2015)). If any lists have been added or removed, update the crawler, otherwise just update the hash"
        )

    path = context.fetch_resource("source.xlsx", xlsx_url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    wb = load_workbook(path)
    assert len(wb.worksheets) == 1
    for row in h.parse_xlsx_sheet(context, sheet=wb.worksheets[0], extract_links=True):
        crawl_row(context, row)

    # UN Security Council stubs
    un_sc, doc = load_un_sc(context)

    for _node, entity in get_persons(context, un_sc.prefix, doc, UN_SC_PREFIXES):
        context.emit(entity, target=True)

    for _node, entity in get_legal_entities(context, un_sc.prefix, doc, UN_SC_PREFIXES):
        context.emit(entity, target=True)
