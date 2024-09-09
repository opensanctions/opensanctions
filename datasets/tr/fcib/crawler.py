from lxml import etree
from normality import collapse_spaces
from openpyxl import load_workbook
from typing import Dict, List, Optional
import re
from pantomime.types import XLSX

from zavod import Context
from zavod import helpers as h
from zavod.shed.zyte_api import fetch_html
from zavod.shed.un_sc import Regime, get_legal_entities, get_persons, load_un_sc

DATE_FORMAT = ["%d.%m.%Y", "%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"]

# original link for assertion
DOCX_LINK = "https://ms.hmb.gov.tr/uploads/sites/2/2024/08/A-BIRLESMIS-MILLETLER-GUVENLIK-KONSEYI-KARARINA-ISTINADEN-MALVARLIKLARI-DONDURULANLAR-6415-SAYILI-KANUN-5.-MADDEw.docx"
XLSX_LINKS = [
    (
        "https://ms.hmb.gov.tr/uploads/sites/2/2024/05/B-YABANCI-ULKE-TALEPLERINE-ISTINADEN-MALVARLIKLARI-DONDURULANLAR-6415-SAYILI-KANUN-6.-MADDE.xlsx",
        "Asset freezes pursuant to Article 6 of Law No. 6415, targeting individuals"
        " and entities based on requests made by foreign governments.",
        "B - Foreign government requests",
    ),
    (
        "https://ms.hmb.gov.tr/uploads/sites/2/2024/09/C-IC-DONDURMA-KARARI-ILE-MALVARLIKLARI-DONDURULANLAR-6415-SAYILI-KANUN-7.-MADDE-2.xlsx",
        "Asset freezes pursuant to Article 7 of Law No. 6415, targeting individuals"
        " and entities through domestic legal actions and decisions.",
        "C - Domestic legal actions",
    ),
    (
        "https://ms.hmb.gov.tr/uploads/sites/2/2024/05/D-7262-SAYILI-KANUN-3.A-VE-3.B-MADDELERI.xlsx",
        "Asset freezes within the scope of Articles 3.A and 3.B of Law No. 7262,"
        " aimed at preventing the financing of proliferation of weapons of mass destruction.",
        "D - Prevention of proliferation of weapons of mass destruction",
    ),
]

# Exclude newlines to avoid splitting addresses unless they're numbered
REGEX_SPLIT = re.compile(r",?\s*\b\w[\.\)]")
REGEX_GAZZETE_DATE = re.compile(r"(\d{2}\.\d{2}\.\d{4})")
UN_SC_PREFIXES = [Regime.TALIBAN, Regime.DAESH_AL_QAIDA]


def split(text: Optional[str]) -> List[str]:
    if text is None:
        return []
    return [s.strip() for s in REGEX_SPLIT.split(text)]


def crawl_row(context: Context, row: Dict[str, str], program: str, url: str):
    name = row.pop("name")
    if not name:
        return  # in the XLSX file, there are empty rows

    pass_no = row.pop("passport_number", "")  # Person
    passport_other = row.pop("passport_number_other_info", "")  # LegalEntity
    birth_establishment_date = split(row.pop("date_of_birth_establishment", ""))
    birth_place = row.pop("birth_place", "")
    birth_dates = split(row.pop("birth_date", ""))
    gazette_date = row.pop("gazette_date", "")
    nationality = row.pop("nationality", "")
    mother_name = row.pop("mother_name", "")
    father_name = row.pop("father_name", "")
    if gazette_date:
        matched_date = REGEX_GAZZETE_DATE.search(gazette_date)
        gazette_date = matched_date.group(0) if matched_date else ""

    # Birthplace is also used for organisations
    if birth_dates or mother_name or father_name:
        entity = context.make("Person")
        entity.id = context.make_id(
            name, nationality, birth_dates, birth_place, pass_no
        )
        entity.add("nationality", nationality)
        entity.add("nationality", row.pop("other_nationality", ""))
        entity.add("birthPlace", birth_place)
        h.apply_dates(entity, "birthDate", birth_dates)
        h.apply_dates(entity, "birthDate", birth_establishment_date)
        entity.add("passportNumber", collapse_spaces(passport_other))
        entity.add("passportNumber", collapse_spaces(pass_no))
        entity.add("position", row.pop("position", ""))
        entity.add("motherName", mother_name)
        entity.add("fatherName", father_name)
    else:
        entity = context.make("LegalEntity")
        entity.id = context.make_id(name, birth_place, nationality, passport_other)
        entity.add("name", row.pop("legal_entity_name", ""))
        entity.add("idNumber", collapse_spaces(passport_other))
        h.apply_dates(entity, "incorporationDate", birth_establishment_date)
        entity.add("description", row.pop("position", ""))
        entity.add("country", nationality)

    entity.add("name", name)
    entity.add("alias", split(row.pop("alias")))
    entity.add("previousName", split(row.pop("previous_name", "")))
    entity.add("address", split(row.pop("address", "")))
    entity.add("notes", row.pop("other_information", ""))
    entity.add("topics", "sanction")

    sanction = h.make_sanction(context, entity)
    sanction.add("description", row.pop("sanction_type", ""))
    sanction.add("reason", row.pop("organization", ""))
    sanction.add("program", program)  # depends on the xlsx file
    sanction.add("sourceUrl", url)
    h.apply_date(sanction, "listingDate", row.pop("listing_date", None))
    h.apply_date(sanction, "listingDate", gazette_date)

    context.emit(entity, target=True)
    context.emit(sanction)
    context.audit_data(row, ignore=["sequence_no", "decision_date"])


def crawl_xlsx(context: Context, url: str, program: str, short_name: str):
    path = context.fetch_resource(f"{short_name}.xlsx", url)
    context.export_resource(
        path,
        XLSX,
        title=f"{context.SOURCE_TITLE} - {short_name}",
    )
    wb = load_workbook(path, read_only=True)
    for sheet in wb.worksheets:
        context.log.info(f"Processing sheet {sheet.title} with program {program}")
        for row in h.parse_xlsx_sheet(context, sheet, header_lookup="columns"):
            crawl_row(context, row, program, url)


def unblock_validator(doc: etree._Element) -> bool:
    return doc.find('.//table[@class="table table-bordered"]') is not None


def crawl(context: Context):
    # Use browser to render javascript-based frontend
    doc = fetch_html(context, context.data_url, unblock_validator, cache_days=3)
    doc.make_links_absolute(context.data_url)
    table = doc.find('.//table[@class="table table-bordered"]')
    category_urls = [link.get("href") for link in table.findall(".//a")]

    found_links = set()

    for url in category_urls:
        section_doc = fetch_html(context, url, unblock_validator, cache_days=3)

        # Determine whether to search for .docx or .xlsx based on URL
        if "5madde_ing" in url:
            doc_links = section_doc.xpath('//a[contains(@href, ".docx")]')
        else:
            doc_links = section_doc.xpath('//a[contains(@href, ".xlsx")]')

        for doc_link in doc_links:
            found_links.add(doc_link.get("href"))

    expected_links = set([DOCX_LINK, *(link for link, _, _ in XLSX_LINKS)])
    # Check if new links have appeared
    new_links = found_links - expected_links
    assert not new_links, f"Unexpected links found: {new_links}"
    # Check if all expected links are found
    missing_links = expected_links - found_links
    assert not missing_links, f"Expected links not found: {missing_links}"

    # the actual crawling part if all links are verified
    context.log.info("Fetching data from the provided XLSX links")
    for url, program, short in XLSX_LINKS:
        context.log.info(f"Processing URL: {url}")
        crawl_xlsx(context, url, program, short)
    context.log.info("Finished processing the Excel files")

    # UN Security Council stubs
    un_sc, doc = load_un_sc(context)

    for _node, entity in get_persons(context, un_sc.prefix, doc, UN_SC_PREFIXES):
        context.emit(entity, target=True)

    for _node, entity in get_legal_entities(context, un_sc.prefix, doc, UN_SC_PREFIXES):
        context.emit(entity, target=True)
