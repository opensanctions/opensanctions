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

# original link for assertion
DOCX_LINK = "https://ms.hmb.gov.tr/uploads/sites/2/2024/08/A-BIRLESMIS-MILLETLER-GUVENLIK-KONSEYI-KARARINA-ISTINADEN-MALVARLIKLARI-DONDURULANLAR-6415-SAYILI-KANUN-5.-MADDEw.docx"

# Mapping of slug to full label (program) and short label
LABEL_MAPPING = {
    "6MADDE_ING": (
        "Asset freezes pursuant to Article 6 of Law No. 6415, targeting individuals and entities based on requests made by foreign governments.",
        "B - Foreign government requests",
    ),
    "7MADDE_ING": (
        "Asset freezes pursuant to Article 7 of Law No. 6415, targeting individuals and entities through domestic legal actions and decisions.",
        "C - Domestic legal actions",
    ),
    "3A3B": (
        "Asset freezes within the scope of Articles 3.A and 3.B of Law No. 7262, aimed at preventing the financing of the proliferation of weapons of mass destruction.",
        "D - Prevention of weapons of mass destruction proliferation",
    ),
}


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
    entity.add("topics", "sanction.counter")

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

    # Ensure exactly 4 links are found
    if len(category_urls) != 4:
        context.log.warning(
            f"Expected to find 4 category URLs, but found {len(category_urls)}"
        )

    found_bcd_links = []

    # Process each category URL
    for url in category_urls:
        if "5madde_ing" in url:
            # Skip the "A" section (UN Security Council list)
            context.log.info("Skipping A section (UN Security Council list)")
            continue

        # Fetch the content of the section
        section_doc = fetch_html(context, url, unblock_validator, cache_days=3)
        doc_links = section_doc.xpath('//a[contains(@href, ".xlsx")]')

        # Expect exactly one .xlsx link in each section
        if len(doc_links) != 1:
            context.log.warning(
                f"Expected to find 1 .xlsx link in section {url}, but found {len(doc_links)}"
            )

        # Extract the link
        doc_link = doc_links[0].get("href")

        # Extract the slug (e.g., 6MADDE_ING) from the filename or URL structure
        slug = url.split("/")[-1].upper()

        # Ensure we have a mapping for the letter
        if slug not in LABEL_MAPPING:
            context.log.error(f"Unexpected slug found: {slug}")
            continue

        # Get the program and short label from the mapping
        program, short_label = LABEL_MAPPING[slug]

        # Log and store the found links
        context.log.info(f"Processing {short_label} - URL: {doc_link}")
        found_bcd_links.append((doc_link, program, short_label))

    # Ensure we found exactly 3 links for B, C, and D sections
    if len(found_bcd_links) != 3:
        context.log.warning(
            f"Expected to find 3 (6MADDE_ING, 7MADDE_ING, 3A3B) sections, but found {len(found_bcd_links)}"
        )

    # Process each found link
    for url, program, short in found_bcd_links:
        context.log.info(f"Processing URL: {url} - Program: {program}")
        # Call the crawl_xlsx function to process the link
        crawl_xlsx(context, url, program, short)

    context.log.info("Finished processing the Excel files")

    # UN Security Council stubs
    un_sc, doc = load_un_sc(context)
    for _node, entity in get_persons(context, un_sc.prefix, doc, UN_SC_PREFIXES):
        context.emit(entity, target=True)
    for _node, entity in get_legal_entities(context, un_sc.prefix, doc, UN_SC_PREFIXES):
        context.emit(entity, target=True)
