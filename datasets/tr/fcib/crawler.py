from normality import collapse_spaces
from openpyxl import load_workbook
from typing import Dict, List, Optional
import re
from rigour.mime.types import XLSX

from zavod import Context
from zavod import helpers as h
from zavod.extract.zyte_api import fetch_html, fetch_resource
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
# split only with value ##-## or ##/##
REGEX_SPLITTABLE_PASSPORT = re.compile(
    r"(^\d{5,}[-]\d{5,}$)|(^\d{5,}/\d{5,}(/\d{5,})?$)"
)

# https://masak.hmb.gov.tr/law-no-6415-on-the-prevention-of-the-financing-of-terrorism/#:~:text=(5)%20If%20natural%20and%20legal,following%20the%20date%20of%20request.
# ARTICLE 5- (1) Decisions on freezing of assets under the possession of persons,
# institutions and organisations designated through the United Nations Security
# Council Resolutions 1267(1999), 1988 (2011), 1989 (2011) and 2253 (2015) and
# decisions on the repeal of assets freezing for those who are de-listed shall
# be executed without delay through the decision of the President published in
# the Official Gazette.
#
# 1267 (1999) Taliban, Al-Qaida
# 1988 (2011) Taliban
# 1989 (2011) Al-Qaida
# 2253 (2015) ISIL (Daesh)
UN_SC_PREFIXES = [Regime.TALIBAN, Regime.DAESH_AL_QAIDA]


def split(text: Optional[str]) -> List[str]:
    if text is None:
        return []
    return [s.strip() for s in REGEX_SPLIT.split(text)]


def parse_passport_numbers(pass_no: Optional[str]) -> List[str]:
    if pass_no is None:
        return []
    if REGEX_SPLITTABLE_PASSPORT.match(pass_no):
        return h.multi_split(pass_no, ["-", "/"])
    else:
        return [pass_no]


def crawl_row(context: Context, row: Dict[str, str], program: str, url: str):
    name = row.pop("name")
    if not name:
        return  # in the XLSX file, there are empty rows

    pass_no = row.pop("passport_number", "")  # Person
    passport_other = row.pop("passport_number_other_info", "")  # LegalEntity
    birth_establishment_date = split(row.pop("date_of_birth_establishment", ""))
    birth_place = row.pop("birth_place", "")
    birth_dates = split(row.pop("birth_date"))
    gazette_date = row.pop("gazette_date")
    nationality = row.pop("nationality")
    mother_name = row.pop("mother_name")
    father_name = row.pop("father_name")
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
        entity.add(
            "passportNumber", parse_passport_numbers(collapse_spaces(passport_other))
        )
        entity.add("passportNumber", parse_passport_numbers(collapse_spaces(pass_no)))
        entity.add("position", row.pop("position", ""))
        entity.add("motherName", mother_name)
        entity.add("fatherName", father_name)
    else:
        entity = context.make("LegalEntity")
        entity.id = context.make_id(name, birth_place, nationality, passport_other)
        entity.add("name", row.pop("legal_entity_name", ""))
        id_number = collapse_spaces(passport_other)
        if id_number is not None and len(id_number) > 0:
            if id_number.startswith("IMO number:"):
                id_number = id_number.replace("IMO number:", "").strip()
                entity.add_schema("Organization")
                entity.add("imoNumber", id_number)
            elif id_number.startswith("SWIFT/BIC:"):
                id_number = id_number.replace("SWIFT/BIC:", "").strip()
                entity.add("swiftBic", id_number)
            else:
                entity.add("idNumber", id_number)
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
    listing_date = row.pop("listing_date", "")
    if listing_date is not None:
        listing_dates = listing_date.replace("\n", " ").split(" (", 1)
        h.apply_date(sanction, "listingDate", listing_dates[0])
        # Reviewed, revised
        h.apply_dates(sanction, "date", listing_dates[1:])
    h.apply_date(sanction, "listingDate", gazette_date)

    context.emit(entity)
    context.emit(sanction)
    context.audit_data(row, ignore=["sequence_no", "decision_date"])


def crawl_xlsx(context: Context, url: str, program: str, short_name: str):
    _, _, _, path = fetch_resource(context, f"{short_name}.xlsx", url, XLSX)
    context.export_resource(path, XLSX, title=f"{context.SOURCE_TITLE} - {short_name}")
    wb = load_workbook(path, read_only=True)
    for sheet in wb.worksheets:
        context.log.info(f"Processing sheet {sheet.title} with program {program}")
        for row in h.parse_xlsx_sheet(
            context, sheet, header_lookup=context.get_lookup("columns")
        ):
            crawl_row(context, row, program, url)


def crawl(context: Context):
    # Use browser to render javascript-based frontend
    table_xpath = './/table[@class="table table-bordered"]'
    doc = fetch_html(
        context, context.data_url, table_xpath, cache_days=3, absolute_links=True
    )
    table = doc.find(table_xpath)
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
        section_doc = fetch_html(context, url, table_xpath, cache_days=3)
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
        context.emit(entity)
    for _node, entity in get_legal_entities(context, un_sc.prefix, doc, UN_SC_PREFIXES):
        context.emit(entity)
