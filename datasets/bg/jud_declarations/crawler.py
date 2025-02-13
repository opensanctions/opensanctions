import re
import pdfplumber
from lxml.html import HtmlElement
from rigour.mime.types import PDF
from normality import slugify
from typing import Dict, Generator, List, Optional

from zavod import Context, helpers as h
from zavod.logic.pep import categorise, OccupancyStatus
from zavod.shed.trans import apply_translit_full_name, make_position_translation_prompt


TRANSLIT_OUTPUT = {"eng": ("Latin", "English")}
POSITION_PROMPT = prompt = make_position_translation_prompt("bul")


def extract_judicial_declaration(context, url, doc_id_date) -> Dict[str, Optional[str]]:
    """Extract name, role, and organization from the first page of a judicial declaration PDF."""

    pdf_path = context.fetch_resource(f"{doc_id_date}.pdf", url)
    context.export_resource(pdf_path, PDF)

    extracted_data = {}
    try:
        with pdfplumber.open(pdf_path) as pdf:
            first_page = pdf.pages[0]
            full_text = first_page.extract_text() or ""

            # Regex patterns for extracting relevant fields
            patterns = {
                # Capture up until a newline or end of text
                "name": r"Име:\s*([А-Яа-яЁё\s-]+)(?=\n|$)",
                "role": r"Длъжност:\s*([А-Яа-яЁё\s-]+)(?=\n|$)",
                "organization": r"Орган на съдебната власт:\s*([А-Яа-яЁё\s,.-]+)(?=\n|$)",
            }

            for key, pattern in patterns.items():
                match = re.search(pattern, full_text)
                extracted_data[key] = match.group(1).strip() if match else None
    except Exception as e:
        context.log.warning(f"Skipping {pdf_path} due to error: {e}")
        return {}

    context.log.info(f"Extracted Data: {extracted_data}")
    return extracted_data


def parse_html_table(
    table: HtmlElement,
    skiprows: int = 0,
    headers: List[str] = [],
) -> Generator[Dict[str, HtmlElement], None, None]:

    first_row = table.find(".//tr[1]")
    actual_headers = [td.text_content().strip() for td in first_row.findall("./td")]
    assert len(actual_headers) == len(
        headers
    ), f"Header length mismatch: {actual_headers}"

    for rownum, row in enumerate(table.findall(".//tr")):
        if rownum < skiprows:
            continue
        cells = row.findall("./td")
        yield {hdr: c for hdr, c in zip(headers, cells)}


def crawl_row(context: Context, row: Dict[str, HtmlElement]):
    str_row = h.cells_to_str(row)
    name = str_row.pop("name")
    doc_id_date = str_row.pop("doc_id_date")
    # Skip the header row
    if name == "Име" and doc_id_date == "Входящ номер":
        return
    assert re.match(r"^[\u0400-\u04FF]", name), f"Invalid name format: {name}"
    assert re.match(r"^\d", doc_id_date), f"Invalid doc_id_date format: {doc_id_date}"

    if len(doc_id_date.split("/")) == 2:
        _, date = doc_id_date.split("/")
    else:
        date = context.lookup_value("doc_id_date", doc_id_date)
        if date is None:
            context.log.warning(f"Invalid doc_id_date: {doc_id_date}")
        return
    # Link is in the same cell as the name
    name_link_elem = HtmlElement(row["name"]).find(".//a")
    declaration_url = name_link_elem.get("href")
    extracted_data = extract_judicial_declaration(
        context,
        declaration_url,
        doc_id_date,
    )
    if not extracted_data:
        return
    name_dec = extracted_data.pop("name")
    if slugify(name, sep="-") != slugify(name_dec, sep="-"):
        context.log.warning(f"Name mismatch: {name} (HTML) != {name_dec} (PDF)")
    role = extracted_data.pop("role")
    organization = extracted_data.pop("organization")
    if organization is None:
        context.log.warning(
            f"Missing organization for {name}", declaration_url=declaration_url
        )
        return
    # Common pattern: "organization - city"
    parts = organization.split(" - ")
    if len(parts) == 2:
        organization = parts[0].strip()
        city = parts[1].strip()
    else:
        organization = organization
        city = None

    position_name = f"{role}, {organization}"
    person = context.make("Person")
    # We want the same person for 2 different years to have the same ID
    person.id = context.make_id(name, role, city)
    person.add("name", name, lang="bul")
    person.add("topics", "role.pep")
    person.add("sourceUrl", declaration_url)
    person.add("position", position_name, lang="bul")

    position = h.make_position(
        context,
        name=position_name,
        lang="bul",
        country="BG",
    )
    apply_translit_full_name(
        context, position, "bul", position_name, TRANSLIT_OUTPUT, POSITION_PROMPT
    )

    categorisation = categorise(context, position, is_pep=True)
    if not categorisation.is_pep:
        return

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        no_end_implies_current=False,
        categorisation=categorisation,
        status=OccupancyStatus.UNKNOWN,
    )
    # Switch to the declarationDate once it's introduced
    h.apply_date(occupancy, "date", date)

    if occupancy is not None:
        context.emit(position)
        context.emit(occupancy)
        context.emit(person)


def crawl(context: Context):
    doc = context.fetch_html(context.data_url, cache_days=1)
    alphabet_links = doc.xpath(".//div[@itemprop='articleBody']/p//a[@href]")
    assert len(alphabet_links) >= 58, "Expected at least 58 links in `alphabet_links`."
    # Bulgarian alphabet has 30 letters, but the name can start only with 29 of them
    # We want to cover the last 2 years at any time
    for a in alphabet_links[:58]:
        doc.make_links_absolute(context.data_url)
        link = a.get("href")
        doc = context.fetch_html(link, cache_days=1)
        table = doc.xpath(".//table")
        if len(table) == 0:
            context.log.info("No tables found")
            continue
        assert len(table) == 1
        table = table[0]
        for row in parse_html_table(table, headers=["name", "doc_id_date"]):
            doc.make_links_absolute(context.data_url)
            crawl_row(context, row)
