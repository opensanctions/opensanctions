import re
from datetime import datetime
from urllib.parse import quote, urljoin

from normality import collapse_spaces, slugify
import pdfplumber
from rigour.mime.types import PDF

from zavod import Context
from zavod import helpers as h
from zavod.util import Element

HEADERS = [
    "last_name",
    "first_name",
    "npi",
    "address_1",
    "address_2",
    "city",
    "state",
    "zip",
    "action_date",
    "excluded_terminated",
    "reason_for_exclusion_termination",
]


def flat(multiline: str | None) -> str:
    return (multiline or "").replace("\n", " ")


def crawl_item(row: dict[str, str | None], context: Context) -> None:
    address = h.make_address(
        context,
        street=flat(row.pop("address_1")),
        street2=flat(row.pop("address_2")),
        postal_code=(row.pop("zip") or "").replace("\n", ""),
        city=flat(row.pop("city")),
        state=flat(row.pop("state")),
        country_code="us",
    )
    npi = row.pop("npi")
    first_name = row.pop("first_name")
    last_name = row.pop("last_name")

    if first_name:
        entity = context.make("Person")
        entity.id = context.make_id(first_name, first_name, npi)
        h.apply_name(entity, first_name=flat(first_name), last_name=flat(last_name))
    else:
        entity = context.make("Company")
        entity.id = context.make_id(last_name, npi)
        entity.add("name", flat(last_name))

    entity.add("npiCode", h.multi_split(npi, [",", "\n"]))
    entity.add("topics", "debarment")
    entity.add("country", "us")
    h.apply_address(context, entity, address)

    sanction = h.make_sanction(context, entity)
    h.apply_date(
        sanction,
        "startDate",
        (row.pop("action_date") or "").replace("\n", ""),
        # Exclusions cannot predate the 1977 Medicare-Medicaid Anti-Fraud and Abuse
        # Amendments.
        two_digit_year_base=1977,
    )
    sanction.add("reason", flat(row.pop("reason_for_exclusion_termination")))
    sanction.add("provisions", flat(row.pop("excluded_terminated")))

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row)


def grid_rows(doc: Element) -> list[Element]:
    # The RadGrid alternates row classes, so both make up the listing.
    return h.xpath_elements(doc, "//tr[@class='rgRow' or @class='rgAltRow']")


def find_latest_pdf(context: Context) -> tuple[str, str]:
    """Return the URL of the latest monthly list and of the folder page listing it."""
    doc = context.fetch_html(context.data_url)
    folders: dict[int, str] = {}
    for row in grid_rows(doc):
        cells = [h.element_text(cell) for cell in h.xpath_elements(row, "./td")]
        year_text, folder_name = cells[1], cells[3]
        assert re.fullmatch(r"\d{4}", year_text), year_text
        assert folder_name.endswith(f"--{year_text}"), folder_name
        folders[int(year_text)] = folder_name
    assert len(folders) > 0, "No year folders found"

    # A new year's folder can exist before its first monthly list is uploaded.
    for year in sorted(folders, reverse=True):
        folder_url = urljoin(context.data_url, quote(folders[year]))
        folder = context.fetch_html(folder_url)
        files: dict[int, tuple[str, str]] = {}
        for row in grid_rows(folder):
            cells = [h.element_text(cell) for cell in h.xpath_elements(row, "./td")]
            doc_name, parent_folder = cells[1], cells[3]
            match = re.fullmatch(
                r"WV Provider Exclusions and Terminations (\w+) (\d{4})\.pdf", doc_name
            )
            assert match is not None, doc_name
            assert int(match.group(2)) == year, doc_name
            month = datetime.strptime(match.group(1), "%B").month
            assert month not in files, doc_name
            files[month] = (doc_name, parent_folder)
        if len(files) == 0:
            context.log.info("Year folder is empty", year=year)
            continue
        # Each monthly file is a full snapshot of the list.
        doc_name, parent_folder = files[max(files)]
        context.log.info("Using monthly list", doc_name=doc_name)
        pdf_url = urljoin(
            context.data_url,
            f"/SharepointDownload?parent={parent_folder}&docname={quote(doc_name)}",
        )
        return pdf_url, folder_url
    raise ValueError("No monthly list found in any year folder")


def crawl(context: Context) -> None:
    pdf_url, folder_url = find_latest_pdf(context)
    # The server answers with an empty body when the Referer is the top-level page.
    path = context.fetch_resource(
        "source.pdf", pdf_url, headers={"Referer": folder_url}
    )
    context.export_resource(path, PDF, title=context.SOURCE_TITLE)

    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            # The header row is repeated on every page, but a record spilling
            # over from the previous page can appear above the header row.
            # So let's assert that there's a header row somewhere on every page
            # and that it's ordered as expected for safety, but skip it when we see it.
            seen_header = False
            rows = page.extract_table({"text_x_tolerance": 1})
            assert rows is not None, page.page_number
            for row in rows:
                if row[0] == "Last Name":
                    slugified = [slugify(collapse_spaces(c), "_") for c in row if c]
                    assert slugified == HEADERS, slugified
                    seen_header = True
                    continue
                assert len(row) == len(HEADERS), row
                crawl_item(dict(zip(HEADERS, row)), context)
            assert seen_header, page.page_number
