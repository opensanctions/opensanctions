import csv
from zavod import Context
from zavod import helpers as h
from typing import Dict


def convert_date(date_str: str) -> list[str]:
    """Convert various date formats to 'YYYY-MM-DD'."""
    formats = [
        "%B %d, %Y",  # 'Month DD, YYYY' format
        "%d-%b-%y",  # 'DD-MMM-YY' format
    ]
    return h.parse_date(date_str, formats, default=None)


def parse_table(table: HtmlElement) -> Generator[Dict[str, str], None, None]:
    headers = None
    for row in table.findall(".//tr"):
        if headers is None:
            headers = []
            for el in row.findall("./th"):
                # Workaround because lxml-stubs doesn't yet support HtmlElement
                # https://github.com/lxml/lxml-stubs/pull/71
                eltree = cast(HtmlElement, el)
                headers.append(slugify(eltree.text_content()))
            continue

        cells = [collapse_spaces(el.text_content()) for el in row.findall("./td")]
        assert len(headers) == len(cells), (headers, cells)
        yield {hdr: c for hdr, c in zip(headers, cells)}


def crawl_row(context: Context, row: Dict[str, str]):
    row = clean_row(row)  # Clean the row before processing

    # Ensure the row contains necessary data
    if not row:
        return

    bank_name = row.get("Bank Name", "").strip()
    closing_date = row.get("Closing Date", "").strip()
    closing_date_iso = convert_date(closing_date)  # Convert closing date to ISO format

    if not bank_name or not closing_date:
        context.log.warning("Missing bank name or closing date", row=row)
        return

    acquiring_institution = row.get("Acquiring Institution", "").strip()
    entity = context.make("Company")
    entity.id = context.make_id(bank_name, closing_date)
    entity.add("name", bank_name)
    entity.add("topics", "fin.bank")
    entity.add("topics", "reg.warn")
    entity.add("notes", f"Cert number {row.get('Cert', '')}")
    entity.add("dissolutionDate", closing_date_iso)
    entity.add("notes", f"Fund: {row.get('Fund', '')}")

    # Check if acquiring_institution has a value
    if acquiring_institution:
        succ_entity = context.make("Company")
        succ_entity.id = context.make_id(acquiring_institution)
        succ_entity.add("name", acquiring_institution)
        succ = context.make("Succession")
        succ.id = context.make_id(entity.id, "successor", succ_entity.id)

        succ.add("predecessor", entity.id)
        succ.add("successor", succ_entity.id)

        context.emit(succ)
        context.emit(succ_entity)

    address = h.make_address(
        context,
        city=row.pop("City"),
        state=row.pop("State"),
        country_code="us",
    )
    h.copy_address(entity, address)

    context.emit(entity, target=True)
    context.log.info(f"Emitted entity for bank: {bank_name}")


def crawl(context: Context):
    context.log.info("Fetching data from FDIC Failed Bank List")

    # Fetch the CSV file from the source URL
    source_path = context.fetch_resource("source.csv", context.data_url)

    # Register the CSV file as a resource with the dataset
    context.export_resource(source_path, "text/csv", title="FDIC Failed Banks CSV file")

    # Attempt to open the CSV file with different encodings

    with open(source_path, "r", encoding="latin-1") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            crawl_row(context, row)

    context.log.info("Finished processing FDIC Failed Bank List")
