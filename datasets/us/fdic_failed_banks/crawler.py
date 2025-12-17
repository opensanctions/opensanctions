import csv
from rigour.mime.types import CSV

from zavod import Context
from zavod import helpers as h
from typing import Dict


def clean_row(row: Dict[str, str]) -> Dict[str, str]:
    """Clean non-standard spaces from row keys and values."""
    return {
        k.replace("\xa0", " ").strip(): v.replace("\xa0", " ").strip()
        for k, v in row.items()
    }


def crawl_row(context: Context, row: Dict[str, str]):
    row = clean_row(row)  # Clean the row before processing

    # Ensure the row contains necessary data
    if not row:
        return

    bank_name = row.get("Bank Name", "").strip()
    closing_date = row.get("Closing Date", "").strip()

    if not bank_name or not closing_date:
        context.log.warning("Missing bank name or closing date", row=row)
        return

    acquiring_institution = row.get("Acquiring Institution", "").strip()
    entity = context.make("Company")
    entity.id = context.make_id(bank_name, closing_date)
    entity.add("name", bank_name)
    entity.add("topics", "fin.bank")
    entity.add("topics", "reg.warn")
    entity.add("jurisdiction", "us")
    entity.add("registrationNumber", row.get("Cert", "").strip())
    # entity.add("notes", f"Cert: {row.get('Cert', '')}")
    # entity.add("notes", f"Fund: {row.get('Fund', '')}")
    h.apply_date(entity, "dissolutionDate", closing_date)

    # FIXME: Remove acquiring institution handling for now because it
    # emits a lot of single-property entities that clutter the dataset and
    # create the perception that these banks, too, have failed:
    entity.add("notes", f"Acquiring Institution: {acquiring_institution}")

    # Check if acquiring_institution has a value
    # if acquiring_institution:
    #     succ_entity = context.make("Company")
    #     succ_entity.id = context.make_id(acquiring_institution)
    #     succ_entity.add("name", acquiring_institution)
    #     succ = context.make("Succession")
    #     succ.id = context.make_id(entity.id, "successor", succ_entity.id)

    #     succ.add("predecessor", entity.id)
    #     succ.add("successor", succ_entity.id)

    #     context.emit(succ)
    #     context.emit(succ_entity)

    address = h.format_address(
        city=row.pop("City"),
        state=row.pop("State"),
        country_code="us",
    )
    entity.add("address", address)

    context.emit(entity)
    # context.log.info(f"Emitted entity for bank: {bank_name}")


def crawl(context: Context):
    context.log.info("Fetching data from FDIC Failed Bank List")

    # Fetch the CSV file from the source URL
    source_path = context.fetch_resource("source.csv", context.data_url)

    # Register the CSV file as a resource with the dataset
    context.export_resource(source_path, CSV, title="FDIC Failed Banks CSV file")

    # Attempt to open the CSV file with different encodings

    with open(source_path, "r", encoding="latin-1") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            crawl_row(context, row)

    # context.log.info("Finished processing FDIC Failed Bank List")
