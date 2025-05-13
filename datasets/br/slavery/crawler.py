import csv
import re
from typing import Dict

from rigour.ids import CNPJ, CPF
from rigour.mime.types import CSV

from zavod import Context, helpers as h

LISTING_INTERVAL_RE = re.compile(r"(?P<start_date>.+) a (?P<end_date>.+)")


def crawl_row(context: Context, row: Dict[str, str]):
    tax_number = row.pop("CNPJ/CPF")

    if CNPJ.is_valid(tax_number):
        entity = context.make("Company")
        tax_number = CNPJ.normalize(tax_number)
        entity.id = context.make_slug(tax_number, prefix="br-cnpj")
    elif CPF.is_valid(tax_number):
        entity = context.make("Person")
        tax_number = CPF.normalize(tax_number)
        entity.id = context.make_slug(tax_number, prefix="br-cpf")
    else:
        entity = context.make("LegalEntity")
        entity.id = context.make_id(tax_number)

    entity.add("taxNumber", tax_number)
    entity.add("country", "br")
    entity.add("sector", row.pop("CNAE"))

    entity.add("name", row.pop("Empregador"))

    address = row.pop("Estabelecimento").strip()
    # If address is just "RESIDÊNCIA <name>", use only state as address
    if address.startswith("RESIDÊNCIA"):
        entity.add("address", row.pop("UF"))
    else:
        entity.add("address", address)
    entity.add("topics", "reg.action")
    entity.add("topics", "risk.forced.labor")

    # Sometimes the listing date looks like this "05/10/2022 a 13/01/2023, 11/11/2024", indicating that the
    # company was removed and re-added to the list.
    for listing_interval in row.pop("Inclusão no Cadastro de Empregadores").split(", "):
        sanction = h.make_sanction(context, entity)

        listing_interval_match = LISTING_INTERVAL_RE.match(listing_interval)
        if listing_interval_match:
            h.apply_date(
                sanction, "startDate", listing_interval_match.groupdict()["start_date"]
            )
            h.apply_date(
                sanction, "endDate", listing_interval_match.groupdict()["end_date"]
            )
        else:
            # This is just a normal date string, indicating an ongoing listing
            h.apply_date(sanction, "startDate", listing_interval)

        context.emit(sanction)

    context.audit_data(
        row,
        ignore=[
            "ID",  # We don't trust their IDs to remain stable
            # Directly tanslates to "fiscal action" and contains a year. Most likely this refers to a date of first
            # inspection that eventually lead to the inclusion in this list.
            "Ano da ação fiscal",
            "UF",  # State (almost always also part of the address)
            "CNAE",  # The primary business activity code of the company
            "Trabalhadores envolvidos",  # Number of workers involved in case
            "Decisão administrativa de procedência",  # Date of decision for inclusion in this list
        ],
    )
    context.emit(entity)


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r", encoding="latin-1") as fh:
        # Skip garbage at beginning of file
        while True:
            prev_pos = fh.tell()
            line = fh.readline()
            if line == "":
                # End of file reached without finding a header
                context.log.error("No CSV header found in file")
                return
            if line.startswith("ID;"):
                # Seek to the beginning of the line we just read
                fh.seek(prev_pos)
                break

        for row in csv.DictReader(fh, delimiter=";"):
            # Skip garbage at the end of the file
            if not row["ID"].isnumeric():
                continue

            crawl_row(context, row)
