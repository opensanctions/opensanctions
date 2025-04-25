import csv
from typing import Dict

from zavod import Context, Entity
import zavod.helpers as h
from nomenklatura.resolver import Linker
from zavod.integration import get_dataset_linker


def crawl_row(
    context: Context, linker: Linker[Entity], row_idx: int, row: Dict[str, str]
) -> None:
    """Process one row of the CSV data"""
    row_id = row.pop("List ID").strip(" \t.")
    entity_type = row.pop("Type").strip()
    name = row.pop("Name").strip()
    country = row.pop("Country").strip()
    reg_number = row.pop("registrationNumber").strip()

    context.log.info(f"Processing row ID {row_id}: {name}")
    entity = context.make(entity_type)
    entity.id = context.make_id(row_id, name, country)
    context.log.debug(f"Unique ID {entity.id}")
    canonical_id = linker.get_canonical(entity.id)
    for other_id in linker.get_referents(canonical_id):
        if other_id.startswith("eu-fsf-"):
            context.log.warning(
                f"Row {row_idx} is also present in FSF XML: {other_id}, can be removed from sheet",
                row_id=row_id,
                other_id=other_id,
                name=name,
                entity_type=entity_type,
                country=country,
            )

    entity.add("topics", "sanction")
    dob = row.pop("DOB")
    if entity.schema.is_a("Organization"):
        h.apply_dates(entity, "incorporationDate", h.multi_split(dob, ";"))
    elif entity.schema.is_a("Person"):
        h.apply_dates(entity, "birthDate", h.multi_split(dob, ";"))
    entity.add("birthPlace", row.pop("POB"), quiet=True)
    entity.add("country", h.multi_split(country, ";"))
    entity.add("name", h.multi_split(name, ";"))
    entity.add("alias", h.multi_split(row.pop("Alias"), ";"))
    entity.add_cast("Person", "passportNumber", h.multi_split(row.pop("passport"), ";"))
    entity.add("taxNumber", h.multi_split(row.pop("taxNumber"), ";"), quiet=True)
    entity.add("idNumber", h.multi_split(row.pop("idNumber"), ";"), quiet=True)
    entity.add("imoNumber", row.pop("imoNumber"), quiet=True)
    entity.add("notes", row.pop("Notes").strip())
    entity.add("position", h.multi_split(row.pop("Position", None), ";"), quiet=True)
    entity.add("address", h.multi_split(row.pop("Address", None), ";"), quiet=True)
    entity.add("gender", row.pop("Gender", None), quiet=True)
    entity.add("sourceUrl", h.multi_split(row.pop("Source URL"), ";"))
    if "ru" in entity.get("country"):
        entity.add("ogrnCode", h.multi_split(reg_number, ";"))
    else:
        entity.add("registrationNumber", h.multi_split(reg_number, ";"))

    for related_name in h.multi_split(row.pop("related"), ";"):
        related = context.make("LegalEntity")
        related.id = context.make_id(related_name, entity.id)
        related.add("name", related_name)

        rel = context.make("UnknownLink")
        rel.id = context.make_id(related.id, entity.id)
        rel.add("subject", related)
        rel.add("object", entity)

        context.emit(related)
        context.emit(rel)

    sanction = h.make_sanction(context, entity)
    start_date = row.pop("startDate")
    h.apply_date(sanction, "startDate", start_date)

    for public_key in h.multi_split(row.pop("crypto wallet"), [";"]):
        wallet = context.make("CryptoWallet")
        wallet.id = context.make_id(public_key)
        wallet.add("publicKey", public_key)
        wallet.add("holder", entity)
        wallet.add("topics", "sanction")

        wallet_sanction = h.make_sanction(context, wallet)
        h.apply_date(wallet_sanction, "startDate", start_date)

        context.emit(wallet)
        context.emit(wallet_sanction)

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row)


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    linker = get_dataset_linker(context.dataset)
    with open(path, "rt") as infh:
        for idx, row in enumerate(csv.DictReader(infh)):
            crawl_row(context, linker, idx, row)
