import csv
import os
from typing import Dict, Set, Any

from zavod import Context, Entity
from zavod.shed.internal_data import fetch_internal_data, list_internal_data

LOCAL_BUCKET_PATH = "/Users/leon/internal-data/"
PROCESSED_EGRUL_PREFIX = "ru_egrul/processed/current_2025-01-14/"


def emit_person(context: Context, row: dict[str, Any]) -> Entity:
    entity = context.make("Person")
    entity.id = row["id"]

    entity.add("name", row["first_name"])
    entity.add("firstName", row["first_name"])
    entity.add("fatherName", row["father_name"])
    entity.add("lastName", row["last_name"])
    entity.add("country", row["country"])
    entity.add("innCode", row["inn_code"])

    context.emit(entity)


def emit_legal_entity(
    context: Context, row: dict[str, Any], is_company: bool = False
) -> Entity:
    entity = context.make(row["schema"])

    entity.id = row["id"]
    entity.add("name", row["name"])
    entity.add("name", row["name_latin"])
    entity.add("name", row["name_full"])
    entity.add("name", row["name_short"])
    entity.add("jurisdiction", row["jurisdiction"])
    entity.add("country", row["country"])
    entity.add("innCode", row["inn_code"])
    entity.add("ogrnCode", row["ogrn_code"])
    entity.add("publisher", row["publisher"])
    entity.add("registrationNumber", row["registration_number"])
    entity.add("incorporationDate", row["incorporation_date"])
    entity.add("dissolutionDate", row["dissolution_date"])
    entity.add("legalForm", row["legal_form"])
    entity.add("email", row["email"])
    entity.add("address", row["address"])

    if row["schema"] == "Company":
        entity.add("kppCode", row["kpp_code"])

    context.emit(entity)


def emit_ownership(context: Context, row: Dict[str, Any]) -> None:
    entity = context.make("Ownership")

    entity.id = row["id"]
    entity.add("asset", row["asset_id"])
    entity.add("owner", row["owner_id"])
    entity.add("summary", row["summary_1"])
    entity.add("summary", row["summary_2"])
    entity.add("recordId", row["record_id"])
    entity.add("date", row["date"])
    entity.add("endDate", row["end_date"])
    entity.add("startDate", row["start_date"])
    entity.add("sharesCount", row["shares_count"])
    entity.add("percentage", row["percentage"])

    context.emit(entity)


def emit_directorship(context: Context, row: dict[str, Any]) -> None:
    entity = context.make("Directorship")
    entity.id = row["id"]
    entity.add("organization", row["organization_id"])
    entity.add("director", row["director_id"])
    entity.add("role", row["role"])
    entity.add("summary", row["summary"])
    entity.add("startDate", row["start_date"])
    entity.add("endDate", row["end_date"])

    context.emit(entity)


def emit_succession(context: Context, row: dict[str, Any]) -> None:
    entity = context.make("Succession")
    entity.id = row["id"]
    entity.add("predecessor", row["predecessor_id"])
    entity.add("successor", row["successor_id"])

    context.emit(entity)


def list_csv_in_internal_bucket(prefix: str) -> Set[str]:
    # For debug: use local paths
    # archives = [
    #     str(name)
    #     for name in Path(prefix)
    #     .glob("**/*.csv")
    # ]
    # return set(archives)
    return set(
        [
            blob_name
            for blob_name in list_internal_data(prefix)
            if blob_name.endswith(".csv")
        ]
    )


def emit_csv(context: Context, emit_fn, blob_name: str) -> None:
    # For debug: just use a local path
    # local_path = blob_name
    local_path = context.get_resource_path(blob_name)
    fetch_internal_data(blob_name, local_path)

    with open(local_path, "r", newline="") as fh:
        for row in csv.DictReader(fh):
            emit_fn(context, row)

    local_path.unlink(missing_ok=True)


def crawl(context: Context) -> None:
    file_prefix_to_emit_fn = [
        ("persons", emit_person),
        ("legalentities", emit_legal_entity),
        ("ownerships", emit_ownership),
        ("directorships", emit_directorship),
        ("successions", emit_succession),
    ]
    for blob_prefix, emit_fn in file_prefix_to_emit_fn:
        context.log.info(f"Crawling {blob_prefix}")
        for csv_blob in list_csv_in_internal_bucket(
            os.path.join(PROCESSED_EGRUL_PREFIX, blob_prefix)
        ):
            emit_csv(context, emit_fn, csv_blob)
