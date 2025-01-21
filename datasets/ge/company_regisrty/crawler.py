import csv
from normality.cleaning import collapse_spaces

from zavod import Context, helpers as h
from zavod.shed.internal_data import fetch_internal_data, list_internal_data


def crawl_row(context: Context, row: list) -> None:
    id = row.pop("id")
    name = row.pop("name")
    legal_form = row.pop("legal_form")
    if legal_form != "შეზღუდული პასუხისმგებლობის საზოგადოება":
        context.log.warning("Unknown company type", legal_form=legal_form)
    reg_date = row.pop("registration_date")
    email = row.pop("email")
    director = row.pop("director")
    partner = row.pop("partner")

    entity = context.make("Company")
    entity.id = context.make_id(id, name, reg_date)
    entity.add("name", name)
    entity.add("classification", legal_form)
    entity.add("incorporationDate", reg_date)
    entity.add("address", row.pop("address"))
    entity.add("status", row.pop("status"))
    if email != "NULL":
        for email in h.multi_split(email, [";", ","]):
            email = email.replace(" ", "").strip()
            entity.add("email", email)
    context.emit(entity)

    if director != "NULL":
        emit_rel(
            context,
            "Directorship",
            row,
            director,
            entity,
            "director_id",
            "director_citizenship",
            "director_start_date",
            "director",
        )
        context.audit_data(
            row,
            ignore=[
                "partner_id",
                "partner_citizenship",
                "partner_start_date",
                "partner_share",
            ],
        )
    if partner != "NULL":
        emit_rel(
            context,
            "Ownership",
            row,
            partner,
            entity,
            "partner_id",
            "partner_citizenship",
            "partner_start_date",
            "owner",
        )
        context.audit_data(
            row,
            ignore=[
                "director_id",
                "director_citizenship",
                "director_start_date",
            ],
        )


def emit_rel(
    context: Context,
    type_label: str,
    row: list,
    name: str,
    entity,
    id_key: str,
    citizenship_key: str,
    start_date_key: str,
    relationship_type: str,
):
    """Generalized function to process a director or partner."""
    person_id = row.pop(id_key)
    person_citizenship = row.pop(citizenship_key)
    person_start_date = row.pop(start_date_key)

    person = context.make("Person")
    person.id = context.make_id(name, person_id)
    person.add("name", name)
    if person_citizenship != "NULL":
        person.add("citizenship", person_citizenship)
    context.emit(person)

    relationship = context.make(type_label)
    relationship.id = context.make_id(person.id, relationship_type, entity.id)
    if person_start_date != "NULL":
        h.apply_date(relationship, "startDate", person_start_date)
    relationship.add(relationship_type, person)
    relationship.add(
        "organization" if relationship_type == "director" else "asset", entity
    )

    if relationship_type == "owner":
        relationship.add("percentage", row.pop("partner_share"))

    context.emit(relationship)


def crawl(context: Context) -> None:
    local_path = context.get_resource_path("companyinfo_v3.csv")
    for blob in list_internal_data("ge_ti_companies/"):
        if not blob.endswith(".csv"):
            continue

        fetch_internal_data(blob, local_path)
        context.log.info("Parsing: %s" % blob)

        with open(local_path, "r", encoding="utf-8") as fh:
            reader = csv.reader(fh, delimiter=";")
            original_headers = next(reader)

            # Translate headers to English
            header_mapping = [
                context.lookup_value("columns", collapse_spaces(cell))
                for cell in original_headers
            ]
            if len(header_mapping) != len(original_headers):
                context.log.warning("Mismatch between headers and row length.")
                return
            # Reset file pointer and skip original header
            fh.seek(0)
            next(fh)

            # Use DictReader with mapped headers
            dict_reader = csv.DictReader(fh, fieldnames=header_mapping, delimiter=";")
            for index, row in enumerate(dict_reader):
                crawl_row(context, row)
                if index >= 100000:
                    break
