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
    address = row.pop("address")
    email = row.pop("email")
    status = row.pop("status")
    director = row.pop("director")
    director_id = row.pop("director_id")
    director_citizenship = row.pop("director_citizenship")
    director_start_date = row.pop("director_start_date")
    partner = row.pop("partner")
    partner_share = row.pop("partner_share")
    partner_id = row.pop("partner_id")
    partner_citizenship = row.pop("partner_citizenship")
    partner_start_date = row.pop("partner_start_date")

    entity = context.make("Company")
    entity.id = context.make_id(id, name, reg_date)
    entity.add("name", name)
    # entity.add("legalForm", legal_form)
    entity.add("registrationDate", reg_date)
    entity.add("address", address)
    if email != "NULL":
        entity.add("email", email)
    entity.add("status", status)
    context.emit(entity)

    if director is not None:
        dir = context.make("Person")
        dir.id = context.make_id(director_id)
        dir.add("name", director)
        if director_citizenship != "NULL":
            dir.add("citizenship", director_citizenship)
        context.emit(dir)

        directorship = context.make("Directorship")
        directorship.id = context.make_id(dir.id, "director", entity.id)
        if director_start_date != "NULL":
            h.apply_date(directorship, "startDate", director_start_date)
        directorship.add("director", dir)
        directorship.add("organization", entity)
        context.emit(directorship)

    if partner is not None:
        partner = context.make("Person")
        partner.id = context.make_id(partner_id)
        partner.add("name", partner)
        if partner_citizenship != "NULL":
            partner.add("citizenship", partner_citizenship)
        context.emit(partner)

        own = context.make("Ownership")
        own.id = context.make_id(partner.id, "owns", entity.id)
        if partner_start_date != "NULL":
            h.apply_date(own, "startDate", partner_start_date)
        own.add("asset", entity)
        own.add("owner", partner)
        own.add("percentage", partner_share)
        context.emit(own)


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
                if index >= 10000:
                    break
