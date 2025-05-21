from zavod import Context, helpers as h
from zavod.shed import zyte_api


def crawl(context: Context) -> None:
    data = zyte_api.fetch_json(context, context.data_url, geolocation="us")
    for row in data["data"]:
        first_name = row.pop("First Name")
        last_name = row.pop("Last Name")
        npi = row.pop("NPI")

        if first_name:
            entity = context.make("Person")
            entity.id = context.make_id(last_name, first_name, npi)
            h.apply_name(entity, first_name=first_name, last_name=last_name)
        else:
            entity = context.make("Company")
            entity.id = context.make_id(last_name, npi)
            entity.add("name", last_name)

        entity.add("npiCode", npi)
        entity.add("topics", "debarment")
        entity.add("country", "us")

        sanction = h.make_sanction(context, entity)
        h.apply_date(sanction, "startDate", row.pop("Effective Date"))
        sanction.add("reason", row.pop("Reason"))

        context.emit(entity)
        context.emit(sanction)

        context.audit_data(row)
