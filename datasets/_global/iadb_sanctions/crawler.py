from itertools import count

from zavod import Context
from zavod import helpers as h

FORMATS = ["%m/%d/%Y %H:00:00 AM"]


def parse_countries(countries):
    parsed = []
    for country in countries.split(", "):
        country = country.strip()
        if len(country):
            parsed.append(country)
    return parsed


def crawl(context: Context):
    for page in count(1):
        url = str(context.data_url)
        url = url.replace("pPageNumber=1", "pPageNumber=%s" % page)
        headers = {
            "Accept": "application/json",
            "Referer": "https://www.iadb.org/en/transparency/sanctioned-firms-and-individuals",
        }
        page_data = context.fetch_json(url, headers=headers)
        ids = []
        for row in page_data:
            for field, value in list(row.items()):
                if value == "N/A":
                    row[field] = ""
            row_id = row.pop("id")
            ids.append(row_id)
            entity_type = row.pop("entity")
            schema = context.lookup_value("types", entity_type)
            if schema is None:
                context.log.warning("Unknown entity type", entity=entity_type)
                continue
            entity = context.make(schema)
            entity.id = context.make_slug(row_id)
            entity.add("name", row.pop("firmName"))
            entity.add("topics", "debarment")
            entity.add("alias", row.pop("additionalName"))
            entity.add("notes", row.pop("title"))
            entity.add("notes", row.pop("additionalTitle"))
            entity.add("country", parse_countries(row.pop("country")))

            nat = "nationality"
            if schema == "Company":
                nat = "jurisdiction"
            entity.add(nat, parse_countries(row.pop("nationality")))

            affiliated = row.pop("affiliatedWithEntityId")
            if len(affiliated):
                other = context.make("LegalEntity")
                other.id = context.make_slug(affiliated)
                other.add("name", affiliated)
                link = context.make("UnknownLink")
                link.id = context.make_id(row_id, affiliated)
                link.add("subject", entity.id)
                link.add("object", other.id)
                context.emit(link)

            sanction = h.make_sanction(context, entity)
            sanction.add("status", row.pop("statusName"))
            sanction.add("reason", row.pop("grounds"))
            sanction.add("authority", row.pop("source"))
            sanction.add("authority", row.pop("idBinstSource"))
            sanction.add("program", row.pop("idBinstType"))
            sanction.add("startDate", h.parse_date(row.pop("datefrom"), FORMATS))
            sanction.add("endDate", h.parse_date(row.pop("dateto"), FORMATS))
            # context.inspect(row)

            context.emit(sanction)
            context.emit(entity, target=True)

        if min(ids) == 1:
            return
