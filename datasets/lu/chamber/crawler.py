import csv
import re

from rigour.mime.types import CSV
from urllib.parse import urljoin

from zavod import Context, helpers as h, Entity
from zavod.stateful.positions import categorise

ENDPOINTS = {
    "current": "la-liste-des-deputes-actifs-a-la-chambre-des-deputes-du-luxembourg/",
    "historical": "la-liste-des-anciens-deputes-de-la-chambre-des-deputes-du-luxembourg/",
}
# Matches placeholder names like "CSV 15" or "déi gréng 02" used when the
# source lacks the actual candidate name for a historical seat.
PLACEHOLDER_RE = re.compile(r"\d+$")


def crawl_row(context: Context, row: dict[str, str], position: Entity) -> None:
    first_name = row.pop("FIRSTNAME")
    last_name = row.pop("NAME")
    # Skip placeholder entries for historical seats that lack the actual candidate name.
    if "Siège" in first_name and PLACEHOLDER_RE.search(last_name):
        return
    dob = row.pop("BIRTH_DATE")

    entity = context.make("Person")
    entity.id = context.make_id(first_name, last_name, dob)
    h.apply_name(entity, first_name=first_name, last_name=last_name)
    entity.add("gender", row.pop("PERSON_TITLE"))
    entity.add("political", row.pop("POLITICAL_GROUP"))
    entity.add("political", row.pop("POLITICAL_PARTY"))
    entity.add("citizenship", "lu")
    h.apply_date(entity, "birthDate", dob)

    categorisation = categorise(context, position, is_pep=True)
    if not categorisation.is_pep:
        return

    occupancy = h.make_occupancy(
        context,
        person=entity,
        position=position,
        start_date=row.pop("START_DATE"),
        end_date=row.pop("END_DATE", None),
        categorisation=categorisation,
    )
    if occupancy is not None:
        context.emit(occupancy)
        context.emit(position)
        context.emit(entity)

    context.audit_data(row, ["PHONE_EXT", "MOBILE", "EMAIL", "ADDRESS"])


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Deputy of the Chamber of Deputies of Luxembourg",
        wikidata_id="Q21328592",
        country="lu",
        topics=["gov.legislative", "gov.national"],
        lang="eng",
    )
    context.emit(position)

    for resource_name, endpoint in ENDPOINTS.items():
        # data.public.lu per-dataset metadata endpoint; "resources" lists downloadable files.
        data = context.fetch_json(urljoin(context.data_url, endpoint), cache_days=5)
        csv_resources = [
            r for r in data.get("resources", []) if r.get("format") == "csv"
        ]
        assert len(csv_resources) == 1, (endpoint, len(csv_resources)
        csv_resource = csv_resources[0]

        path = context.fetch_resource(
            f"{resource_name}_deputies.csv", csv_resource["url"]
        )
        context.export_resource(path, CSV, title=context.SOURCE_TITLE)
        with open(path, "r", encoding="utf-8-sig") as fh:
            rows = list(csv.DictReader(fh))
            if "current" in resource_name:
                # The Chamber is made up of 60 seats.
                assert len(rows) >= 55, len(rows)
            for row in rows:
                crawl_row(context, row, position)
