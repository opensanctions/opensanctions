import csv

from rigour.mime.types import CSV
from urllib.parse import urljoin

from zavod import Context, helpers as h, Entity
from zavod.stateful.positions import categorise

# data.public.lu dataset slug for the active deputies list. The catalog republishes
# the CSV daily under a new timestamped URL, so we resolve the current resource via
# the metadata API on every run rather than hard-coding a download link.
DATASET_SLUG = "la-liste-des-deputes-actifs-a-la-chambre-des-deputes-du-luxembourg/"


def crawl_row(context: Context, row: dict[str, str], position: Entity) -> None:
    first_name = row.pop("pph_prenom")
    last_name = row.pop("pph_nom")
    dob = row.pop("pph_date_naissance")

    entity = context.make("Person")
    entity.id = context.make_id(first_name, last_name, dob)
    h.apply_name(entity, first_name=first_name, last_name=last_name)
    entity.add("gender", row.pop("per_titre"))
    entity.add("political", row.pop("rattachement_abrv"))
    entity.add("citizenship", "lu")
    h.apply_date(entity, "birthDate", dob)

    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return

    constituency = row.pop("derniere_circonscription")
    occupancy = h.make_occupancy(
        context,
        person=entity,
        position=position,
        start_date=row.pop("date_debut_depute"),
        categorisation=categorisation,
    )
    if occupancy is not None:
        occupancy.add("constituency", constituency)
        context.emit(occupancy)
        context.emit(position)
        context.emit(entity)

    # rattachement_type ("Groupe politique"/"Sensibilité politique") describes the
    # group's parliamentary standing, which has no FollowTheMoney property, so it is
    # audited but not emitted.
    context.audit_data(
        row,
        [
            "rattachement_type",
            "address",
            "phone_ext",
            "phone_mobile",
            "email",
        ],
    )


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

    # The per-dataset metadata endpoint; "resources" lists the downloadable files.
    # Not cached: the catalog republishes the CSV daily under a new timestamped URL
    # and deletes the old one, so a stale resource URL would 404 on the next download.
    data = context.fetch_json(urljoin(context.data_url, DATASET_SLUG))
    csv_resources = [r for r in data.get("resources", []) if r.get("format") == "csv"]
    assert len(csv_resources) == 1, len(csv_resources)
    csv_resource = csv_resources[0]

    path = context.fetch_resource("deputies.csv", csv_resource["url"])
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    # The source exports as Windows-1252, not UTF-8. Automatic detection is unreliable
    # here: byte 0xe8 is valid in several single-byte codepages, so a detector mistakes
    # the French/Luxembourgish text for cp1250 and yields mojibake.
    with open(path, encoding="cp1252") as fh:
        rows = list(csv.DictReader(fh))
        # The Chamber has 60 seats.
        assert len(rows) >= 55, len(rows)
        for row in rows:
            crawl_row(context, row, position)
