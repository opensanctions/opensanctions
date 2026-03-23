import csv
from rigour.mime.types import CSV
from urllib.parse import urljoin
from typing import Dict, Optional

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise, OccupancyStatus


def crawl_row(context: Context, row: Dict[str, Optional[str]]) -> None:
    person = context.make("Person")
    person.id = context.make_id(row.pop("person_slug"))
    h.apply_name(
        person, first_name=row.pop("first_name"), last_name=row.pop("last_name")
    )
    person.add("title", row.pop("title"))

    url = urljoin("https://www.hatvp.fr", row.pop("dossier_url"))
    person.add("sourceUrl", url)  # url to person's dossier
    role = row.pop("role")
    assert role is not None
    person.add("position", role)
    person.add("citizenship", "fr")

    position_type = row.pop("position_type")

    # Skip parliamentarians and mayors, as they are covered by other
    # datasets and we don't want to duplicate them here.
    if position_type in (
        "senateur",
        "depute",
        "europe",
    ) or role.startswith("Maire"):
        return

    position = h.make_position(
        context,
        name=role,
        country="fr",
    )

    categorisation = categorise(context, position, is_pep=None)
    if not categorisation.is_pep:
        return

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        status=OccupancyStatus.UNKNOWN,
        no_end_implies_current=False,
    )
    if occupancy is None:
        return
    h.apply_date(occupancy, "declarationDate", row.pop("filing_date"))

    url_declaration_pdf = urljoin(
        "http://www.hatvp.fr/livraison/dossiers/", row.pop("file_name")
    )
    occupancy.add("sourceUrl", url_declaration_pdf)

    if occupancy is not None:
        context.emit(person)
        context.emit(position)
        context.emit(occupancy)

    context.audit_data(
        row,
        ignore=[
            "open_data",
            "publication_date",
            "photo_url",
            "publication_status",
            "document_type",
            "department",
            # only available for deputies and senators, which we skip
            "source_id",
        ],
    )


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url)
    url = h.xpath_string(
        doc,
        "//h4[contains(., 'Liens utiles')]/following::a[contains(@href, 'liste.csv')]/@href",
    )
    path = context.fetch_resource("source.csv", url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as f:
        dict_reader = csv.DictReader(f, delimiter=";")
        headers = {}
        for header in dict_reader.fieldnames or []:
            res = context.lookup("columns", header, warn_unmatched=True)
            headers[header] = res.value if res is not None else header
        for row in dict_reader:
            translated = {headers[k]: v for k, v in row.items()}
            crawl_row(context, translated)
