import csv
from urllib.parse import urljoin

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise, OccupancyStatus
from rigour.mime.types import CSV


def crawl_row(context, row: dict) -> None:
    first_name = row.pop("prenom")
    last_name = row.pop("nom")
    role = row.pop("qualite")

    person = context.make("Person")
    person.id = context.make_id(first_name, last_name, role)
    h.apply_name(person, first_name=first_name, last_name=last_name)
    person.add("title", row.pop("civilite"))
    person.add("idNumber", row.pop("id_origine"))

    url = urljoin("https://www.hatvp.fr", row.pop("url_dossier"))
    person.add("sourceUrl", url)  # url to person's dossier

    person.add("position", role)
    person.add("citizenship", "fr")

    mandate_type = row.pop("type_mandat")

    # Skip parliamentarians and mayors, as they are covered by other
    # datasets and we don't want to duplicate them here.
    if mandate_type in (
        "senateur",
        "depute",
        "europe",
    ) or mandate_type.startswith("Maire"):
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
    h.apply_date(occupancy, "declarationDate", row.pop("date_depot"))

    url_declaration_pdf = urljoin(
        "http://www.hatvp.fr/livraison/dossiers/", row.pop("nom_fichier")
    )
    occupancy.add("sourceUrl", url_declaration_pdf)

    if occupancy is not None:
        context.emit(person)
        context.emit(position)
        context.emit(occupancy)

    context.audit_data(
        row,
        ignore=[
            "classement",
            "open_data",
            "date_publication",
            "url_photo",
            "statut_publication",
            "type_document",
            "departement",
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
        for row in dict_reader:
            crawl_row(context, row)
