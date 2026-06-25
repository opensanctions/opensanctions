from openpyxl import load_workbook
from rigour.mime.types import XLSX

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

GENDERS = {"Homme": "male", "Femme": "female"}


def crawl_member(
    context: Context,
    row: dict[str, str | None],
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    name = row.get("prenom_et_nom")
    if name is None:
        return

    person = context.make("Person")
    # The source has no stable per-deputy id; key on name and electoral district.
    constituency = row.get("nom_de_la_circonscription")
    person.id = context.make_id(name, constituency)
    h.apply_name(person, full=name, lang="fra")
    gender = row.get("genre")
    if gender is not None:
        person.add("gender", GENDERS[gender])
    person.add("political", row.get("appartenance_politique"), lang="fra")
    # The right to be elected to the House of Representatives is reserved to Moroccan
    # citizens (Constitution art. 30).
    # https://www.constituteproject.org/constitution/Morocco_2011
    person.add("citizenship", "ma")

    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return
    occupancy.add("constituency", constituency, lang="fra")
    occupancy.add(
        "politicalGroup", row.get("groupe_ou_groupement_parlementaire"), lang="fra"
    )

    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    path = context.fetch_resource("deputies.xlsx", context.data_url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    position = h.make_position(
        context,
        name="Member of the House of Representatives of Morocco",
        country="ma",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q21328583",
        lang="eng",
    )
    categorisation = categorise(context, position)
    context.emit(position)

    # Two parallel sheets ("ar"/"fr"); the French sheet is the complete Latin-script
    # roster (the Arabic sheet is one row short and has no shared key to join on).
    workbook = load_workbook(path, read_only=True)
    for row in h.parse_xlsx_sheet(context, workbook["fr"]):
        crawl_member(context, row, position, categorisation)
