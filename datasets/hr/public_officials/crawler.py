import csv

from rigour.mime.types import CSV

from zavod import Context, Entity
from zavod import helpers as h
from zavod.stateful.positions import categorise

# We predefine expected column headings because the source data repeats
# the column headings for the tenure details. We use predefined distinct field
# names for extraction, but verify that the columns match what we assume
# them to be when reading the CSV.

EXPECTED_COLUMNS_OBLIGATED = [
    "Ime",
    "Prezime",
    "Primarna dužnost",
    "Pravna osoba u kojoj obnaša dužnost",
    "Datum početka obnašanja dužnosti",
    "Datum kraja obnašanja dužnosti",
    "Sekundarna dužnost",
    "Pravna osoba u kojoj obnaša dužnost",
    "Datum početka obnašanja dužnosti",
    "Datum kraja obnašanja dužnosti",
]

EXPECTED_COLUMNS_CIVIL_SERVANTS = [
    "Ime",
    "Prezime",
    "Dužnost",
    "Pravna osoba u kojoj obnaša dužnost",
    "Datum početka obnašanja dužnosti",
    "Datum kraja obnašanja dužnosti",
    "Dužnost",
    "Pravna osoba u kojoj obnaša dužnost",
    "Datum početka obnašanja dužnosti",
    "Datum kraja obnašanja dužnosti",
]

# Define readable column names
FIELDS = [
    "first_name",
    "last_name",
    "primary_position",
    "primary_legal_entity",
    "primary_position_start_date",
    "primary_position_end_date",
    "secondary_position",
    "secondary_legal_entity",
    "secondary_position_start_date",
    "secondary_position_end_date",
]


def build_position_name(data: dict[str, str]) -> str | None:
    """Builds the full Croatian position name from the title and legal entity name.

    When there's no title we fall back to the Croatian "Nepoznata dužnost" ("Unknown
    position") so the whole name stays in one language and can be handed to
    make_position for translation in a single pass. Returns None when there is neither
    a title nor a legal entity.
    """
    title = data.pop("position")
    legal_entity_name = data.pop("legal_entity")
    if title is None and legal_entity_name is None:
        return None

    # Use the Croatian "Unknown position" placeholder so the whole name is one
    # language and make_position can translate it (and key the ID off it) in a single
    # pass. This does mean the placeholder ends up in the name's original_value even
    # though it isn't literally from the source, but that's not catastrophic.
    # The alternative is a whole dance: translate only the legal entity name, then
    # prepend an English "Unknown position" ourselves, and set the name, lang and
    # translation origin by hand rather than letting make_position do it.
    position_name = title or "Nepoznata dužnost"
    if legal_entity_name:
        position_name = f"{position_name}, {legal_entity_name}"
    return position_name


def make_affiliation_entities(
    context: Context,
    person: Entity,
    *,
    position_name: str | None,
    position_data: dict[str, str],
) -> list[Entity]:
    """Creates Position and Occupancy provided that the Occupancy meets OpenSanctions criteria.
    * A position's name include the title and optionally the name of the legal entity
    * A position with a legal entity but no title is titled 'Unknown position'
    * All positions (and Occupancies, Persons) are assumed to be Croatian
    * Positions with start and/or end date but no position name or legal entity name are discarded
    """
    if position_name is None or position_name.strip() == "":
        return []

    start_date = position_data.pop("position_start_date")
    end_date = position_data.pop("position_end_date")
    context.audit_data(position_data)

    # The name is Croatian; make_position keys the ID on the untranslated name (so it
    # stays stable) and stores the English translation for display.
    position = h.make_position(
        context, position_name, country="HR", lang="hrv", translate_name=True
    )

    categorisation = categorise(context, position, default_is_pep=True)
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        start_date=start_date,
        end_date=end_date,
    )
    entities = []
    if occupancy:
        entities.extend([position, occupancy])
    return entities


def make_person(
    context: Context,
    *,
    first_name: str,
    last_name: str,
    primary_position_name: str | None,
    secondary_position_name: str | None,
) -> Entity:
    positions = sorted(
        p for p in [primary_position_name, secondary_position_name] if p is not None
    )
    position = positions[0] if positions else None
    person = context.make("Person")
    person.id = context.make_id(first_name, last_name, position)

    # citizenship for state service is not required as per Article 52:
    # https://www.zakon.hr/z/108/zakon-o-drzavnim-sluzbenicima
    person.add("country", "hr")
    h.apply_name(person, first_name=first_name, last_name=last_name)
    return person


def dict_keys_by_prefix(data: dict[str, str], prefix: str) -> dict[str, str]:
    """
    Returns a new dict with keys and values from the original matching the prefix.
    The prefix is removed from the keys in the new dict.
    """
    return {
        k.removeprefix(prefix): data.pop(k)
        for k in list(data.keys())
        if k.startswith(prefix)
    }


def crawl_row(context: Context, row: dict[str, str]) -> None:
    position_entities = []

    primary_position_data = dict_keys_by_prefix(row, "primary_")
    primary_position_name = build_position_name(primary_position_data)
    secondary_data = dict_keys_by_prefix(row, "secondary_")
    secondary_position_name = build_position_name(secondary_data)

    person = make_person(
        context,
        first_name=row.pop("first_name"),
        last_name=row.pop("last_name"),
        primary_position_name=primary_position_name,
        secondary_position_name=secondary_position_name,
    )

    position_entities.extend(
        make_affiliation_entities(
            context,
            person,
            position_name=primary_position_name,
            position_data=primary_position_data,
        )
    )
    position_entities.extend(
        make_affiliation_entities(
            context,
            person,
            position_name=secondary_position_name,
            position_data=secondary_data,
        )
    )

    context.audit_data(row)
    if position_entities:
        for entity in position_entities:
            context.emit(entity)
        context.emit(person)


def crawl_file(
    context: Context,
    *,
    url: str,
    filename: str,
    fields: list[str],
    expected_headings: list[str],
) -> None:
    path = context.fetch_resource(filename, url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)

    with open(path, encoding="utf-8-sig") as fh:
        headings = next(csv.reader(fh, delimiter=";"))
        assert headings == expected_headings, (url, headings)
        reader = csv.DictReader(fh, fieldnames=fields, delimiter=";")
        for row in reader:
            crawl_row(context, row)


def crawl(context: Context) -> None:
    # Register of appointed civil servants
    #
    # It'd be nice to pass position topics like gov.admin here, but
    # the secondary positions or positions with entity but no role label
    # might get mis-tagged.
    crawl_file(
        context,
        url="https://www.sukobinteresa.hr/export/registar_rukovodecih_drzavnih_sluzbenika_koje_imenuje_vlada_republike_hrvatske.csv",
        filename="appointed.csv",
        fields=FIELDS,
        expected_headings=EXPECTED_COLUMNS_CIVIL_SERVANTS,
    )
    # Register of obligors
    crawl_file(
        context,
        url="https://www.sukobinteresa.hr/export/registar_duznosnika.csv",
        filename="obligated.csv",
        fields=FIELDS,
        expected_headings=EXPECTED_COLUMNS_OBLIGATED,
    )
