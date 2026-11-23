import re

from zavod.stateful.positions import PositionCategorisation, categorise

from zavod import Context, Entity
from zavod import helpers as h


def crawl_row(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    line: str,
) -> None:
    # Nothing escapes commas inside a field, so a row with a different column
    # count can't be split reliably.
    raw_cells = line.split(",")
    cells = [cell.strip() for cell in raw_cells]
    if len(cells) != 8:
        # Only the leading cells are logged: the trailing ones are phone numbers.
        raise ValueError(f"Expected 8 columns, got {len(cells)}: {cells[:3]}")
    # The row number (`id`) is the position in the alphabetical list and shifts
    # whenever a senator is added or removed. The phone numbers (`tel1`, `tel2`)
    # are not extracted for PEPs.
    _row_number, first, last, gender, dob, birth_place, _tel1, _tel2 = cells

    # The page displays "prenom nom", but the two columns are swapped for some
    # senators (e.g. "BEBZOUNE BONGORO, THEOPHILE"), so we can't reliably use
    # apply_name()
    raw_name = f"{first} {last}"
    person = context.make("Person")
    person.id = context.make_id(first, last, dob)
    name = h.strip_name_titles(context, raw_name)
    h.apply_name(person, full=name)
    # Corrects genders the source gets wrong; see the lookup in the dataset YAML.
    override = context.lookup("gender_override", name)
    person.add("gender", override.value if override is not None else gender)
    # About a third of the birth dates are 1 January, the placeholder for a
    # year-only date, so keep only the year for those.
    h.apply_date(person, "birthDate", dob.removeprefix("01/01/"))
    person.add("birthPlace", birth_place)
    # Elected senators must be Chadian (Constitution of Chad 2023, Art. 115), but
    # the source doesn't tell them apart from the presidential appointees, for whom
    # the Constitution sets no such requirement.
    person.add("country", "td")

    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Senate of Chad",
        country="td",
        topics=["gov.legislative", "gov.national"],
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        context.log.warning("Position is not categorised as PEP", position=position.id)
        return
    context.emit(position)

    # The roster isn't in the page markup: a script feeding the map holds it as an
    # inline CSV in a JS template literal. The page isn't exported as a source
    # resource because that CSV carries the senators' personal phone numbers.
    text = context.fetch_text(context.data_url)
    assert text is not None, "Failed to fetch senators listing"
    matches = re.findall(r"const docx_senators_raw_data = `([^`]*)`;", text)
    if len(matches) != 1:
        raise ValueError(f"Expected one senators data block, found {len(matches)}")
    for line in matches[0].splitlines():
        crawl_row(context, position, categorisation, line)
