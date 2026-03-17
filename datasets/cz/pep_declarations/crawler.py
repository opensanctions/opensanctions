from typing import Dict, Any

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise


IGNORE = [
    "fullName",
    "active",
    "concatenatedWorkingPositions",
    "concatenatedWorkingPositionDates",
    "concatenatedWorkingPositionOrganizations",
    "deputy",
    "senator",
    "deputyAndOthers",
    "senatorAndOthers",
    "workingFrom",
    "workingTo",
    "judge",
]


def crawl_person(context: Context, item: Dict[str, Any]) -> None:
    person_id = item.pop("id")
    first_name = item.pop("firstName")
    last_name = item.pop("lastName")
    middle_name = item.pop("middleName", None)

    entity = context.make("Person")
    entity.id = context.make_id(person_id)
    h.apply_name(
        entity,
        first_name=first_name,
        middle_name=middle_name,
        last_name=last_name,
    )
    entity.add("title", item.pop("titleBefore", None))
    entity.add("nameSuffix", item.pop("titleAfter", None))
    entity.add("citizenship", "cz")
    for wp in item.pop("workingPositions", []):
        wp_data = wp.pop("workingPosition")
        # is_deputy and is_senator are position-level flags
        is_deputy = wp_data.pop("deputy")
        is_senator = wp_data.pop("senator")
        # Only deputies and senators are marked as PEP by default
        if is_deputy or is_senator:
            is_pep = True
        else:
            is_pep = None

        role_name = wp_data.pop("name")
        org_name = wp.pop("organization")
        # Construct a clean position_name:
        # e.g. Poslanec, Kancelář Poslanecké sněmovny Parlamentu
        position_name = (
            f"{role_name.capitalize()}, {org_name}" if org_name else role_name
        )
        position = h.make_position(
            context,
            name=position_name,
            country="cz",
            lang="ces",
        )
        entity.add("position", position_name)

        categorisation = categorise(context, position, is_pep=is_pep)
        if not categorisation.is_pep:
            continue

        occupancy = h.make_occupancy(
            context,
            person=entity,
            position=position,
            start_date=wp.pop("start"),
            # writtenDateOfEnd and dateOfEnd: look like the written/formal
            # submission date not the actual end date of the position
            end_date=wp.pop("end", None),
            categorisation=categorisation,
        )
        if occupancy is not None:
            context.emit(
                entity,
                origin=f"https://cro.justice.cz/verejnost/api/funkcionari/{person_id}",
            )
            context.emit(position)
            context.emit(occupancy)

    context.audit_data(item, ignore=IGNORE)


def crawl(context: Context) -> None:
    page = 0
    while True:
        url = f"{context.data_url}?sort=created&order=DESC&page={page}&pageSize=100"
        data = context.fetch_json(url)
        assert data is not None, "Expected JSON response"

        items = data.pop("items")
        if not items:
            break

        for item in items:
            crawl_person(context, item)

        page += 1
