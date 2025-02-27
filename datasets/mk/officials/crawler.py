from typing import Dict, List

from zavod import Context
from zavod import helpers as h
from zavod.logic.pep import categorise

IGNORE_COLUMNS = [
    "id",
    "formType",
    "subInstitution",
    "otherInstitution",
]


def make_position_name(position, institution) -> List[str]:
    institution_langs = institution.split("\n") if institution else []

    if position is None and institution is not None:
        return [i.strip() for i in institution_langs]
    if position is not None and institution is None:
        return [position.strip()]
    if position and institution:
        return [f"{position.strip()}, {i.strip()}" for i in institution_langs]
    raise ValueError("No position or institution")


def crawl_row(context: Context, row: Dict[str, str]):
    person = context.make("Person")

    first_name = row.pop("name")
    last_name = row.pop("lastName")
    position_name = row.pop("workingPosition")
    position_institution = row.pop("institution")
    person.id = context.make_id(
        first_name, last_name, position_name, position_institution
    )

    h.apply_name(
        person,
        first_name=first_name,
        last_name=last_name,
    )
    position_names = make_position_name(position_name, position_institution)
    position = h.make_position(
        context,
        position_names[0],
        country="mk",
        lang="mkd",
    )
    position.add("name", position_names[1:])

    categorisation = categorise(context, position, is_pep=True)

    if not categorisation.is_pep:
        return

    start_date = row.pop("dateNaming")
    end_date = row.pop("dateTermination")
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        no_end_implies_current=False,
        start_date=start_date,
        end_date=end_date,
        categorisation=categorisation,
    )

    if occupancy:
        context.emit(person)
        context.emit(position)
        context.emit(occupancy)

    context.audit_data(row, IGNORE_COLUMNS)


def crawl(context: Context):
    page = 0
    while True:
        # the API maximum number of records per page is 2000
        params = {"page": page, "size": 2000}
        res = context.http.post(
            context.dataset.data.url,
            params=params,
            json={"naming": True, "termination": True},
        )
        data = res.json()

        if data.get("empty"):
            break

        for row in data.get("content"):
            crawl_row(context, row)

        page += 1
