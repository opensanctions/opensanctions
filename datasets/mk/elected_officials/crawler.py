from typing import Dict
import requests

from zavod import Context
from zavod import helpers as h
from zavod.logic.pep import categorise

IGNORE_COLUMNS = [
    "formType",
    "subInstitution",
]


def crawl_row(context: Context, row: Dict[str, str]):
    person = context.make("Person")

    person.id = context.make_slug(row.pop("id"))

    h.apply_name(
        person,
        first_name=row.pop("name"),
        last_name=row.pop("lastName"),
    )

    position_name = row.pop("workingPosition")
    position_institution = row.pop("institution")
    position = h.make_position(
        context, f"{position_name}, {position_institution}", country="mk", lang="mkd"
    )

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
        context.emit(person, target=True)
        context.emit(position)
        context.emit(occupancy)

    context.audit_data(row, IGNORE_COLUMNS)


def crawl(context: Context):
    page = 0
    while True:
        # the API maximum number of records per page is 2000
        params = {"page": page, "size": 5}
        res = requests.post(
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
