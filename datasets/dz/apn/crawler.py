import json
import re
from itertools import count
from typing import Any

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import (
    OccupancyStatus,
    PositionCategorisation,
    categorise,
)

# The public /nuwab page is a JavaScript single-page app; the deputy roster is
# loaded from this paginated JSON POST endpoint, PAGE_SIZE records per page.
PAGE_SIZE = 100

# The API rejects the POST unless it carries a JSON Content-Type (415 otherwise) and
# a French Accept-Language (400 otherwise); the browser User-Agent it also requires
# (500 otherwise) is set via http.user_agent in the dataset metadata.
HEADERS = {
    "Accept-Language": "fr",
    "Content-Type": "application/json",
}

# Seats whose holder the APN does not disclose are listed with a row of dashes
# instead of a name (`nom`, `prenom` and `fullName` are all `-----`). There is no
# person to record for those rows.
PLACEHOLDER_NAME = re.compile(r"^[\s-]+$")


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    record: dict[str, Any],
) -> None:
    full_name = record["fullName"]
    district = record["wilaya"]
    assert full_name is not None and district is not None
    if PLACEHOLDER_NAME.match(full_name):
        return

    person = context.make("Person")
    person.id = context.make_id(full_name, district)
    person.add("name", full_name)
    h.apply_name(
        person,
        first_name=record["prenom"],
        last_name=record["nom"],
        lang="eng",
    )
    if record["parti"]:
        person.add("political", record["parti"])
    # A candidate for the APN must be of Algerian nationality (Organic Law 21-01 on
    # the electoral regime, Article 200; 2020 Constitution Article 128).
    # https://cour-constitutionnelle.dz/wp-content/uploads/2023/02/loi%20-electFR.pdf
    person.add("citizenship", "dz")

    # A recorded seat vacancy (death, resignation or a declared incompatibility)
    # means the deputy no longer sits; their occupancy has ended.
    status = OccupancyStatus.ENDED if record["vacancesiege"] is not None else None
    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation, status=status
    )
    if occupancy is None:
        return
    occupancy.add("constituency", district)
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the People's National Assembly of Algeria",
        country="dz",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q21290886",
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    # Fetch pages until the API returns an empty batch; the expected roster size is
    # enforced by the dataset assertions.
    for page in count():
        payload = json.dumps({"size": PAGE_SIZE, "page": page})
        data = context.fetch_json(
            context.data_url,
            method="POST",
            data=payload,
            headers=HEADERS,
            cache_days=7,
        )
        rows = data["data"]
        if len(rows) == 0:
            break
        for record in rows:
            crawl_member(context, position, categorisation, record)
