from typing import Any
from urllib.parse import urljoin

from zavod import Context, Entity
from zavod import helpers as h
from zavod.stateful.positions import (
    OccupancyStatus,
    PositionCategorisation,
    categorise,
)


def crawl_deputy(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    row: dict[str, Any],
) -> None:
    person = context.make("Person")
    person.id = context.make_id(row.pop("id"))
    h.apply_name(person, first_name=row.pop("prenom"), last_name=row.pop("nom"))
    person.add("gender", row.pop("genre"))
    party = row.pop("parti_politique")
    person.add("political", context.lookup_value("political", party, default=party))
    person.add("profession", row.pop("profession_avant_mandat"))
    person.add("sourceUrl", urljoin(context.data_url, f"/depute/{row.pop('slug')}/"))
    # Loi organique n° 2018-010, art. 8: https://cnlegis.gov.mg/page_acces_pdf/eoVfpUisR44
    person.add("citizenship", "mg")

    status = context.lookup("occupancy_status", row.pop("status"))
    assert status is not None  # the lookup is required
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        status=OccupancyStatus(status.value),
    )
    if occupancy is None:
        return
    occupancy.add("constituency", row.pop("district"))
    group = row.pop("groupe_parlementaire")
    if group is not None:
        group_name = group.pop("nom")
        # No parliamentary group.
        if group_name != "Non_Affilié":
            occupancy.add("politicalGroup", group_name)
        # The office held within the group.
        context.audit_data(group, ignore=["fonction"])
    context.emit(occupancy)
    context.emit(person)

    context.audit_data(
        row,
        ignore=[
            # Seat index, cleared once a deputy leaves.
            "numero",
            # Areas containing the district.
            "region",
            "province",
            "photo_url",
            # Contact details.
            "facebook_url",
            "instagram_url",
            "linkedin_url",
            "twitter_url",
        ],
    )


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the National Assembly of Madagascar",
        wikidata_id="Q21295989",
        country="mg",
        topics=["gov.national", "gov.legislative"],
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    # per_page above 200 is silently truncated; the roster fits in one page.
    data = context.fetch_json(context.data_url, params={"per_page": 200}, cache_days=1)
    assert not data["pagination"]["has_next"], data["pagination"]
    for row in data["data"]:
        crawl_deputy(context, position, categorisation, row)
