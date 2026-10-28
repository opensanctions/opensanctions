from itertools import count
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
    slug = row.pop("slug")

    person = context.make("Person")
    person.id = context.make_id(str(row.pop("id")))
    h.apply_name(person, first_name=row.pop("prenom"), last_name=row.pop("nom"))
    person.add("gender", row.pop("genre"))
    party = row.pop("parti_politique")
    person.add("political", context.lookup_value("political", party, default=party))
    person.add("profession", row.pop("profession_avant_mandat"))
    person.add("sourceUrl", urljoin(context.data_url, f"/depute/{slug}/"))
    # Deputies must be Malagasy citizens: Loi organique n° 2018-010 du 11 mai 2018
    # relative à l'élection des Députés à l'Assemblée nationale, art. 8.
    # https://cnlegis.gov.mg/page_acces_pdf/eoVfpUisR44
    person.add("citizenship", "mg")

    # The source gives no mandate dates anywhere, so this status is the only
    # signal of whether a deputy still sits.
    status = context.lookup("occupancy_status", row.pop("status"))
    assert status is not None  # the lookup is required
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        status=None if status.value is None else OccupancyStatus(status.value),
    )
    if occupancy is None:
        return
    occupancy.add("constituency", row.pop("district"))
    group = row.pop("groupe_parlementaire")
    if group is not None:
        group_name = group.pop("nom")
        # "Non_Affilié" marks a deputy outside any parliamentary group.
        if group_name != "Non_Affilié":
            occupancy.add("politicalGroup", group_name)
        # The member's office within the caucus, which has no FollowTheMoney
        # property.
        context.audit_data(group, ignore=["fonction"])
    context.emit(occupancy)
    context.emit(person)

    context.audit_data(
        row,
        ignore=[
            # Seat index in the chamber, cleared once a deputy leaves.
            "numero",
            # The administrative areas containing the district already captured
            # as the constituency.
            "region",
            "province",
            "photo_url",
            # Contact details are not extracted for PEPs.
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

    for page in count(1):
        # per_page is silently capped at 200: a larger value is accepted and
        # quietly truncated, so page through in smaller steps instead.
        data = context.fetch_json(
            context.data_url,
            params={"page": page, "per_page": 100},
            cache_days=1,
        )
        for row in data["data"]:
            crawl_deputy(context, position, categorisation, row)
        if not data["pagination"]["has_next"]:
            break
