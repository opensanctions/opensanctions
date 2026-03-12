from typing import Dict, Any

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise


IGNORE = [
    "image",
    "wikiTitle",
    "fullName",
    "pId",
    "showAs",
]
HOUSE_TITLES = {"seanad": "Senator", "dail": "Teachtaí Dála"}


def crawl_member(context: Context, member: Dict[str, Any]) -> None:
    first_name = member.pop("firstName")
    last_name = member.pop("lastName")
    id = member.pop("memberCode")

    person = context.make("Person")
    person.id = context.make_id(first_name, last_name, id)
    h.apply_name(person, first_name=first_name, last_name=last_name)
    person.add("country", "ie")

    person.add("deathDate", member.pop("dateOfDeath", None))
    person.add("gender", member.pop("gender", None))
    person.add("sourceUrl", member.pop("uri", None))

    for membership_meta in member.get("memberships", []):
        membership = membership_meta["membership"]

        for party in membership["parties"]:
            person.add("political", party["party"]["showAs"])

        position = h.make_position(
            context,
            name="Member of the Irish Parliament",
            topics=["gov.national", "gov.legislative"],
            country=["ie"],
        )
        title = HOUSE_TITLES.get(membership["house"]["houseCode"].lower())
        if title:
            position.add("name", title)
        if title == "Teachtaí Dála":
            # some TDs can represent several constituencies per term
            for represents in membership["represents"]:
                position.add("subnationalArea", represents["represent"]["showAs"])
        # ^ this also features things like "Administrative Panel" or "Labour Panel" for Senators
        # not sure what to do with these, so skipping for now

        categorisation = categorise(context, position, is_pep=True)
        if not categorisation.is_pep:
            continue

        occupancy = h.make_occupancy(
            context,
            person,
            position,
            start_date=membership["dateRange"]["start"],
            end_date=membership["dateRange"]["end"],
            categorisation=categorisation,
        )
        if occupancy is not None:
            context.emit(person)
            context.emit(position)
            context.emit(occupancy)

        # --- some MPs have also other positions listed (e.g. Minister of Finance): ---
        for office in membership["offices"]:
            position_other_name = office["office"]["officeName"]["showAs"]

            if position_other_name is not None:
                position_other = h.make_position(
                    context,
                    name=position_other_name,
                    topics=["gov.national", "gov.legislative"],
                    country=["ie"],
                )
                categorisation = categorise(context, position_other, is_pep=True)
                if not categorisation.is_pep:
                    continue

                occupancy_other = h.make_occupancy(
                    context,
                    person,
                    position_other,
                    start_date=office["office"]["dateRange"]["start"],
                    end_date=office["office"]["dateRange"]["end"],
                )
                if occupancy_other is not None:
                    context.emit(position_other)
                    context.emit(occupancy_other)

        # --- some MPs have committee positions listed: ---
        if membership.get("committees"):
            for committee in membership["committees"]:
                position_committee_name = (
                    committee["role"]["title"] if committee["role"] else None
                )

                if position_committee_name is not None:
                    position_committee = h.make_position(
                        context,
                        name=position_committee_name,
                        topics=["gov.national", "gov.legislative"],
                        country=["ie"],
                    )
                    position_committee.add(
                        "description", committee["committeeName"][0]["nameEn"]
                    )
                    position_committee.add("sourceUrl", committee["uri"])

                    categorisation = categorise(
                        context, position_committee, is_pep=True
                    )
                    if not categorisation.is_pep:
                        continue

                    occupancy_committee = h.make_occupancy(
                        context,
                        person,
                        position_committee,
                        start_date=committee["role"]["dateRange"]["start"],
                        end_date=committee["role"]["dateRange"]["end"],
                    )
                    if occupancy_committee is not None:
                        context.emit(position_committee)
                        context.emit(occupancy_committee)


def crawl(context: Context) -> None:
    all_members = []
    limit = 1000
    skip = 0

    # the API has a server-side cap of 1k results per request
    # we need a skip parameter to paginate records
    meta = context.fetch_json(context.data_url)
    total = meta["head"]["counts"]["memberCount"]

    while skip < total:
        batch = context.fetch_json(f"{context.data_url}?limit={limit}&skip={skip}")[
            "results"
        ]
        all_members.extend(batch)
        skip += limit

    for member in all_members:
        member = member["member"]
        crawl_member(context, member)
