from typing import Dict, Any

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise

HOUSE_TITLES = {"seanad": "Senator", "dail": "Teachta Dála"}


def crawl_member(context: Context, member: Dict[str, Any]) -> None:
    person = context.make("Person")
    person.id = context.make_id(member.pop("memberCode"))
    h.apply_name(
        person,
        full=member.pop("fullName"),
        first_name=member.pop("firstName"),
        last_name=member.pop("lastName"),
    )
    h.apply_date(person, "deathDate", member.pop("dateOfDeath"))
    person.add("citizenship", "ie")
    person.add("gender", member.pop("gender"))

    for membership_meta in member.pop("memberships", []):
        membership = membership_meta["membership"]
        for party in membership["parties"]:
            person.add("political", party["party"]["showAs"])

        title = HOUSE_TITLES.get(membership["house"]["houseCode"].lower())
        # Dáil and Seanad are modelled as distinct positions in Wikidata
        wikidata_id = "Q654291" if title == "Teachta Dála" else "Q18043391"
        assert title is not None

        position = h.make_position(
            context,
            name=title,
            topics=["gov.national", "gov.legislative"],
            country=["ie"],
            wikidata_id=wikidata_id,
        )

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
            # some TDs can represent several constituencies per term
            # for Senators, this reflects a vocational panel, uni constituency,
            # or nomination by Taoiseach
            for represents in membership["represents"]:
                occupancy.add("constituency", represents["represent"]["showAs"])

            context.emit(person)
            context.emit(position)
            context.emit(occupancy)

        # Some MPs have also other national gov positions listed (e.g. Minister of Finance):
        for office in membership["offices"]:
            position_other_name = office["office"]["officeName"]["showAs"]
            if not position_other_name:
                continue

            position_other = h.make_position(
                context,
                name=position_other_name,
                topics=["gov.national"],
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
                context.emit(person)
    context.audit_data(member, ["uri", "showAs", "image", "pId"])


def crawl(context: Context) -> None:
    limit = 1000
    skip = 0

    # the API has a server-side cap of 1k results per request
    # we need a skip parameter to paginate records
    meta = context.fetch_json(context.data_url)
    total = meta["head"]["counts"]["memberCount"]

    while skip < total:
        url = f"{context.data_url}?limit={limit}&skip={skip}"
        batch = context.fetch_json(url)["results"]
        for member in batch:
            member = member["member"]
            crawl_member(context, member)
        skip += limit
