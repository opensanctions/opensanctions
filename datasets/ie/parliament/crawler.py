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

HEAD_GOV_ROLES = ["Taoiseach", "President of the Executive Council"]
LEGISLATIVE_ROLES = [
    "Ceann Comhairle",
    "Cathaoirleach",
    "Leas-Cathaoirleach",
    "Leas-Cheann Comhairle",
    "President of Dáil Éireann",
]


def crawl_member(context: Context, member: Dict[str, Any]) -> None:
    person = context.make("Person")
    person.id = context.make_id(member.pop("memberCode"))
    h.apply_name(
        person, first_name=member.pop("firstName"), last_name=member.pop("lastName")
    )
    h.apply_name(person, member.pop("fullName"))
    person.add("citizenship", "ie")

    person.add("deathDate", member.pop("dateOfDeath", None))
    person.add("gender", member.pop("gender", None))

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
        position.add("name", title)
        if title == "Teachtaí Dála":
            position.add("wikidataId", "Q654291")
        else:
            position.add("wikidataId", "Q18043391")

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
            # for Senators, this reflects a vocational panel, uni constituency, or nomination by Taoiseach
            for represents in membership["represents"]:
                occupancy.add("constituency", represents["represent"]["showAs"])

            context.emit(person)
            context.emit(position)
            context.emit(occupancy)

        # --- some MPs have also other national gov positions listed (e.g. Minister of Finance): ---
        for office in membership["offices"]:
            position_other_name = office["office"]["officeName"]["showAs"]

            if position_other_name is not None:
                position_other = h.make_position(
                    context,
                    name=position_other_name,
                    topics=["gov.national"],
                    country=["ie"],
                )
                if position_other_name in HEAD_GOV_ROLES:
                    position.add("topics", "gov.head")
                elif position_other_name in LEGISLATIVE_ROLES:
                    position.add("topics", "gov.legislative")
                elif any(
                    admin_role in position_other_name.lower()
                    for admin_role in ["director", "secretary"]
                ):
                    print(position_other_name)
                    position.add("topics", "gov.admin")
                elif position_other_name == "Attorney General":
                    # legal adviser to the government
                    position.add("topics", "gov.judicial")
                else:
                    # Tánaiste, Vice-President of Executive Council, all Ministers, Postmaster General
                    position.add("topics", "gov.executive")

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
