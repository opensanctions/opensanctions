from typing import Any

from zavod import Context
from zavod import helpers as h
from zavod.stateful.positions import categorise

# The roster is a client-side app backed by a JSON API. The list endpoint carries
# names, party and positions; the per-member detail endpoint adds the email.
#
# The entity ID is derived from the member's parliamentary email rather than the
# source's numeric member ID: the email survived the 2026 site relaunch while the
# numeric IDs did not, so it is the more stable key.
LIST_URL = "https://parliament.mn/api/parliament_members_list/"
DETAIL_URL = "https://parliament.mn/api/parliament_member/detail/{id}/"
PARLIAMENT_MEMBER = "Гишүүн"


def crawl_member(context: Context, member: dict[str, Any]) -> None:
    detail_url = DETAIL_URL.format(id=member["id"])
    detail = context.fetch_json(detail_url, cache_days=7)
    email = detail.get("email")
    if not email:
        raise RuntimeError(f"Member has no email: {detail_url}")

    person = context.make("Person")
    person.id = context.make_id("member", email)
    # Source order (patronymic, then given name), matching the source's full_name.
    person.add("name", f"{member['last_name']} {member['first_name']}", lang="mon")
    person.add("email", email)
    person.add("political", member["party"]["name"], lang="mon")
    # Members of the State Great Khural must be citizens of Mongolia.
    # Constitution of Mongolia, Art. 21(3): "Any citizen of Mongolia, who have attained
    # the age of twenty five years and are qualified to vote, shall be eligible to be
    # elected to the State Great Hural (Parliament)."
    # https://www.constituteproject.org/constitution/Mongolia_2001
    person.add("citizenship", "mn")
    person.add("sourceUrl", f"https://parliament.mn/member/{member['id']}")

    position = h.make_position(
        context,
        "Member of the State Great Khural of Mongolia",
        country="mn",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q21328637",
        lang="eng",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    # The membership dates are the same for every member: they are the bounds of the
    # parliamentary term, not of the individual's tenure.
    for term in member["positions"]:
        if term["unit_type"] == "PARLIAMENT" and term["name"] == PARLIAMENT_MEMBER:
            occupancy = h.make_occupancy(
                context,
                person,
                position,
                no_end_implies_current=True,
                start_date=term["start_date"],
                end_date=term["end_date"],
                categorisation=categorisation,
            )
            if occupancy is None:
                return
            context.emit(person)
            context.emit(position)
            context.emit(occupancy)


def crawl(context: Context) -> None:
    for member in context.fetch_json(LIST_URL, cache_days=1)["members"]:
        crawl_member(context, member)
