import json
from typing import Any

from zavod.stateful.positions import PositionCategorisation, categorise

from zavod import Context, Entity
from zavod import helpers as h

TOPICS = ["gov.national", "gov.legislative"]
MEMBER_IGNORE = [
    "id",
    "externalId",
    "userId",
    "empId",
    # Always true.
    "isActive",
    # No matching property.
    "isFreedomFighter",
    "professionBn",
    "electionCount",
    # Not extracted for PEPs.
    "email",
    "additionalEmail",
    "mobile",
    "additionalMobile",
    "presentAddressEng",
    "presentAddressBng",
    "permanentAddressEng",
    "permanentAddressBng",
    "photoUrl",
    "signUrl",
    # Speaker profile page content.
    "speakerProfileImage",
    "speakerHeroSummaryEn",
    "speakerHeroSummaryBn",
    "speakerDetailsBioEn",
    "speakerDetailsBioBn",
    "createdAt",
    "updatedAt",
]
TERM_IGNORE = [
    "id",
    "memberId",
    "partyId",
    "constituencyId",
    "statusVal",
    # No matching property.
    "oathDate",
    # Always 1.
    "count",
    # Read from the parliaments list instead.
    "parliamentNo",
    "parliament",
    # Role flags without office text; the office comes from the member's designation.
    "isPm",
    "isSpeaker",
    "isDeputySpeaker",
    "isOppositionLeader",
    "createdAt",
    "updatedAt",
]


def fetch_prop(context: Context, key: str, **params: Any) -> Any:
    """Decode one prop of the member list from the page's RSC stream, whose rows
    aren't line-delimited."""
    # Larger limits return an empty list.
    params["limit"] = 500
    # Served as text/x-component without a charset, which would decode as Latin-1.
    text = context.fetch_text(
        context.data_url, params=params, headers={"RSC": "1"}, encoding="utf-8"
    )
    assert text is not None
    start = text.index(f'"{key}":') + len(key) + 3
    return json.JSONDecoder().raw_decode(text, start)[0]


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    parliament: dict[str, Any],
    member: dict[str, Any],
) -> None:
    name = member.pop("nameEng")
    dob = member.pop("dateOfBirth")
    # Set only for the sitting Speaker and Deputy Speaker, e.g. "Speaker, Bangladesh
    # Parliament"; empty or null for everyone else.
    designation = member.pop("speakerDesignationEn")
    designation_bn = member.pop("speakerDesignationBn")

    person = context.make("Person")
    # Source member ids are per parliament, so they don't link terms.
    person.id = context.make_id(name, dob)
    person.add(
        "name", h.strip_name_titles(context, name), lang="eng", original_value=name
    )
    person.add("name", member.pop("nameBng"), lang="ben")
    person.add("fatherName", member.pop("fatherNameEng"), lang="eng")
    person.add("fatherName", member.pop("fatherNameBng"), lang="ben")
    person.add("motherName", member.pop("motherNameEng"), lang="eng")
    person.add("motherName", member.pop("motherNameBng"), lang="ben")
    h.apply_date(person, "birthDate", dob)
    person.add("gender", member.pop("gender"))
    person.add("citizenship", "bd")

    # A sitting parliament's (status 1) last date is only scheduled.
    period_end = None if parliament["status"] == 1 else parliament["parliamentLastDate"]

    for term in member.pop("terms"):
        status = context.lookup_value(
            "term_status", term.pop("status"), warn_unmatched=True
        )
        end_date = term.pop("endDate")
        if status == "active":
            # Scheduled, or stale from the 11th parliament (2022-12-11).
            end_date = None
        elif status == "died":
            h.apply_date(person, "deathDate", end_date)
        person.add("political", term.pop("party")["nameEng"])
        constituency = term.pop("constituency") or {}
        start_date = term.pop("startDate")
        election_date = term.pop("electionDate")

        posts = [(position, categorisation)]
        if status == "active" and designation not in (None, ""):
            office_role = h.make_position(
                context, name=designation, country="bd", lang="eng"
            )
            office_role.add("name", designation_bn, lang="ben")
            # Emitted once the office is categorised as PEP in the review UI.
            office_categorisation = categorise(
                context, office_role, default_is_pep=None
            )
            posts.append((office_role, office_categorisation))

        for post, post_categorisation in posts:
            if not post_categorisation.is_pep:
                continue
            occupancy = h.make_occupancy(
                context,
                person,
                post,
                categorisation=post_categorisation,
                start_date=start_date,
                end_date=end_date,
                period_start=parliament["oathDate"],
                period_end=period_end,
                election_date=election_date,
                no_end_implies_current=status == "active",
            )
            if occupancy is None:
                continue
            if post is position:
                occupancy.add("constituency", constituency.get("constituencyEng"))
            context.emit(occupancy)
            context.emit(post)
            context.emit(person)

        context.audit_data(term, ignore=TERM_IGNORE)

    context.audit_data(member, ignore=MEMBER_IGNORE)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Parliament of Bangladesh",
        wikidata_id="Q13058882",
        country="bd",
        topics=TOPICS,
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        context.log.warning("Position is not categorised as PEP", position=position.id)
        return
    context.emit(position)

    cutoff = h.earliest_term_start(TOPICS)
    parliaments = fetch_prop(context, "parliamentsList")
    for parliament in sorted(parliaments, key=lambda p: p["oathDate"], reverse=True):
        number = parliament["parliamentNo"]
        if parliament["parliamentLastDate"] < cutoff:
            context.log.info("Parliament predates the PEP window", parliament=number)
            break
        response = fetch_prop(context, "memberResponse", parliamentNo=number)
        if not response["total"] == len(response["data"]) > 0:
            context.log.warning(
                "Member list is empty or truncated",
                parliament=number,
                total=response["total"],
                returned=len(response["data"]),
            )
        for member in response["data"]:
            crawl_member(context, position, categorisation, parliament, member)
