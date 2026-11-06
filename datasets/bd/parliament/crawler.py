import json
from typing import Any

from zavod.stateful.positions import PositionCategorisation, categorise

from zavod import Context, Entity
from zavod import helpers as h

TOPICS = ["gov.national", "gov.legislative"]
# Per-term flags marking an office held alongside the membership, mapped to a
# position by the `position` lookup.
ROLE_FLAGS = ["isPm", "isSpeaker", "isDeputySpeaker", "isOppositionLeader"]
# A larger limit doesn't fail: the source answers it with an empty list.
PAGE_SIZE = 500


def fetch_props(context: Context, parliament_no: int | None) -> dict[str, Any]:
    """Fetch the props of the member list component for one parliament.

    The site is a Next.js app whose member data comes from a backend that isn't
    publicly reachable. Requested with the `RSC` header, the page returns its React
    Server Components stream, in which the member list component's props are JSON.
    The stream's rows aren't reliably line-delimited (length-prefixed text rows run
    into the next row), so each prop is decoded where its key appears.
    """
    params: dict[str, Any] = {"limit": PAGE_SIZE}
    if parliament_no is not None:
        params["parliamentNo"] = parliament_no
    text = context.fetch_text(context.data_url, params=params, headers={"RSC": "1"})
    assert text is not None, parliament_no
    decoder = json.JSONDecoder()
    props: dict[str, Any] = {}
    for key in (
        "memberResponse",
        "parliamentsList",
        "initialParliamentNo",
        "lastParliamentNo",
    ):
        marker = f'"{key}":'
        assert text.count(marker) == 1, (parliament_no, key)
        props[key], _ = decoder.raw_decode(text, text.index(marker) + len(marker))
    response = props["memberResponse"]
    assert response["totalPages"] == 1, response["totalPages"]
    assert response["total"] == len(response["data"]) > 0, response["total"]
    return props


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    parliament: dict[str, Any],
    member: dict[str, Any],
) -> None:
    name = member.pop("nameEng")
    dob = member.pop("dateOfBirth")

    person = context.make("Person")
    # The source's member ids are assigned per parliament, so they don't link a
    # member's terms; the name and birth date do.
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
    person.add("email", member.pop("email"))
    person.add("email", member.pop("additionalEmail"))
    # Members must be citizens of Bangladesh: Constitution of Bangladesh, Art. 66(1).
    # https://bdlaws.minlaw.gov.bd/act-367/section-24620.html
    person.add("citizenship", "bd")

    sitting = parliament["status"] == 1
    # Only the sitting parliament's dates are open-ended. Its scheduled last date
    # is not an end date.
    period_end = None if sitting else parliament["parliamentLastDate"]

    emit = False
    for term in member.pop("terms"):
        assert term.pop("parliamentNo") == parliament["parliamentNo"], term
        status = term.pop("status")
        term.pop("statusVal")  # the numeric code of `status`
        end_date = term.pop("endDate")
        if status == "Active":
            if not sitting:
                raise ValueError(f"Active term in an ended parliament: {name}")
            # On sitting members, the end date is either the parliament's scheduled
            # last date, or stale: a few carry 2022-12-11, from the mass resignation
            # of their party's members of the 11th parliament.
            end_date = None
        elif status == "Died":
            h.apply_date(person, "deathDate", end_date)
        elif status not in ("Completed", "Resigned", "Cancelled"):
            raise ValueError(f"Unknown term status: {status}")

        constituency = term.pop("constituency")
        party = term.pop("party")
        person.add("political", party.pop("nameEng"))

        dates: dict[str, Any] = dict(
            start_date=term.pop("startDate"),
            end_date=end_date,
            period_start=parliament["oathDate"],
            period_end=period_end,
            election_date=term.pop("electionDate"),
        )
        occupancy = h.make_occupancy(
            context, person, position, categorisation=categorisation, **dates
        )
        if occupancy is not None:
            if constituency is not None:
                occupancy.add("constituency", constituency.pop("constituencyEng"))
            context.emit(occupancy)
            emit = True

        for flag in ROLE_FLAGS:
            if not term.pop(flag):
                continue
            res = context.lookup("position", flag)
            assert res is not None, flag
            office = h.make_position(
                context,
                name=res.name,
                country="bd",
                topics=res.topics,
                wikidata_id=res.wikidata_id,
                lang="eng",
            )
            office_categorisation = categorise(context, office)
            if not office_categorisation.is_pep:
                continue
            office_occupancy = h.make_occupancy(
                context,
                person,
                office,
                categorisation=office_categorisation,
                **dates,
            )
            if office_occupancy is not None:
                context.emit(office_occupancy)
                context.emit(office)
                emit = True

        context.audit_data(
            term,
            ignore=[
                "id",
                "memberId",
                "partyId",
                "constituencyId",
                # The oath date of a member; Occupancy has no property for it.
                "oathDate",
                # The number of the member's term, always 1.
                "count",
                # The parliament, read from the list of parliaments instead.
                "parliament",
                "createdAt",
                "updatedAt",
            ],
        )

    if emit:
        context.emit(person)

    context.audit_data(
        member,
        ignore=[
            "id",
            "externalId",
            "userId",
            "empId",
            # True for every member, past parliaments included.
            "isActive",
            # Whether the member fought in the 1971 Liberation War; no matching property.
            "isFreedomFighter",
            # Profession in Bengali, free text; no matching property.
            "professionBn",
            "electionCount",
            # Phone numbers and home addresses are not extracted for PEPs.
            "mobile",
            "additionalMobile",
            "presentAddressEng",
            "presentAddressBng",
            "permanentAddressEng",
            "permanentAddressBng",
            "photoUrl",
            "signUrl",
            # The Speaker's and Deputy Speaker's profile page content.
            "speakerProfileImage",
            "speakerDesignationEn",
            "speakerDesignationBn",
            "speakerHeroSummaryEn",
            "speakerHeroSummaryBn",
            "speakerDetailsBioEn",
            "speakerDetailsBioBn",
            "createdAt",
            "updatedAt",
        ],
    )


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

    # Without a parliament number, the page shows the sitting parliament's members
    # and lists all parliaments with their dates and status (1: sitting, 2: ended).
    props = fetch_props(context, None)
    parliaments = props["parliamentsList"]
    for parliament in parliaments:
        assert parliament["status"] in (1, 2), parliament
    latest = max(parliaments, key=lambda p: p["parliamentNo"])
    sitting = [p for p in parliaments if p["status"] == 1]
    # The newest parliament is the sitting one, unless it has been dissolved.
    assert sitting in ([latest], []), sitting
    assert props["initialParliamentNo"] == latest["parliamentNo"], props
    assert props["lastParliamentNo"] == latest["parliamentNo"], props

    cutoff = h.earliest_term_start(TOPICS)
    for parliament in sorted(parliaments, key=lambda p: p["oathDate"], reverse=True):
        number = parliament["parliamentNo"]
        if parliament["parliamentLastDate"] < cutoff:
            context.log.info("Parliament predates the PEP window", parliament=number)
            break
        if number != latest["parliamentNo"]:
            props = fetch_props(context, number)
        for member in props["memberResponse"]["data"]:
            crawl_member(context, position, categorisation, parliament, member)
