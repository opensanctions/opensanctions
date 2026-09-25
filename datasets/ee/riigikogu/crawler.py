from typing import Any

from normality import slugify

from zavod import Context
from zavod import helpers as h
from zavod.stateful.positions import categorise

TOPICS = ["gov.national", "gov.legislative"]


def crawl_member(
    context: Context,
    data: dict[str, Any],
    english_name: str | None,
    migrate_id: bool,
) -> None:
    uuid = data.pop("uuid")
    full_name = data.pop("fullName")
    # The membership in the composition that was requested. A member who was
    # elected President or Vice-President of the Riigikogu is listed with that
    # job title and its dates instead of the plain membership.
    membership = data.pop("plenaryMembership")
    membershipNumber = membership.pop("membershipNumber")
    start_date = membership.pop("startDate")
    end_date = membership.pop("endDate")
    job_title_est = membership.pop("jobTitle")["value"]
    context.audit_data(membership, ignore=["uuid", "role"])

    if full_name is None:
        context.log.warning(
            "Member without a name",
            uuid=uuid,
            membership=membershipNumber,
            start_date=start_date,
            end_date=end_date,
        )
        return

    person = context.make("Person")
    person.id = context.make_id(uuid)
    # TODO: Remove once production has run the crawler once with UUID-based IDs.
    # IDs used to be keyed on the English name shown on the member's page, for
    # the members of the current composition only. Two different members are
    # called Tarmo Tamm, so their old merged entity is not carried over.
    if migrate_id and english_name is not None and english_name != "Tarmo Tamm":
        context.rekey(context.make_id(english_name), person.id)
    h.apply_name(
        person,
        full=full_name,
        first_name=data.pop("firstName"),
        last_name=data.pop("lastName"),
    )
    h.apply_name(person, full=english_name)
    # Only Estonian citizens may stand for election to the Riigikogu, see § 60 of
    # the Constitution: https://www.riigiteataja.ee/en/eli/530102013003/consolide
    person.add("citizenship", "ee")
    person.add("gender", data.pop("gender"))
    h.apply_date(person, "birthDate", data.pop("dateOfBirth"))
    h.apply_date(person, "deathDate", data.pop("dateOfDeath"))
    person.add("email", data.pop("email"))
    person.add(
        "sourceUrl",
        "https://www.riigikogu.ee/en/parliament-of-estonia/members-of-the-riigikogu/"
        f"{uuid}/{slugify(full_name)}/",
    )

    constituency: str | None = None
    for district in data.pop("electoralDistrictHistory"):
        if district.pop("membership") != membershipNumber:
            continue
        assert constituency is None, (uuid, membershipNumber)
        constituency = district.pop("electoralDistrict")["value"]
        context.audit_data(district)

    groups: list[str] = []
    for faction in data.pop("factions"):
        if faction["membership"]["membershipNumber"] != membershipNumber:
            continue
        assert faction["type"]["code"] == "FRAKTSIOON", faction
        groups.append(faction["name"])

    job_title_eng = context.lookup_value("position_eng", job_title_est)
    assert job_title_eng is not None, job_title_est
    position = h.make_position(
        context, job_title_est, country="ee", topics=TOPICS, lang="eng"
    )
    position.add("name", job_title_eng, lang="eng")
    categorisation = categorise(context, position)
    if categorisation.is_pep:
        occupancy = h.make_occupancy(
            context,
            person,
            position,
            start_date=start_date,
            end_date=end_date,
            categorisation=categorisation,
        )
        if occupancy is not None:
            occupancy.add("constituency", constituency, lang="est")
            occupancy.add("politicalGroup", groups, lang="est")
            context.emit(occupancy)
            context.emit(position)
            context.emit(person)

    context.audit_data(
        data,
        ignore=[
            "_links",
            "active",
            "agesInParliament",
            "committees",
            "parliamentSeniority",
            "phone",
            "photo",
            "upn",
        ],
    )


def crawl(context: Context) -> None:
    earliest = h.earliest_term_start(TOPICS)
    compositions = context.fetch_json("https://api.riigikogu.ee/api/memberships")
    compositions = sorted(compositions, key=lambda c: c["number"], reverse=True)
    current_number = compositions[0]["number"]
    for composition in compositions:
        number = composition.pop("number")
        end_date = composition.pop("endDate")
        context.audit_data(composition, ignore=["_links", "startDate", "sessions"])
        if end_date < earliest:
            context.log.info(
                "Skipping composition that ended before the PEP cut-off",
                number=number,
                end_date=end_date,
            )
            continue
        # Names are missing from many older records in the English version of
        # the data, so the Estonian one is used throughout.
        members = context.fetch_json(
            context.data_url,
            params={"membership": number, "status": "ALL", "lang": "ET"},
        )
        members_en = context.fetch_json(
            context.data_url,
            params={"membership": number, "status": "ALL", "lang": "EN"},
        )
        english_names: dict[str, str] = {
            m["uuid"]: m["fullName"] for m in members_en if m["fullName"] is not None
        }
        for member in members:
            english_name = english_names.get(member["uuid"])
            migrate_id = number == current_number
            crawl_member(context, member, english_name, migrate_id)
