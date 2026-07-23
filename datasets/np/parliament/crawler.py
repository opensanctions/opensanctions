import re
from typing import Any, NamedTuple

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise


class Chamber(NamedTuple):
    slug: str
    url: str
    position: str
    wikidata_id: str | None


# Both houses of the Federal Parliament expose the same JSON shape. The bulk endpoint
# returns every member of every term in a single call. Only the House of Representatives
# member position has a Wikidata item, so the National Assembly one gets a name-derived id.
CHAMBERS = [
    Chamber(
        "hr",
        "https://hr.parliament.gov.np/api/v1/members?getAllMemberDownload=true",
        "Member of the House of Representatives of Nepal",
        "Q61704026",
    ),
    Chamber(
        "na",
        "https://na.parliament.gov.np/api/v1/members",
        "Member of the National Assembly of Nepal",
        None,
    ),
]

# member_type values that denote actual parliamentarians.
MP_TYPES = {"member", "speaker", "vicespeaker"}
# member_type values for secretariat/staff, who are out of scope. Enumerated (rather than
# skipping everything that is not an MP) so a genuinely new member_type surfaces as a
# warning instead of being silently dropped.
NON_MP_TYPES = {"staff", "secretariat", "generalsecretariat"}

# (chamber, id) of MP-typed records the source publishes as pure placeholders — every
# name field is "-". Listed so they skip quietly; any other nameless member warns, since
# dropping a real member the source publishes is a data-loss alarm, not a routine skip.
PLACEHOLDER_IDS = {("hr", 2619)}

# Fields left unused; passed to audit_data so new fields surface as warnings.
IGNORE = [
    "code",
    "parliament_type",
    "sequence",
    "status",
    "images",
    "district_id",
    "registered_date",
    "representation_type_id",
    "political_party_id",
    "election_type_id",
    "election_area_no",
    "territory_no",
    "video_link",
    "secretariat_page",
    "user_id",
    "created_by",
    "published_at",
    "created_at",
    "updated_at",
    "district",
    "representation_type",
    "election_type",
    "name",  # top-level convenience copy of the "en" translation name
    "designation",
    "description",
]


def parse_dob(raw: str | None) -> str | None:
    """Best-effort Gregorian birth *year* from the ``dob`` field.

    Every parseable member value is a Bikram Sambat (BS) date, so this always converts
    from BS; only strict ``YYYY-MM-DD`` values are trusted (the field also carries Excel
    serials, Devanagari digits and placeholders, which are dropped). BS runs ~57 years
    ahead and its new year is in mid-April, so months 1–9 map to BS−57 and months 10–12
    to BS−56. BS months don't align with Gregorian ones, so only the year is kept, and
    only when it falls in a plausible adult birth range.
    """
    if raw is None:
        return None
    match = re.match(r"^(\d{4})-(\d{2})-\d{2}$", raw)
    if match is None:
        return None
    year = int(match.group(1))
    month = int(match.group(2))
    greg_year = year - (56 if month >= 10 else 57)
    if not (1930 <= greg_year <= 2006):  # plausible adult birth range
        return None
    return str(greg_year)


def crawl_member(
    context: Context,
    chamber: Chamber,
    position: Entity,
    categorisation: PositionCategorisation,
    record: dict[str, Any],
) -> None:
    member_type = record.pop("member_type")
    record_id = record.pop("id")
    if member_type in NON_MP_TYPES:
        return
    if member_type not in MP_TYPES:
        context.log.warning(
            "Unknown member_type", member_type=member_type, id=record_id
        )
        return

    names = {
        t["locale"]: t["name"] for t in record.pop("parliament_member_translations")
    }
    slug_name = record.pop("slug", None)
    en_name = (names.get("en") or slug_name or "").strip()
    if en_name in ("", "-"):
        if (chamber.slug, record_id) not in PLACEHOLDER_IDS:
            context.log.warning("Member without a name", id=record_id, record=record)
        return

    person = context.make("Person")
    person.id = context.make_slug(chamber.slug, record_id)
    h.apply_name(person, full=en_name, lang="eng")
    np_name = names.get("np")
    if np_name is not None and np_name.strip() not in ("", "-"):
        person.add("name", np_name.strip(), lang="nep")

    person.add("gender", record.pop("gender"))  # 0/1 mapped via type.gender lookup
    # Const. of Nepal art. 87(1)(a): membership of the Federal Parliament requires Nepali
    # citizenship. https://www.constituteproject.org/constitution/Nepal_2015
    person.add("citizenship", "np")
    h.apply_date(person, "birthDate", parse_dob(record.pop("dob", None)))
    party = record.pop("political_party", None)
    if party is not None:
        person.add("political", party.get("party_name_en"))

    # tenure_end_date is Gregorian; past dates mark former members, future/absent ones
    # current members. The "0000-00-00" sentinel is nulled via the type.date lookup.
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        end_date=record.pop("tenure_end_date", None),
        categorisation=categorisation,
    )
    if occupancy is None:
        return

    context.emit(person)
    context.emit(occupancy)

    context.audit_data(record, ignore=IGNORE)


def crawl(context: Context) -> None:
    # *.parliament.gov.np serves the leaf cert but omits the Sectigo intermediate, so
    # the chain can't be verified (the source's own docs rely on `curl -k`).
    context.http.verify = False
    for chamber in CHAMBERS:
        position = h.make_position(
            context,
            name=chamber.position,
            country="np",
            topics=["gov.national", "gov.legislative"],
            wikidata_id=chamber.wikidata_id,
        )
        categorisation = categorise(context, position, default_is_pep=True)
        context.emit(position)

        data = context.fetch_json(chamber.url, cache_days=1)
        members = data["data"]
        context.log.info("Fetched members", chamber=chamber.slug, count=len(members))
        for record in members:
            crawl_member(context, chamber, position, categorisation, record)
