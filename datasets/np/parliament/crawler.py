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
    """Best-effort Gregorian birth date from the messy ``dob`` field.

    Reach for this because the source mixes Gregorian dates, Bikram Sambat (BS, ~57
    years ahead of the Gregorian calendar), Excel serial numbers and Devanagari digits
    in one field. Only dashed ``YYYY-MM-DD`` values are trusted. In this data Gregorian
    birth years stop at 1973 while BS values start at 2002, so a year from 2000 on is
    BS and is folded to a Gregorian *year* — BS months don't map onto Gregorian ones,
    so the day and month are dropped. Earlier values are already Gregorian and kept in
    full. The result is returned only if it is a plausible adult birth year.
    """
    if raw is None or not re.match(r"^\d{4}-\d{2}-\d{2}$", raw):
        return None
    year = int(raw[:4])
    is_bs = year >= 2000
    greg_year = year - 57 if is_bs else year
    if not (1920 <= greg_year <= 2001):
        return None
    return str(greg_year) if is_bs else raw


def crawl_member(
    context: Context,
    chamber: Chamber,
    position: Entity,
    categorisation: PositionCategorisation,
    record: dict[str, Any],
) -> None:
    member_type = record.pop("member_type")
    if member_type in NON_MP_TYPES:
        return
    if member_type not in MP_TYPES:
        context.log.warning(
            "Unknown member_type", member_type=member_type, id=record["id"]
        )
        return

    names = {
        t["locale"]: t["name"] for t in record.pop("parliament_member_translations")
    }
    slug_name = record.pop("slug", None)
    en_name = (names.get("en") or slug_name or "").strip()
    if en_name in ("", "-"):  # placeholder records carry no real name
        context.log.info("Skipping member without a name", id=record["id"])
        return

    person = context.make("Person")
    person.id = context.make_slug(chamber.slug, str(record.pop("id")))
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
