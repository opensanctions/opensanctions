from typing import Any

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

# Undocumented JSON API that powers the parliament.bg single-page app.
API = "https://www.parliament.bg/api/v1"

# `A_ns_CT_id` (collection type) on membership records. We only model the
# parliamentary mandate itself; party-group, committee and friendship-group
# memberships are out of scope for PEP purposes.
NATIONAL_ASSEMBLY = 1

# Sentinel end date the API uses for an ongoing membership.
OPEN_END = "9999-12-31"

# "gr. " / "гр. " is the Bulgarian abbreviation for "град" (town/city) and
# prefixes the birth city in both language variants.
CITY_PREFIXES = ("gr. ", "гр. ")


def parse_city(value: str | None) -> str | None:
    if value is None:
        return None
    value = value.strip()
    for prefix in CITY_PREFIXES:
        if value.startswith(prefix):
            value = value[len(prefix) :].strip()
    return value or None


def assembly_terms(memberships: list[dict[str, Any]]) -> dict[int, tuple[str, str]]:
    """Collapse a person's membership records into one term span per assembly.

    A member can hold several roles within the same assembly (e.g. first a plain
    Member, then President), which the API lists as separate records. Since we
    model only the single "Member of the National Assembly" position, we reduce
    them to one occupancy per assembly, spanning the earliest start to the latest
    end (an open end on any record makes the whole span open-ended).
    """
    terms: dict[int, tuple[str, str]] = {}
    for record in memberships:
        if record.get("A_ns_CT_id") != NATIONAL_ASSEMBLY:
            continue
        collection_id = record["A_ns_C_id"]
        start = record["A_ns_MSP_date_F"]
        end = record["A_ns_MSP_date_T"]
        if collection_id not in terms:
            terms[collection_id] = (start, end)
            continue
        prev_start, prev_end = terms[collection_id]
        new_start = min(prev_start, start)
        # OPEN_END sorts last lexically, so max() keeps an open end open.
        new_end = max(prev_end, end)
        terms[collection_id] = (new_start, new_end)
    return terms


def crawl_member(
    context: Context,
    mp_id: int,
    position: Entity,
    categorisation: PositionCategorisation,
    cutoff: str,
) -> None:
    en = context.fetch_json(f"{API}/mp-profile/en/{mp_id}", cache_days=14)
    bg = context.fetch_json(f"{API}/mp-profile/bg/{mp_id}", cache_days=14)

    terms = assembly_terms(en.pop("mshipList", []))
    # Keep only terms still relevant within the PEP after-office window. An open
    # end (or any end on/after the cutoff) is relevant; ancient mandates are not.
    relevant = {
        cid: (start, end)
        for cid, (start, end) in terms.items()
        if end == OPEN_END or end >= cutoff
    }
    if not relevant:
        return

    person = context.make("Person")
    # `A_ns_MP_id` is an opaque numeric key, stable across assemblies.
    person.id = context.make_slug("person", str(mp_id))

    h.apply_name(
        person,
        first_name=bg.get("A_ns_MPL_Name1"),
        patronymic=bg.get("A_ns_MPL_Name2"),
        last_name=bg.get("A_ns_MPL_Name3"),
        lang="bul",
    )
    h.apply_name(
        person,
        first_name=en.pop("A_ns_MPL_Name1"),
        patronymic=en.pop("A_ns_MPL_Name2"),
        last_name=en.pop("A_ns_MPL_Name3"),
        lang="eng",
    )

    h.apply_date(person, "birthDate", en.pop("A_ns_MP_BDate"))
    person.add("birthPlace", parse_city(en.pop("A_ns_B_City")), lang="eng")
    person.add("birthPlace", parse_city(bg.get("A_ns_B_City")), lang="bul")
    person.add("birthCountry", en.pop("A_ns_B_Country"), lang="eng")
    person.add("email", en.pop("A_ns_MP_Email"))
    person.add("website", en.pop("A_ns_MP_url"))
    # Electoral coalition / party the member was elected with.
    person.add("political", en.pop("A_ns_CoalL_value"))
    for profession in en.pop("prsList"):
        person.add("profession", profession.get("A_ns_MP_Pr_TL_value"))
    for language in en.pop("lngList"):
        # The value key contains a Cyrillic "Т" (U+0422); match by suffix to
        # avoid embedding a confusable character in the source.
        value = next((v for k, v in language.items() if k.endswith("L_value")), None)
        person.add("spokenLanguage", value)
    # Bulgarian citizenship is required to be elected an MP under Article 65(1)
    # of the Constitution of the Republic of Bulgaria (dual citizens are eligible
    # only with 18 months' residency, but Bulgarian citizenship is mandatory).
    # https://www.parliament.bg/en/const
    person.add("citizenship", "bg")
    person.add("topics", "role.pep")
    person.add("sourceUrl", f"https://www.parliament.bg/en/MP/{mp_id}")

    occupancies = []
    for _cid, (start, end) in sorted(relevant.items()):
        occupancy = h.make_occupancy(
            context,
            person,
            position,
            start_date=start,
            end_date=None if end == OPEN_END else end,
            categorisation=categorisation,
            no_end_implies_current=True,
        )
        if occupancy is not None:
            occupancies.append(occupancy)

    if not occupancies:
        return

    context.emit(person)
    for occupancy in occupancies:
        context.emit(occupancy)
    context.log.info(
        f"Emitting MP {person.first('name')}",
        mp_id=mp_id,
        terms=len(occupancies),
    )

    context.audit_data(
        en,
        ignore=[
            "A_ns_id",
            "A_ns_MP_FM",
            "A_ns_MP_fbook",
            "A_ns_MP_phones",
            "A_ns_MP_leg_count",
            "A_ns_MP_com_count",
            "A_ns_MP_del_count",
            "A_ns_MP_frd_count",
            "A_ns_MPL_id",
            "A_ns_MP_id",
            "C_Lang_id",
            "A_ns_MPL_CV",
            "A_ns_MPL_Spec",
            "A_ns_MPL_Prof",
            "A_ns_MPL_wBranch",
            "A_ns_MPL_City",
            "A_ns_Coal_Prs",
            "A_ns_MRL_value",
            "A_ns_Va_name",
            "A_ns_Va_id",
            "A_ns_MP_img",
            "munList",
            "expenseList",
            "meetingList",
            "penalty",
            "ScList",
            "oldnsList",
            "importActList",
            "controlList",
            "legImportList",
            "penaltyColList",
            "penaltyNsList",
            "strList",
        ],
    )


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the National Assembly of Bulgaria",
        country="bg",
        topics=["gov.national", "gov.legislative"],
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    cutoff = h.earliest_term_start(["gov.national"])

    # Discover every member ID across all assemblies the API exposes. The roster
    # endpoint only returns id + name, so dates come from each member's profile.
    assemblies = context.fetch_json(f"{API}/fn-assembly/en", cache_days=1)
    if not assemblies:
        raise ValueError("No assemblies returned by fn-assembly")

    member_ids: set[int] = set()
    for assembly in assemblies:
        assembly_id = assembly["A_ns_id"]
        roster = context.fetch_json(f"{API}/fn-mps/en/{assembly_id}", cache_days=1)
        for member in roster:
            member_ids.add(member["A_ns_MP_id"])

    context.log.info(
        "Discovered members across assemblies",
        assemblies=len(assemblies),
        members=len(member_ids),
    )
    for mp_id in sorted(member_ids):
        crawl_member(context, mp_id, position, categorisation, cutoff)
