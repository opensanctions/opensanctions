from datetime import datetime, timedelta, timezone
from typing import Any

from zavod import Context
from zavod import helpers as h

# The portal's backend API. The list endpoint returns the designated
# organizations; members of each organization are fetched from a paginated
# per-organization endpoint. Both require the numeric ``Portal-Id`` header
# that selects the Vietnamese-language portal.
API_BASE = "https://api-portal.bocongan.gov.vn/backend-portal"
MEMBER_URL = f"{API_BASE}/terrorist-member"
# The portal serves separate Vietnamese- and English-language content trees,
# each selected by a numeric ``Portal-Id`` header. The Vietnamese portal is the
# authoritative source (it carries the member records);
HEADERS = {"Portal-Id": "22"}
PAGE_SIZE = 50

# The source stores date-only values as local (Indochina Time, UTC+7) midnight
# serialised to UTC, e.g. "1953-11-19T17:00:00Z" is 1953-11-20 in Vietnam.
INDOCHINA_TZ = timezone(timedelta(hours=7))

# ``codeName`` packs several aliases into one string using either separator.
ALIAS_SPLITS = [";", ","]


def parse_local_date(raw: str | None) -> str | None:
    """Convert a UTC timestamp from the source into the intended local date.

    Source birth dates are Indochina-Time midnights serialised to UTC, so the
    calendar date must be read in UTC+7 to avoid an off-by-one day.
    """
    if raw is None or raw == "":
        return None
    dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    return dt.astimezone(INDOCHINA_TZ).date().isoformat()


def crawl_member(
    context: Context, organization: Any, org_slug: str, member: dict[str, Any]
) -> None:
    person = context.make("Person")
    full_name = member.pop("fullName")
    person.id = context.make_id(full_name)

    h.apply_name(person, full=full_name, lang="vie")
    for alias in h.multi_split(member.pop("codeName", None) or "", ALIAS_SPLITS):
        person.add("alias", alias)

    h.apply_date(person, "birthDate", parse_local_date(member.pop("birthDate", None)))

    for nationality in (member.pop("nationality", None) or "").split(","):
        person.add("nationality", nationality.strip())

    # The source labels this field "Nơi sinh" (place of birth) but the values are
    # residential addresses (or, at most, a country), so we record them as such.
    place = member.pop("placeOfBirth", None)
    if place is not None and place.strip() != "":
        address = h.make_address(context, full=place, lang="vie")
        h.copy_address(person, address)

    # The field is labelled "Số hộ chiếu" (passport number) in the source.
    person.add("passportNumber", member.pop("identificationNumber", None))

    gender = member.pop("gender", None)
    if gender is not None:
        person.add("gender", context.lookup_value("gender", str(gender)))

    # notes are only available in Vietnamese;
    person.add("notes", member.pop("infoOther", None), lang="vie")
    person.add("topics", "crime.terror")

    # listing of members is on the org page
    person.add("sourceUrl", f"https://bocongan.gov.vn/to-chuc-khung-bo/{org_slug}")

    position = member.pop("position", None)

    sanction = h.make_sanction(
        context,
        person,
        program_name="Vietnam MPS list of designated terrorists and terrorism financers",
    )

    membership = context.make("Membership")
    membership.id = context.make_id(person.id, "member", organization.id)
    membership.add("member", person)
    membership.add("organization", organization)
    membership.add("role", position, lang="vie")

    context.audit_data(member, ignore=["id", "image", "approvedDate", "status", "slug"])
    context.emit(person)
    context.emit(sanction)
    context.emit(membership)


def crawl_organization(context: Context, org: dict[str, Any]) -> None:
    slug = org.pop("slug")
    if context.lookup_value("skip.organization", slug) is not None:
        context.log.info("Skipping UN Security Council pass-through entry", slug=slug)
        return

    entity = context.make("Organization")
    name = org.pop("name")
    entity.id = context.make_id(name, slug)
    entity.add("name", name, lang="vie")

    entity.add("topics", "crime.terror")
    entity.add("sourceUrl", f"https://bocongan.gov.vn/to-chuc-khung-bo/{slug}")

    sanction = h.make_sanction(
        context,
        entity,
        program_name="Vietnam MPS list of designated terrorists and terrorism financers",
    )

    context.audit_data(org, ignore=["id", "description"])
    context.emit(entity)
    context.emit(sanction)

    # Fetch structured member records (paginated). Most organizations have none;
    # they are listed as organization-only designations.
    for page in range(0, 100):
        params = {"organization_slug": slug, "page": page, "size": PAGE_SIZE}
        data = context.fetch_json(
            MEMBER_URL, params=params, headers=HEADERS, cache_days=1
        )
        members = data.get("data") or []
        for member in members:
            crawl_member(context, entity, slug, member)
        if len(members) < PAGE_SIZE:
            break


def crawl(context: Context) -> None:
    data = context.fetch_json(context.data_url, headers=HEADERS, cache_days=1)
    organizations = data.get("data")
    if not organizations:
        raise ValueError("No organizations returned by the terrorist list endpoint")

    for org in organizations:
        crawl_organization(context, org)
