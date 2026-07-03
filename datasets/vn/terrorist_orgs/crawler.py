from typing import Any

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity

# The portal's backend API. The list endpoint returns the designated
# organizations; members of each organization are fetched from a
# per-organization endpoint. Both require the numeric ``Portal-Id`` header
# that selects the Vietnamese-language portal (the authoritative content tree
# that carries the member records).
MEMBER_URL = "https://api-portal.bocongan.gov.vn/backend-portal/terrorist-member"
HEADERS = {"Portal-Id": "22"}
# The member endpoint exposes no total count and defaults to 10 results. The
# largest designation currently has ~15 members, so we request a page well above
# that and fail loudly if it ever fills up (signalling pagination is needed).
MEMBER_PAGE_SIZE = 100
PROGRAM_KEY = "VN-TERROR"


def crawl_person(
    context: Context, organization: Entity, org_slug: str, member: dict[str, Any]
) -> None:
    person = context.make("Person")
    full_name = member.pop("fullName")
    person.id = context.make_id(full_name)

    # aliases can require splitting on occasion
    original = h.Names()
    original.add("name", full_name, lang="vie")
    original.add("alias", member.pop("codeName"), lang="vie")
    h.apply_reviewed_names(context, person, original=original)

    h.apply_date(person, "birthDate", member.pop("birthDate"))
    person.add("nationality", h.multi_split(member.pop("nationality"), [","]))

    # The source labels this field "Nơi sinh" (place of birth) but the values are
    # residential addresses (or, at most, a country), so we record them as such.
    place = member.pop("placeOfBirth")
    address = h.make_address(context, full=place, lang="vie")
    h.copy_address(person, address)

    # The field is labelled "Số hộ chiếu" (passport number) in the source.
    person.add("passportNumber", member.pop("identificationNumber"))
    person.add("gender", member.pop("gender"))
    person.add("notes", member.pop("infoOther"), lang="vie")
    person.add("sourceUrl", f"{context.dataset.url}/{org_slug}")
    person.add("topics", "crime.terror")

    sanction = h.make_sanction(context, person, program_key=PROGRAM_KEY)

    membership = context.make("Membership")
    membership.id = context.make_id(person.id, "member", organization.id)
    membership.add("member", person)
    membership.add("organization", organization)
    membership.add("role", member.pop("position"), lang="vie")

    context.emit(person)
    context.emit(sanction)
    context.emit(membership)

    context.audit_data(member, ignore=["id", "image", "approvedDate", "status", "slug"])


def crawl_org_members(context: Context, organization: Entity, org_slug: str) -> None:
    # Fetch member records. Most organizations have none; they are listed as
    # organization-only designations.
    params = {"organization_slug": org_slug, "size": MEMBER_PAGE_SIZE}
    data = context.fetch_json(MEMBER_URL, params=params, headers=HEADERS, cache_days=1)
    members = data.get("data") or []
    if len(members) >= MEMBER_PAGE_SIZE:
        raise ValueError(f"Member list for {org_slug} may be truncated; add pagination")
    for member in members:
        crawl_person(context, organization, org_slug, member)


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
    entity.add("sourceUrl", f"{context.dataset.url}/{slug}")

    sanction = h.make_sanction(context, entity, program_key=PROGRAM_KEY)

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(org, ignore=["id", "description"])
    # Fetch and emit member records for this organization, if any.
    crawl_org_members(context, entity, slug)


def crawl(context: Context) -> None:
    data = context.fetch_json(context.data_url, headers=HEADERS, cache_days=1)
    organizations = data.get("data")
    for org in organizations:
        crawl_organization(context, org)
