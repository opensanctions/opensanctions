from itertools import count
from typing import Any, NamedTuple

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise


class Chamber(NamedTuple):
    slug: str
    endpoint: str
    position: str
    wikidata_id: str
    constituency_field: str  # source field naming the seat's electorate


# Both houses of Bhutan's Parliament publish their members — current and past — through
# the same Strapi CMS. Each term of service is a separate record, so a member who served
# several terms appears several times; downstream deduplication merges them.
CHAMBERS = [
    # National Council seats are elected one per dzongkhag (district), so the
    # dzongkhag is the electorate; royally-appointed "eminent members" have none.
    Chamber(
        "nc",
        "https://cms.parliament.gov.bt/api/nc-members",
        "Member of the National Council of Bhutan",
        "Q21328625",
        "dzongkhag",
    ),
    # National Assembly members represent a single-member constituency within a
    # dzongkhag; the constituency is the electorate.
    Chamber(
        "na",
        "https://cms.parliament.gov.bt/api/na-members",
        "Member of the National Assembly of Bhutan",
        "Q21295972",
        "constituency",
    ),
]

# Fields left unused; passed to audit_data so new fields surface as warnings.
IGNORE = [
    "id",
    "createdAt",
    "updatedAt",
    "publishedAt",
    "locale",
    "committee_memberships",
    "description",
    "parliament",  # term label, redundant with start/end
    "dzongkhag",  # National Assembly: the district containing the constituency
    "designation",  # leadership role within the chamber; not modelled separately
]


def crawl_member(
    context: Context,
    chamber: Chamber,
    position: Entity,
    categorisation: PositionCategorisation,
    record: dict[str, Any],
) -> None:
    person = context.make("Person")
    person.id = context.make_slug(chamber.slug, record.pop("documentId"))
    h.apply_name(person, full=record.pop("name"), lang="eng")
    # Const. of Bhutan art. 23(3): a candidate for Parliament must be a Bhutanese citizen.
    # https://www.constituteproject.org/constitution/Bhutan_2008
    person.add("citizenship", "bt")

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        period_start=record.pop("start"),
        period_end=record.pop("end"),
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    # NC eminent members have no electorate, so the field is null and drops out.
    occupancy.add("constituency", record.pop(chamber.constituency_field))
    context.emit(person)
    context.emit(occupancy)
    context.audit_data(record, ignore=IGNORE)


def crawl_chamber(context: Context, chamber: Chamber) -> None:
    position = h.make_position(
        context,
        name=chamber.position,
        country="bt",
        topics=["gov.national", "gov.legislative"],
        wikidata_id=chamber.wikidata_id,
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    for page in count(1):
        data = context.fetch_json(
            chamber.endpoint,
            params={"pagination[page]": page, "pagination[pageSize]": 100},
            cache_days=1,
        )
        for record in data["data"]:
            crawl_member(context, chamber, position, categorisation, record)
        if page >= data["meta"]["pagination"]["pageCount"]:
            break


def crawl(context: Context) -> None:
    for chamber in CHAMBERS:
        crawl_chamber(context, chamber)
