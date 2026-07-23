from itertools import count

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.extract import zyte_api
from zavod.stateful.positions import PositionCategorisation, categorise

# The site resolves but times out / is blocked from non-Ethiopian egress, so it is fetched
# through the Zyte API with an Ethiopian exit.
GEOLOCATION = "et"

# The members list is a Liferay portlet table paginated via this query parameter.
PAGE_PARAM = "_membersearch_WAR_mpPortletsportlet_curMembers"

# The rendered page is unblocked only once the members table is present.
UNBLOCK_VALIDATOR = './/table[.//th[contains(., "Full Name")]]'

MAX_PAGES = 40


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    row: dict[str, str | None],
) -> None:
    full_name = row.get("full_name")
    if full_name is None or not full_name.strip():
        return
    regional_state = row.get("regional_state")
    constituency = row.get("constituency")

    person = context.make("Person")
    person.id = context.make_id(full_name, regional_state, constituency)
    person.add("name", full_name)
    person.add("gender", row.get("gender"))
    person.add("political", row.get("political_party"))
    # Every Ethiopian national has the right to be elected to any office (FDRE
    # Constitution, 1995, Article 38(1)(c)).
    # https://www.constituteproject.org/constitution/Ethiopia_1994
    person.add("citizenship", "et")

    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return
    occupancy.add("constituency", constituency)
    occupancy.add("constituency", regional_state)
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the House of Peoples' Representatives of Ethiopia",
        country="et",
        wikidata_id="Q21328614",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    seen: set[str] = set()
    for page in count(1):
        if page > MAX_PAGES:
            raise ValueError("HoPR members list exceeded the page cap")
        url = f"{context.data_url}?{PAGE_PARAM}={page}"
        doc = zyte_api.fetch_html(
            context,
            url,
            unblock_validator=UNBLOCK_VALIDATOR,
            geolocation=GEOLOCATION,
            cache_days=1,
        )
        table = h.xpath_element(doc, UNBLOCK_VALIDATOR)
        rows = [h.cells_to_str(r) for r in h.parse_html_table(table, header_tag="th")]
        # Stop when a page adds no members we haven't already seen — this ends normal
        # pagination and also guards against the portlet ignoring the page parameter.
        new_rows = 0
        for row in rows:
            key = "|".join(
                (row.get(k) or "")
                for k in ("full_name", "regional_state", "constituency")
            )
            if not row.get("full_name") or key in seen:
                continue
            seen.add(key)
            new_rows += 1
            crawl_member(context, position, categorisation, row)
        if new_rows == 0:
            break

    if not seen:
        raise ValueError("No members parsed from the HoPR members list")
