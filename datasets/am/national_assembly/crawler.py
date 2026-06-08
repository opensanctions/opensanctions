from zavod import Context, helpers as h
from zavod.util import Element
from zavod.stateful.positions import categorise
from zavod.extract import zyte_api

# parliament.am refuses connections from non-Armenian networks, so all requests
# are routed through Zyte with an Armenian exit point. The pages are static
# server-rendered PHP, so the cheaper httpResponseBody source is sufficient.
GEOLOCATION = "am"

# The roster URL implicitly serves the current convocation. The 8th convocation
# convened on 2021-08-02; faction links carry show_session=8. When a new
# convocation is seated, a higher session number appears and we want the crawler
# to fail loudly so a maintainer reviews the new term rather than silently
# emitting a changed roster.
EXPECTED_SESSION = 8

DETAIL_LINK_XPATH = ".//a[contains(@href, 'sel=details')]"
NAME_XPATH = ".//div[@class='dep_name']"

# Fields we read from the detail-page key/value table. Any label we don't
# recognise is surfaced via audit_data() so the crawler breaks on new data.
F_DISTRICT = "Sequential number"
F_BIRTH_DATE = "Birth date"
F_PARTY = "Party"
F_FACTION = "Faction"
F_COMMITTEE = "Committee"
F_MEMBERSHIP = "Membership"
F_EMAIL = "E-mail"


def parse_fields(doc: Element) -> dict[str, Element]:
    """Map each detail-page row label to its value element.

    The detail page renders attributes as <div class=description_1>LABEL</div>
    paired with <div class=description_2>VALUE</div> inside the same table row.
    Returning the value element (not text) lets callers reach into structured
    cells such as the faction block, which nests a membership start date.
    """
    fields: dict[str, Element] = {}
    for label_div in h.xpath_elements(doc, ".//div[@class='description_1']"):
        label = h.element_text(label_div)
        rows = h.xpath_elements(label_div, "./ancestor::tr[1]")
        if not rows:
            continue
        values = h.xpath_elements(rows[0], ".//div[contains(@class, 'description_2')]")
        if not values:
            continue
        if label in fields:
            raise ValueError(f"Duplicate detail field: {label!r}")
        fields[label] = values[0]
    return fields


def crawl_person(context: Context, url: str, member_id: str) -> None:
    doc = zyte_api.fetch_html(
        context,
        url,
        unblock_validator=NAME_XPATH,
        html_source="httpResponseBody",
        cache_days=7,
        geolocation=GEOLOCATION,
        absolute_links=True,
    )
    name = h.xpath_string(doc, NAME_XPATH + "/text()")
    if not name:
        context.log.warning("Member without a name", url=url)
        return

    fields = parse_fields(doc)
    birth_date = (
        h.element_text(fields.pop(F_BIRTH_DATE)) if F_BIRTH_DATE in fields else None
    )
    party = h.element_text(fields.pop(F_PARTY)) if F_PARTY in fields else None
    email = h.element_text(fields.pop(F_EMAIL)) if F_EMAIL in fields else None

    # The faction cell lists each membership as a grey <span> start date (e.g.
    # "02.08.2021") followed by the faction name. The first is the term start.
    start_date = None
    if F_FACTION in fields:
        spans = h.xpath_strings(fields.pop(F_FACTION), ".//span/text()")
        if spans:
            start_date = spans[0].strip()

    # The remaining fields are informational and intentionally not modelled:
    # the electoral "Sequential number", committee and interparliamentary
    # memberships. Surface them through audit so new labels break the crawler.
    context.audit_data(
        {k: h.element_text(v) for k, v in fields.items()},
        ignore=[F_DISTRICT, F_COMMITTEE, F_MEMBERSHIP],
    )

    entity = context.make("Person")
    # The numeric member ID is an opaque, stable source identifier.
    entity.id = context.make_slug(member_id)
    h.apply_name(entity, full=name)
    entity.add("sourceUrl", url)
    entity.add("political", party)
    entity.add("email", email)
    # The Electoral Code of the Republic of Armenia requires deputies to hold
    # only Armenian citizenship (no dual nationals); see Article 48 of the
    # Constitution and the Electoral Code:
    # http://www.parliament.am/legislation.php?sel=show&ID=2020&lang=eng
    entity.add("citizenship", "am")
    h.apply_date(entity, "birthDate", birth_date)

    position = h.make_position(
        context,
        name="Member of the National Assembly of Armenia",
        wikidata_id="Q17277248",
        country="am",
        topics=["gov.legislative", "gov.national"],
        lang="eng",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return

    occupancy = h.make_occupancy(
        context,
        person=entity,
        position=position,
        start_date=start_date,
        categorisation=categorisation,
    )
    if occupancy is not None:
        context.emit(occupancy)
        context.emit(position)
        context.emit(entity)


def crawl(context: Context) -> None:
    doc = zyte_api.fetch_html(
        context,
        context.data_url,
        unblock_validator=DETAIL_LINK_XPATH,
        html_source="httpResponseBody",
        cache_days=1,
        geolocation=GEOLOCATION,
        absolute_links=True,
    )

    sessions: set[int] = set()
    for link in h.xpath_elements(doc, ".//a[contains(@href, 'show_session=')]"):
        href = link.get("href")
        if href is None:
            continue
        sessions.add(int(href.split("show_session=")[1].split("&")[0]))
    if sessions and max(sessions) != EXPECTED_SESSION:
        raise ValueError(
            f"Unexpected convocation on the roster: found sessions {sorted(sessions)}, "
            f"expected {EXPECTED_SESSION}. A new term may have been seated; review the source."
        )

    seen: set[str] = set()
    for link in h.xpath_elements(doc, DETAIL_LINK_XPATH):
        url = link.get("href")
        if url is None or "ID=" not in url:
            continue
        member_id = url.split("ID=")[1].split("&")[0]
        if member_id in seen:
            continue
        seen.add(member_id)
        crawl_person(context, url, member_id)

    if len(seen) < 90:
        raise ValueError(f"Only found {len(seen)} members; expected ~107.")
