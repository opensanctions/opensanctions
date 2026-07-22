import re

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise


# Member profile links look like ``profile.php?uid=1017``; the uid is a stable,
# opaque source identifier we key the person entity on.
UID_RE = re.compile(r"profile\.php\?uid=(\d+)")


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    uid: str,
) -> None:
    member_url = (
        context.data_url.replace(
            "current_members.php", "profile.php?uid=%s&catid=0&subcatid=0&cattitle=0"
        )
        % uid
    )
    doc = context.fetch_html(member_url, cache_days=7)

    # The profile info table carries no class/id, so identify it as the table that
    # contains the "Name:" row. It is a vertical label/value table where td[1] is the
    # label (with a trailing colon) and td[2] is the value.
    table = h.xpath_element(
        doc,
        '//div[@id="printableArea"]'
        '//table[.//td[starts-with(normalize-space(.), "Name:")]]',
    )
    data: dict[str, str] = {}
    for row in h.xpath_elements(table, "./tr[td[2]]"):
        label = h.element_text(h.xpath_element(row, "./td[1]")).rstrip(":").strip()
        value = h.element_text(h.xpath_element(row, "./td[2]"))
        if label:
            data[label] = value

    raw_name = data.pop("Name")
    name = h.strip_name_titles(context, raw_name)
    original_name = raw_name if name != raw_name else None
    assert name

    person = context.make("Person")
    person.id = context.make_slug(uid)
    person.add("name", name, lang="eng", original_value=original_name)
    person.add("sourceUrl", member_url)

    party = data.pop("Party", None)
    if party is not None and not party.startswith("Independent"):
        person.add("political", party)

    # A member of Majlis-e-Shoora (Parliament), which includes the Senate, must be a
    # citizen of Pakistan under Article 62(1)(a) of the Constitution of Pakistan, 1973
    # ("he is a citizen of Pakistan").
    # https://www.pakistani.org/pakistan/constitution/part3.ch2.html
    person.add("citizenship", "pk")

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        no_end_implies_current=True,
        start_date=data.pop("Oath Taking Date", None),
        categorisation=categorisation,
    )
    if occupancy is None:
        return

    # Senators represent a province rather than a single-member constituency.
    province = data.pop("Province", None)
    if province is not None:
        occupancy.add("constituency", province)

    context.audit_data(
        data,
        ignore=[
            "Tenure",
            "Seat Description",
            "Designation",
            "In Vice of",
            "Committee Member",
        ],
    )

    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Senate of Pakistan",
        country="pk",
        wikidata_id="Q20081441",
    )
    categorisation = categorise(context, position)
    context.emit(position)

    doc = context.fetch_html(context.data_url, cache_days=1)
    hrefs = h.xpath_strings(doc, '//a[contains(@href, "profile.php?uid=")]/@href')
    uids = {match.group(1) for href in hrefs if (match := UID_RE.search(href))}

    for uid in sorted(uids, key=int):
        crawl_member(context, position, categorisation, uid)
