import re

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

MEMBERS_URL = "https://www.na.gov.pk/en/all-members.php"
PROFILE_URL = "https://www.na.gov.pk/en/profile.php?uid=%s"

# Member profile links look like ``profile.php?uid=1617``; the uid is a stable,
# opaque source identifier we key the person entity on.
UID_RE = re.compile(r"profile\.php\?uid=(\d+)")

# Names carry an honorific prefix ("Mr.", "Ms.", "Dr."). Strip it so the emitted
# name is the person's actual name; the matcher normalises case anyway.
HONORIFIC_RE = re.compile(r"^(mr|mrs|ms|dr)\.?\s+", re.IGNORECASE)


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    uid: str,
) -> None:
    url = PROFILE_URL % uid
    doc = context.fetch_html(url, cache_days=7)

    # The profile is a simple label/value table (``<tr><th>Label</th><td>…</td>``).
    data: dict[str, str] = {}
    rows = h.xpath_elements(
        doc, '//table[contains(@class, "profile_tbl")]//tr[th and td]'
    )
    for row in rows:
        label = h.element_text(h.xpath_element(row, "./th"))
        value = h.element_text(h.xpath_element(row, "./td[1]"))
        data[label] = value

    name = HONORIFIC_RE.sub("", data.pop("Name", "")).strip()
    if not name:
        context.log.warning("Member profile without a name", uid=uid, url=url)
        return

    person = context.make("Person")
    person.id = context.make_slug(uid)
    person.add("name", name)
    person.add("sourceUrl", url)

    # "IND" denotes an independent (no party), not a political affiliation.
    party = data.pop("Party", None)
    if party is not None and party != "IND":
        person.add("political", party)

    # CNIC is the Pakistani national ID number; the field is usually absent or a
    # "-" placeholder, so only record values that carry actual digits.
    cnic = data.pop("CNIC", None)
    if cnic is not None and any(char.isdigit() for char in cnic):
        person.add("idNumber", cnic)
    # A member of Parliament must be a citizen of Pakistan under Article 62(1)(a)
    # of the Constitution of Pakistan, 1973 ("he is a citizen of Pakistan").
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

    # Deliberately dropped: private residences, phone number, father's name (no
    # FTM property), and the constituency/province (kept off the position name).
    context.audit_data(
        data,
        ignore=[
            "Father's Name",
            "Father/Husband's Name",
            "Permanent Address",
            "Local Address",
            "Contact Number",
            "Province",
            "Constituency",
            "Email",
        ],
    )

    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the National Assembly of Pakistan",
        country="pk",
        wikidata_id="Q20760546",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    context.emit(position)

    doc = context.fetch_html(MEMBERS_URL, cache_days=1)
    hrefs = h.xpath_strings(doc, '//a[contains(@href, "profile.php?uid=")]/@href')
    uids = {match.group(1) for href in hrefs if (match := UID_RE.search(href))}
    if not uids:
        raise ValueError("No member profile links found on %s" % MEMBERS_URL)
    context.log.info("Found members", count=len(uids))

    for uid in sorted(uids, key=int):
        crawl_member(context, position, categorisation, uid)
