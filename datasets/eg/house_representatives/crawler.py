from lxml import etree

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

# The chamber has 596 members; detail pages are enumerated by a sequential id, with ids
# above the chamber size returning an empty placeholder page.
MAX_MEMBER_ID = 596

# Detail pages are ASP.NET WebForms; the member fields are rendered into fixed label ids.
FIELDS = {
    "short_name": "ContentPlaceHolder1_Label1",
    "full_name": "ContentPlaceHolder1_Label2",
    "birth_place": "ContentPlaceHolder1_Label4",
    "membership_type": "ContentPlaceHolder1_Label5",
    "governorate": "ContentPlaceHolder1_Label6",
    "constituency": "ContentPlaceHolder1_Label7",
    "party": "ContentPlaceHolder1_Label8",
}

# Membership types: individual-candidacy seat, closed-list seat, presidential appointee.
MEMBERSHIP_TYPES = {"فردى", "قائمة", "معين"}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}


def field(doc: etree._Element, label_id: str) -> str | None:
    elements = h.xpath_elements(doc, f'//*[@id="{label_id}"]')
    if not elements:
        return None
    text = h.element_text(elements[0])
    return text or None


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    member_id: int,
) -> None:
    url = f"{context.data_url}?id={member_id}"
    doc = context.fetch_html(url, headers=HEADERS, cache_days=7)

    full_name = field(doc, FIELDS["full_name"])
    if full_name is None:
        # Empty placeholder page: id beyond the chamber size or a vacant seat.
        return

    membership_type = field(doc, FIELDS["membership_type"])
    if membership_type is not None and membership_type not in MEMBERSHIP_TYPES:
        context.log.warning("Unknown membership type", value=membership_type, url=url)
    governorate = field(doc, FIELDS["governorate"])
    constituency = field(doc, FIELDS["constituency"])

    person = context.make("Person")
    person.id = context.make_id(full_name, governorate, constituency)
    person.add("name", full_name, lang="ara")
    person.add("name", field(doc, FIELDS["short_name"]), lang="ara")
    person.add("birthPlace", field(doc, FIELDS["birth_place"]), lang="ara")
    person.add("political", field(doc, FIELDS["party"]), lang="ara")
    person.add("sourceUrl", url)
    # Members of the House of Representatives must be Egyptian citizens (Constitution of
    # Egypt 2014, Article 102). https://www.constituteproject.org/constitution/Egypt_2014
    person.add("citizenship", "eg")

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    if constituency is not None:
        occupancy.add("constituency", constituency, lang="ara")
    if governorate is not None:
        occupancy.add("constituency", governorate, lang="ara")

    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the House of Representatives of Egypt",
        country="eg",
        wikidata_id="Q21290857",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    for member_id in range(1, MAX_MEMBER_ID + 1):
        crawl_member(context, position, categorisation, member_id)
