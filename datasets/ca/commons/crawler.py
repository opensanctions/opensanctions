from lxml import etree

from zavod import Context
from zavod import helpers as h

MINISTERS = "https://www.ourcommons.ca/Members/en/ministries/XML"


def crawl_member(context: Context, el: etree._Element) -> None:
    """Crawl a single member element."""
    person_id = el.findtext("PersonId")
    if not person_id:
        return
    entity = context.make("Person")
    entity.id = context.make_slug("person", person_id)
    entity.add("citizenship", "ca")
    entity.add("topics", "role.pep")
    entity.add("sourceUrl", f"https://www.ourcommons.ca/Members/en/{person_id}")
    # <MemberOfParliament>
    # <PersonId>28286</PersonId>
    # <PersonShortHonorific>Right Hon.</PersonShortHonorific>
    entity.add("title", el.findtext("PersonShortHonorific"))
    # <PersonOfficialFirstName>Mark</PersonOfficialFirstName>
    # <PersonOfficialLastName>Carney</PersonOfficialLastName>
    h.apply_name(
        entity,
        first_name=el.findtext("PersonOfficialFirstName"),
        last_name=el.findtext("PersonOfficialLastName"),
    )
    # <ConstituencyName>Nepean</ConstituencyName>
    # <ConstituencyProvinceTerritoryName>Ontario</ConstituencyProvinceTerritoryName>
    constituency_name = el.findtext("ConstituencyName")
    constituency_province = el.findtext("ConstituencyProvinceTerritoryName")
    constituency = ", ".join(filter(None, [constituency_name, constituency_province]))
    entity.add("address", constituency)
    # <CaucusShortName>Liberal</CaucusShortName>
    entity.add("political", el.findtext("CaucusShortName"))

    position = h.make_position(
        context,
        "Member of the House of Commons of Canada",
        wikidata_id="Q15964890",
        country="ca",
        topics=["gov.legislative", "gov.national"],
    )
    context.emit(position)
    # <FromDateTime>2025-04-28T00:00:00</FromDateTime>
    # <ToDateTime xsi:nil="true"/>
    occupancy = h.make_occupancy(
        context,
        person=entity,
        position=position,
        start_date=el.findtext("FromDateTime"),
        end_date=el.findtext("ToDateTime"),
        no_end_implies_current=True,
        propagate_country=False,
    )
    if occupancy is not None:
        context.emit(occupancy)

    context.emit(entity)
    # </MemberOfParliament>


def yes_minister(context: Context, el: etree._Element) -> None:
    """Crawl a single minister element."""
    person_id = el.findtext("PersonId")
    if not person_id:
        return
    entity = context.make("Person")
    entity.id = context.make_slug("person", person_id)
    entity.add("citizenship", "ca")
    entity.add("topics", "role.pep")
    # <Minister>
    # <PersonId>28286</PersonId>
    # <OrderOfPrecedence>1</OrderOfPrecedence>
    # <PersonShortHonorific>Right Hon.</PersonShortHonorific>
    entity.add("title", el.findtext("PersonShortHonorific"))
    # <PersonOfficialFirstName>Mark</PersonOfficialFirstName>
    # <PersonOfficialLastName>Carney</PersonOfficialLastName>
    h.apply_name(
        entity,
        first_name=el.findtext("PersonOfficialFirstName"),
        last_name=el.findtext("PersonOfficialLastName"),
    )
    # <Title>Prime Minister</Title>
    title = el.findtext("Title")
    assert title is not None, "Missing position title"
    position = h.make_position(
        context,
        title,
        country="ca",
        topics=["gov.executive", "gov.national"],
    )
    context.emit(position)
    # <FromDateTime>2025-03-14T11:31:00</FromDateTime>
    # <ToDateTime xsi:nil="true"/>
    occupancy = h.make_occupancy(
        context,
        person=entity,
        position=position,
        start_date=el.findtext("FromDateTime"),
        end_date=el.findtext("ToDateTime"),
        no_end_implies_current=True,
        propagate_country=False,
    )
    if occupancy is not None:
        context.emit(occupancy)
    # </Minister>
    context.emit(entity)


def crawl(context: Context) -> None:
    # Members XML:
    data = context.fetch_text(context.data_url)
    assert data is not None, "Failed to fetch data"
    doc = etree.fromstring(data)
    for el in doc.findall(".//MemberOfParliament"):
        crawl_member(context, el)

    # Ministers XML:
    data = context.fetch_text(MINISTERS)
    assert data is not None, "Failed to fetch ministers data"
    doc = etree.fromstring(data)
    for el in doc.findall(".//Minister"):
        yes_minister(context, el)
