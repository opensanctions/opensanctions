from lxml import etree
from typing import Optional
from urllib.parse import urljoin
from zipfile import ZipFile

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity


def create_emit_position(context: Context) -> Entity:
    position = h.make_position(
        context,
        name="Member of the German Bundestag",
        country="de",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q1939555",
    )
    context.emit(position)
    return position


def crawl_person(context: Context, mdb: etree._Element, position: Entity) -> None:
    person = context.make("Person")
    person.id = context.make_slug("mdb", mdb.findtext("./ID"))

    # STERBEDATUM: Date of death
    if mdb.findtext(".//STERBEDATUM"):
        # Skip deceased persons
        return

    person.add("citizenship", "de")
    for nel in mdb.findall("./NAMEN/NAME"):
        person.add("title", nel.findtext("./ANDREDE_TITLE"))
        h.apply_name(
            person,
            first_name=nel.findtext("./VORNAME"),
            last_name=nel.findtext("./NACHNAME"),
        )
    h.apply_date(person, "birthDate", mdb.findtext(".//GEBURTSDATUM"))
    person.add("birthPlace", mdb.findtext(".//GEBURTSORT"))
    person.add("birthCountry", mdb.findtext(".//GEBURTSLAND"))
    person.add("gender", mdb.findtext(".//GESCHLECHT"))
    person.add("religion", mdb.findtext(".//RELIGION"))
    person.add("political", mdb.findtext(".//PARTEI_KURZ"))
    person.add("education", mdb.findtext(".//BERUF"))

    has_occupancy = False
    for period in mdb.findall(".//WAHLPERIODE"):
        occupancy = h.make_occupancy(
            context,
            person,
            position,
            # MDBWP: Mitglied des Bundestags (MoP) Wahlperiode (term):
            start_date=period.findtext("./MDBWP_VON"),
            end_date=period.findtext("./MDBWP_BIS"),
            birth_date=mdb.findtext(".//GEBURTSDATUM"),
            propagate_country=True,
        )
        if occupancy is not None:
            has_occupancy = True
            context.emit(occupancy)

    if has_occupancy:
        context.emit(person)


def crawl(context: Context) -> None:
    html = context.fetch_html(context.data_url)

    xml_url: Optional[str] = None
    for link in html.findall(".//ul[@class='bt-linkliste']//a"):
        url = urljoin(context.data_url, link.get("href"))
        if not url.endswith("Stammdaten.zip"):
            continue
        xml_url = url

    assert xml_url, "Could not find XML data URL"

    path = context.fetch_resource("source.zip", url)
    context.export_resource(path, "application/zip", title=context.SOURCE_TITLE)
    with ZipFile(path, "r") as zip:
        for name in zip.namelist():
            if name.lower().endswith(".xml"):
                with zip.open(name) as fh:
                    doc = etree.parse(fh)
                    position = create_emit_position(context)
                    for mdb in doc.findall(".//MDB"):
                        crawl_person(context, mdb, position)
