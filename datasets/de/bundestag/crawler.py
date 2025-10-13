from typing import Optional
from urllib.parse import urljoin

from zavod import Context
from zavod import helpers as h


def crawl_xml(context: Context, url: str) -> None:
    path = context.fetch_resource("source.xml", url)
    context.export_resource(path, "text/xml", title=context.SOURCE_TITLE)
    doc = context.parse_resource_xml(path)

    position = h.make_position(
        context,
        name="Member of the German Bundestag",
        country="de",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q1939555",
    )
    context.emit(position)

    for mdb in doc.findall(".//MDB"):
        person = context.make("Person")
        person.id = context.make_slug("mdb", mdb.findtext("./ID"))

        # STERBEDATUM: Date of death
        if mdb.findtext(".//STERBEDATUM"):
            # Skip deceased persons
            continue

        # person.add("topics", "role.pep")
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
        if not url.endswith("Stammdaten.xml"):
            continue
        xml_url = url

    assert xml_url, "Could not find XML data URL"
    crawl_xml(context, xml_url)
