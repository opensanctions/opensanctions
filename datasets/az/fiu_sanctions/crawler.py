from lxml import etree
from rigour.mime.types import XML

from zavod import Context
from zavod import helpers as h


def crawl(context: Context) -> None:
    html_doc = context.fetch_html(context.data_url)
    xml_url = h.xpath_string(html_doc, ".//a[contains(text(), 'XML')]/@href")
    path = context.fetch_resource("source.xml", xml_url)
    context.export_resource(path, XML, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        doc = etree.parse(fh)
    # date = doc.getroot().get("dateGenerated")
    # if date is None:
    #     context.log.error("No date found in XML file")
    #     return
    # datetime.strptime(date, "%Y-%m-%dT%H:%M:%S")
    entities = doc.find("./ENTITIES")
    assert entities is not None
    if len(list(entities.iterchildren())) > 0:
        context.log.warn("Entities found in XML file, parser needs to be adapted")

    for ind in doc.findall("./INDIVIDUALS/INDIVIDUAL"):
        entity = context.make("Person")
        data_id = ind.findtext("DATAID")
        name_original = ind.findtext("NAME_ORIGINAL_SCRIPT")
        entity.id = context.make_id(data_id, name_original)
        entity.add("name", name_original, lang="aze")
        # SECOND NAME and THIRD NAME are murky.
        # For Azeri names, SECOND_NAME is the family name, THIRD_NAME is a patronymic
        # (the part suffixed by "oğlu" in NAME_ORIGINAL_SCRIPT).
        # For Arabic name, SECOND_NAME is a patronymic, THIRD_NAME is the family name.
        #
        # Who knows how else they'll use it, so I'm only confidnent about FIRST_NAME,
        # the rest we just join and stuff in `name`.
        entity.add("firstName", ind.findtext("FIRST_NAME"), lang="eng")
        entity.add(
            "name",
            h.make_name(
                first_name=ind.findtext("FIRST_NAME"),
                second_name=ind.findtext("SECOND_NAME"),
                last_name=ind.findtext("THIRD_NAME"),
            ),
            lang="eng",
        )
        if ind.findtext("FOURTH_NAME"):
            context.log.warn("Fourth name found in individual: %s" % data_id)

        entity.add("birthDate", ind.findtext("./INDIVIDUAL_DATE_OF_BIRTH/DATE"))
        entity.add("country", "az")
        entity.add("topics", "sanction.counter")

        sanction = h.make_sanction(context, entity)
        context.emit(sanction)
        context.emit(entity)
