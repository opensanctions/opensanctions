from pantomime.types import XML

from opensanctions.core import Context
from opensanctions import helpers as h

FORMATS = ["%d.%m.%Y"]

def parse_start_date(text: str) -> str | None:
    if text and text.startswith("включен от"):
        start_date = text.replace("включен от ", "").strip()
        return h.parse_date(start_date, FORMATS)


def make_entity(context: Context, el, schema, entity_id):
    entity = context.make(schema)
    entity.id = entity_id
    entity.add("notes", h.clean_note(el.findtext("./note")))
    entity.add("topics", "sanction")

    sanction = h.make_sanction(context, entity)
    sanction.add("summary", el.findtext("./correction"))
    sanction.add("startDate", parse_start_date(el.findtext("./correction")))
    context.emit(sanction)

    return entity


def crawl(context: Context):
    path = context.fetch_resource("source.xml", context.source.data.url)
    context.export_resource(path, XML, title=context.SOURCE_TITLE)

    doc = context.parse_resource_xml(path)
    for el in doc.findall(".//person"):
        fname = el.findtext("./fname")
        mname = el.findtext("./mname")
        lname = el.findtext("./lname")
        bdate = el.findtext("./birthdate")
        iin = el.findtext("./iin")
        name = h.make_name(given_name=fname, middle_name=mname, last_name=lname)
        entity_id = context.make_id(name, bdate, iin)
        entity = make_entity(context, el, "Person", entity_id)
        h.apply_name(entity, given_name=fname, middle_name=mname, last_name=lname)
        entity.add("innCode", iin)
        entity.add("birthDate", h.parse_date(bdate, FORMATS, bdate))
        context.emit(entity, target=True)

    for el in doc.findall(".//org"):
        name = el.findtext(".//org_name")
        entity_id = context.make_id(el.findtext("./note"), name)
        entity = make_entity(context, el, "Organization", entity_id)
        for tag in (".//org_name", ".//org_name_en"):
            names = el.findtext(tag)
            if names is None:
                continue
            names = names.split("; ")
            entity.add("name", names)

        context.emit(entity, target=True)
