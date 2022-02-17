from pantomime.types import XML

from opensanctions.core import Context
from opensanctions import helpers as h

FORMATS = ["%d.%m.%Y"]


def make_entity(context: Context, el, schema, *keys):
    entity = context.make(schema, target=True)
    entity.id = context.make_slug(el.findtext("./num"), *keys)
    entity.add("notes", h.clean_note(el.findtext("./note")))
    entity.add("topics", "sanction")

    sanction = h.make_sanction(context, entity)
    sanction.add("summary", el.findtext("./correction"))
    context.emit(sanction)

    return entity


def crawl(context: Context):
    path = context.fetch_resource("source.xml", context.dataset.data.url)
    context.export_resource(path, XML, title=context.SOURCE_TITLE)

    doc = context.parse_resource_xml(path)
    for el in doc.findall(".//person"):
        fname = el.findtext("./fname")
        mname = el.findtext("./mname")
        lname = el.findtext("./lname")
        name = h.make_name(given_name=fname, middle_name=mname, last_name=lname)
        entity = make_entity(context, el, "Person", "person", name)
        h.apply_name(entity, given_name=fname, middle_name=mname, last_name=lname)
        entity.add("idNumber", el.findtext("./iin"))
        bdate = el.findtext("./birthdate")
        entity.add("birthDate", h.parse_date(bdate, FORMATS, bdate))
        context.emit(entity, target=True)

    for el in doc.findall(".//org"):
        name = el.findtext(".//org_name")
        entity = make_entity(context, el, "Organization", "org", name)
        for tag in (".//org_name", ".//org_name_en"):
            names = el.findtext(tag)
            if names is None:
                continue
            names = names.split("; ")
            entity.add("name", names)

        context.emit(entity, target=True)
