from zavod import Context
from zavod import helpers as h
from zavod.shed.fsf import parse_entry, parse_sanctions

URL = "https://www.sanctionsmap.eu/api/v1/travelbans/file/%s"


def salvage_entity(context: Context, entry):
    texts = [t.text for t in entry.findall("./remark")]
    assert len(texts) == 2, texts
    name, details = texts
    name = name.split("(", 1)[0]
    entity = context.make("LegalEntity")
    entity.id = context.make_id(name)
    entity.add("name", name)
    entity.add("notes", details)
    entity.add("topics", "sanction")
    parse_sanctions(context, entity, entry, program_attrib="numberTitle")
    context.emit(entity)


def crawl(context: Context):
    data = context.fetch_json(context.data_url)
    for ban in data.get("data", {}).get("travelBansFiles"):
        if not ban.get("fileName").endswith(".xml"):
            continue
        data_url = URL % ban.get("id")
        path = context.fetch_resource("source.xml", data_url)
        context.export_resource(path, "text/xml", title=context.SOURCE_TITLE)
        doc = context.parse_resource_xml(path)
        doc = h.remove_namespace(doc)
        for entry in doc.findall(".//sanctionEntity"):
            subject_type = entry.find("./subjectType")
            if subject_type is None:
                salvage_entity(context, entry)
                continue
            parse_entry(context, entry)
