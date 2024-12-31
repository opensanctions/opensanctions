from typing import cast
from rigour.mime.types import XML

from zavod import Context
from zavod import helpers as h

from urllib.parse import urljoin


CATEGORY1_PROGRAM = "Kazakh Terror Financing list"
CATEGORY2_PROGRAM = "Participants in terrorist activities"
CATEGORY1_URL = "xml_category_1/?status=acting"
CATEGORY2_URL = "xml_category_2/?status=acting"
CATEGORY1_EXPORT = "terrorism-financiers-source.xml"
CATEGORY2_EXPORT = "terrorists-source.xml"


def added_date_from_note(text: str) -> str | None:
    # "Added from"
    if text and text.startswith("включен от"):
        start_date = text.replace("включен от ", "").strip()
        return start_date
    return None


def make_entity(context: Context, el, schema, entity_id, topics, program):
    entity = context.make(schema)
    entity.id = entity_id
    entity.add("notes", h.clean_note(el.findtext("./note")))
    entity.add("topics", topics)

    sanction = h.make_sanction(context, entity)
    sanction.add("summary", el.findtext("./correction"), lang="rus")
    sanction.add("program", program)
    listingDate = cast(str | None, el.findtext("./added_to_list"))
    note_date = added_date_from_note(el.findtext("./correction"))
    h.apply_date(sanction, "listingDate", listingDate or note_date)

    context.emit(sanction)
    return entity


def crawl(context: Context):
    crawl_financiers(context)
    crawl_terrorists(context)


def crawl_financiers(context: Context):
    path = context.fetch_resource(
        CATEGORY1_EXPORT, urljoin(context.data_url, CATEGORY1_URL)
    )
    context.export_resource(path, XML, title=CATEGORY1_PROGRAM)

    doc = context.parse_resource_xml(path)
    for el in doc.findall(".//person"):
        fname = el.findtext("./fname")
        mname = el.findtext("./mname")
        lname = el.findtext("./lname")
        bdate = el.findtext("./birthdate")
        iin = el.findtext("./iin")
        name = h.make_name(given_name=fname, middle_name=mname, last_name=lname)
        entity_id = context.make_id(name, bdate, iin)
        entity = make_entity(
            context, el, "Person", entity_id, "sanction", CATEGORY1_PROGRAM
        )
        h.apply_name(entity, given_name=fname, middle_name=mname, last_name=lname)
        entity.add("idNumber", iin)
        h.apply_date(entity, "birthDate", bdate)
        context.emit(entity, target=True)

        if iin:
            ident = h.make_identification(
                context,
                entity,
                number=iin,
                doc_type="IIN",
                country="kz",
            )
            context.emit(ident)

    for el in doc.findall(".//org"):
        name = el.findtext(".//org_name")
        entity_id = context.make_id(el.findtext("./note"), name)
        entity = make_entity(
            context, el, "Organization", entity_id, "sanction", CATEGORY1_PROGRAM
        )
        for tag in (".//org_name", ".//org_name_en"):
            names = el.findtext(tag)
            if names is None:
                continue
            entity.add("name", names.split("; "))
        context.emit(entity, target=True)


def crawl_terrorists(context: Context):
    # Note: This list specifies that it refers to individuals but uses organisation xml tags
    path = context.fetch_resource(
        CATEGORY2_EXPORT, urljoin(context.data_url, CATEGORY2_URL)
    )
    context.export_resource(path, XML, title=CATEGORY2_PROGRAM)

    doc = context.parse_resource_xml(path)
    for el in doc.findall(".//org"):
        name = el.findtext(".//org_name")
        iin = el.findtext("./org_iin")
        identification = [name]
        if iin:
            identification.append(iin)
        else:
            identification.append(el.findtext("./note"))
        entity_id = context.make_id(*identification)
        entity = make_entity(
            context,
            el,
            "Person",
            entity_id,
            ["sanction", "crime.terror"],
            CATEGORY2_PROGRAM,
        )
        entity.add("name", name)
        entity.add("idNumber", iin)
        context.emit(entity)
