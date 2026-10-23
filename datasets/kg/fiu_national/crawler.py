from zavod.extract import zyte_api
from zavod.util import Element

from zavod import Context, Entity
from zavod import helpers as h


def parse_person(context: Context, node: Element) -> None:
    entity = context.make("Person")

    name = node.findtext("./Name")
    patronymic = node.findtext("./Patronymic")
    surname = node.findtext("./Surname")
    # The ID keys start with the element names of the former fiu.gov.kg/sked/9
    # XML, which keeps IDs consistent with entities first published from it.
    entity.id = context.make_id("KyrgyzPhysicPerson", name, patronymic, surname)
    h.apply_name(
        entity,
        given_name=name,
        patronymic=patronymic if patronymic != "-" else None,
        last_name=surname,
    )
    h.apply_date(entity, "birthDate", node.findtext("./DataBirth"))
    entity.add("birthPlace", node.findtext("./PlaceBirth"))
    entity.add("idNumber", node.findtext("./Pin"))
    emit_target(context, entity)


def parse_legal(context: Context, node: Element) -> None:
    entity = context.make("LegalEntity")
    names = node.findtext("./Name")
    assert names is not None
    entity.id = context.make_id("KyrgyzLegalPerson", names)
    entity.add("name", names.split(", "))
    emit_target(context, entity)


def emit_target(context: Context, entity: Entity) -> None:
    sanction = h.make_sanction(context, entity)
    entity.add("topics", "sanction")
    context.emit(entity)
    context.emit(sanction)


def fetch_list(
    context: Context, list_type: str, filename: str, title: str
) -> list[Element]:
    url = f"https://fiu.gov.kg/api/v1/public/sanction-lists/{list_type}/report?fileType=XML"
    _, _, _, path = zyte_api.fetch_resource(
        context, filename=filename, url=url, expected_media_type="application/xml"
    )
    context.export_resource(path, mime_type="application/xml", title=title)
    return list(context.parse_resource_xml(path).getroot())


def crawl(context: Context) -> None:
    persons = fetch_list(
        context,
        list_type="PERSON_PHYSIC",
        filename="source-physical.xml",
        title="Source data (individuals)",
    )
    for person in persons:
        assert person.tag == "SanctionsListKGPhysicDto", person.tag
        parse_person(context, person)

    legals = fetch_list(
        context,
        list_type="PERSON_LEGAL",
        filename="source-legal.xml",
        title="Source data (legal entities)",
    )
    for legal in legals:
        assert legal.tag == "SanctionsListKGLegalDto", legal.tag
        parse_legal(context, legal)
