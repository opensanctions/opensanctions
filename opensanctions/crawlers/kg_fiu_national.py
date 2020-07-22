import re
from pprint import pprint  # noqa
from datetime import datetime
from ftmstore.memorious import EntityEmitter

from opensanctions.util import jointext


def parse_date(text):
    pattern = re.compile(r"[^0-9\.]")
    if text is not None:
        date = pattern.sub("", text)
        try:
            date = datetime.strptime(text, "%d.%m.%Y")
            return date.date().isoformat()
        except ValueError:
            return text


def parse_person(emitter, node):
    entity = emitter.make("Person")
    last_name = node.findtext("./Surname")
    entity.add("lastName", last_name)
    first_name = node.findtext("./Name")
    entity.add("firstName", first_name)
    patronymic = node.findtext("./Patronomic")
    entity.add("fatherName", patronymic)
    entity.add("name", jointext(first_name, patronymic, last_name))
    entity.add("birthDate", parse_date(node.findtext("./DataBirth")))
    entity.add("birthPlace", node.findtext("./PlaceBirth"))
    parse_common(emitter, node, entity)


def parse_legal(emitter, node):
    entity = emitter.make("LegalEntity")
    names = node.findtext("./Name")
    entity.add("name", names.split(", "))
    parse_common(emitter, node, entity)


def parse_common(emitter, node, entity):
    entity.make_id(node.tag, node.findtext("./Number"))
    sanction = emitter.make("Sanction")
    sanction.make_id("Sanction", entity.id)
    sanction.add("entity", entity)
    sanction.add("authority", "Kyrgyz Financial Intelligence Unit")
    sanction.add("reason", node.findtext("./BasicInclusion"))
    sanction.add("program", node.findtext("./CategoryPerson"))
    inclusion_date = parse_date(node.findtext("./DateInclusion"))
    sanction.add("startDate", inclusion_date)
    entity.context["created_at"] = inclusion_date
    emitter.emit(entity)
    emitter.emit(sanction)


def parse(context, data):
    emitter = EntityEmitter(context)
    with context.http.rehash(data) as res:
        for person in res.xml.findall(".//KyrgyzPhysicPerson"):
            parse_person(emitter, person)
        for legal in res.xml.findall(".//KyrgyzLegalPerson"):
            parse_legal(emitter, legal)
    emitter.finalize()
