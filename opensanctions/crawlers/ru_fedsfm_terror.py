from lxml import html
from pantomime.types import HTML
from prefixdate import parse_format

from opensanctions.core import Context
from opensanctions import helpers as h

SECTIONS = {
    "russianUL": ("National part", "Organization"),
    "russianFL": ("National part", "Person"),
    "foreignUL": ("International part", "Organization"),
    "foreignFL": ("International part", "Person"),
}


def parse_name(entity, text):
    text = text.strip().rstrip("*")
    if "(" in text:
        text, akas = text.split("(", 1)
        akas = akas.replace(")", "")
        akas = akas.split(";")
        entity.add("alias", akas)
    text = text.strip().rstrip("*")
    entity.add("name", text)


def parse_russian_orgs(context: Context, entity, text):
    while "," in text:
        text, section = text.rsplit(",", 1)
        fragment = section.strip()
        if not len(fragment):
            continue
        date = parse_format(fragment, "%d.%m.%Y")
        if date.text is not None:
            entity.add("incorporationDate", date)
            continue
        if fragment.startswith("ИНН:"):
            entity.add("innCode", fragment.replace("ИНН:", ""))
            continue
        if fragment.startswith("ОГРН:"):
            entity.add_cast("Company", "ogrnCode", fragment.replace("ОГРН:", ""))
            continue
        text = f"{text},{section}"
        break
    parse_name(entity, text)


async def parse_russian_persons(context: Context, entity, text):
    while "," in text:
        text, section = text.rsplit(",", 1)
        fragment = section.strip()
        if not len(fragment):
            continue
        date = parse_format(fragment, "%d.%m.%Y г.р.")
        if date.text is not None:
            entity.add("birthDate", date)
            continue
        if fragment.startswith("("):
            fragment = fragment.replace(")", "")
            entity.add("alias", fragment)
            continue

        obj = h.make_address(context, full=fragment, country_code="ru")
        await h.apply_address(context, entity, obj)
    parse_name(entity, text)


def parse_foreign_orgs(context, entity, text):
    while text.endswith(","):
        text = text.rstrip(",").strip()
    parse_name(entity, text)


def parse_foreign_persons(context: Context, entity, text):
    while "," in text:
        text, section = text.rsplit(",", 1)
        fragment = section.strip()
        if not len(fragment):
            continue
        date = parse_format(fragment, "%d.%m.%Y г. р.")
        if date.text is not None:
            entity.add("birthDate", date)
            continue
        entity.add("notes", fragment)
    parse_name(entity, text)


async def crawl(context: Context):
    path = await context.fetch_resource("source.html", context.dataset.data.url)
    await context.export_resource(path, HTML, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        doc = html.parse(fh)

    for sec_id, (section, schema) in SECTIONS.items():
        el = doc.find(".//div[@id='%s']" % sec_id)
        for item in el.findall(".//li"):
            text = item.text_content().strip()
            index, text = text.split(".", 1)
            text = text.strip()
            if text.endswith(";"):
                text = text.rstrip(";")
            entity = context.make(schema)
            entity.id = context.make_id(text)
            sanction = h.make_sanction(context, entity)
            sanction.add("program", section)
            sanction.add("recordId", index)
            if sec_id == "russianUL":
                parse_russian_orgs(context, entity, text)
            if sec_id == "russianFL":
                await parse_russian_persons(context, entity, text)
            if sec_id == "foreignUL":
                parse_foreign_orgs(context, entity, text)
            if sec_id == "foreignFL":
                parse_foreign_persons(context, entity, text)

            if entity.has("name"):
                await context.emit(entity, target=True)
                await context.emit(sanction)
