import shutil
import re
from lxml import html
from normality import collapse_spaces
from pantomime.types import HTML

from zavod import Context
from zavod import helpers as h

CLEAN = [
    ", all its formations and front organizations.",
    ", all its formations and front organizations",
    ", All its formations and front organizations",
    ", All its formations and Front Organisations",
    "all its formations and front organizations.",
    ", and all its Manifestations",
]
REGEX_PERSON_PREFIX = re.compile(r"^\d+\.")

def crawl(context: Context):
    path = context.fetch_resource(
         "organisations.html",
         context.data_url + "banned-organisations",
    #     headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "},
    )
    context.export_resource(path, HTML)
    context.export_resource(path, HTML, title="Banned organisations")
    with open(path, "rb") as fh:
        doc = html.fromstring(fh.read())
    for row in doc.findall('.//div[@class="field-content"]//table//tr'):
        cells = [c.text for c in row.findall("./td")]
        if len(cells) != 2:
            continue
        serial, name = cells
        if "Organisations listed in the Schedule" in name:
            continue
        entity = context.make("Organization")
        entity.id = context.make_id(serial, name)
        entity.add("topics", "sanction")
        for alias in name.split("/"):
            for clean in CLEAN:
                alias = alias.replace(clean, " ")
            entity.add("name", collapse_spaces(alias))

        context.emit(entity, target=True)

    people_url = context.data_url + "page/individual-terrorists-under-uapa"
    people_path = context.fetch_resource("individuals.html", people_url)
    context.export_resource(people_path, HTML, "Individuals under UAPA")
    with open(people_path, "rb") as fh:
        doc = html.fromstring(fh.read())
    doc.make_links_absolute(people_url)
    for para in doc.findall(".//p"):
        line = collapse_spaces(para.text_content())
        if not line:
            continue
        if not REGEX_PERSON_PREFIX.match(line):
            context.log.warn("Couldn't parse item", item=line)
            continue
        names = REGEX_PERSON_PREFIX.sub("", line)
        names = re.sub(r"\.$", "", names)
        name_list = [n.strip() for n in names.split("@")]
        entity = context.make("Person")
        entity.id = context.make_id(names)
        entity.add("name", name_list.pop(0))
        if name_list:
            entity.add("alias", name_list)
        entity.add("topics", "sanction")
        entity.add("sourceUrl", para.find(".//a").get("href"))
        context.emit(entity, target=True)