import re

from rigour.mime.types import XML

from zavod import Context
from zavod import helpers as h

ALIAS_SPLITS = ["; ", ", "]
PROGRAM_KEY = "CA-UNSC1373"
# A comma-separated list or lowercase (not just acronym) within parentheses
REGEX_NAME_NEEDS_CHECKING = re.compile(r"\([^)]+[,a-z]")


def name_needs_checking(name: str) -> bool:
    return bool(REGEX_NAME_NEEDS_CHECKING.search(name))


def crawl(context: Context):
    path = context.fetch_resource("source.xml", context.data_url)
    context.export_resource(path, XML, title=context.SOURCE_TITLE)
    doc = context.parse_resource_xml(path)
    doc = h.remove_namespace(doc)
    for node in doc.findall("./entry"):
        entity = context.make("Organization")
        name = node.findtext("./title")
        entity.id = context.make_slug(node.findtext("./id"), name)
        entity.add("name", name)

        link = node.find("./link").get("href")
        entity.add("sourceUrl", link)
        aliases_string = node.findtext("./summary")
        weak_aliases = None
        if aliases_string != "N/A":
            if name_needs_checking(aliases_string):
                res = context.lookup("aliases", aliases_string, warn_unmatched=True)
                aliases = res.aliases if res else []
                weak_aliases = res.weak_aliases if res else []
            else:
                aliases = h.multi_split(aliases_string, ALIAS_SPLITS)
            entity.add("alias", aliases)
            entity.add("weakAlias", weak_aliases)
        entity.add("notes", node.findtext("./content"))
        entity.add("createdAt", node.findtext("./published"))
        entity.add("modifiedAt", node.findtext("./updated"))
        entity.add("topics", "crime.terror")

        sanction = h.make_sanction(context, entity, program_key=PROGRAM_KEY)
        context.emit(sanction)
        context.emit(entity)
