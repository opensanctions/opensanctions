from lxml.etree import _Element
from typing import Dict, List, Optional
from rigour.mime.types import HTML, CSV
import re
from lxml import html
import csv

from zavod import Context
from zavod import helpers as h


ASSOCIATIONS_LABEL = "UNLAWFUL ASSOCIATIONS UNDER SECTION 3 OF UNLAWFUL ACTIVITIES (PREVENTION) ACT, 1967"
ORGANISATIONS_LABEL = "TERRORIST ORGANISATIONS LISTED IN THE FIRST SCHEDULE OF THE UNLAWFUL ACTIVITIES (PREVENTION) ACT, 1967"
INDIVIDUALS_LABEL = "INDIVIDUALS TERRORISTS LISTED IN THE FOURTH SCHEDULE OF THE UNLAWFUL ACTIVITIES (PREVENTION) ACT, 1967"

REGEX_ACRONYM_PARENS = re.compile(r"^(?P<name>.+?)(?P<acronym>\s+\([A-Z-]+\))?$")
REGEX_NUM_NAME = re.compile(r"(\d+)\.\s*")

COMPLEX_TERMS = {
    "wing",
    "associate",
    "affiliate",
    "namely",
    "factions",
    "/",
    "manifestation",
    "formations",
    "front organisations",
    "security council",
    " un ",
}


def crawl_entity(
    context: Context,
    schema: str,
    names_string: str,
    program: str,
    authority_id: str,
    source_url: str,
    detail_url: str | None,
) -> None:
    entity = context.make(schema)
    # Include aliases in ID because there are different individuals whose alias
    # is all that distinguishes them.
    entity.id = context.make_id(names_string)

    # Split a primary name from all names
    names = h.multi_split(names_string, ";@")
    name = names[0]
    aliases = names[1:]

    # Split out acronym in parens from name
    names_match = REGEX_ACRONYM_PARENS.match(name)
    name = names_match.group("name").strip()
    assert name
    if names_match.group("acronym"):
        aliases.append(names_match.group("acronym"))

    entity.add("name", name)
    entity.add("alias", aliases)
    entity.add("sourceUrl", source_url)
    entity.add("sourceUrl", detail_url)
    entity.add("topics", "sanction")

    sanction = h.make_sanction(context, entity, key=program)
    sanction.add("program", program)
    sanction.add("authorityId", authority_id)

    context.emit(entity, target=True)
    context.emit(sanction)

    return entity


def crawl_common(
    context: Context,
    schema: str,
    names: str,
    program: str,
    authority_id: str,
    source_url: str,
    detail_url: List[str],
) -> None:
    if any(term in names.lower() for term in COMPLEX_TERMS):
        res = context.lookup("names", names)
        if res is None:
            context.log.warn("Complex name needs cleaning", url=source_url, name=names)
            crawl_entity(
                context, schema, names, program, authority_id, source_url, detail_url
            )
        else:
            for group in res.entities:
                entity = crawl_entity(
                    context,
                    schema,
                    group["main_name"],
                    program,
                    authority_id,
                    source_url,
                    detail_url,
                )
                if group.get("related_name", None):
                    related = crawl_entity(
                        context,
                        schema,
                        group["related_name"],
                        program,
                        authority_id,
                        source_url,
                        detail_url,
                    )

                    rel = context.make("UnknownLink")
                    rel.id = context.make_id(entity.id, related.id)
                    rel.add("subject", entity.id)
                    rel.add("object", related.id)
                    rel.add("role", group["relationship"])
                    context.emit(rel)
    else:
        crawl_entity(
            context, schema, names, program, authority_id, source_url, detail_url
        )


def crawl_organisations(
    context: Context, url: str, filename: str, program: str
) -> None:
    path = context.fetch_resource(filename, url)
    context.export_resource(path, HTML, filename)
    with open(path, "rb") as fh:
        doc = html.fromstring(fh.read())
    doc.make_links_absolute(url)

    table = doc.xpath(".//table")[0]
    for row in h.parse_html_table(table):
        authority_id = row.pop("sr_no").text_content()
        names = row.pop("title").text_content()
        detail_url = row.pop("download_link").xpath(".//a/@href")
        crawl_common(
            context, "Organization", names, program, authority_id, url, detail_url
        )


def crawl_individuals(context: Context, url: str, filename: str, program: str) -> None:
    path = context.fetch_resource(filename, url)
    context.export_resource(path, HTML, filename)
    with open(path, "rb") as fh:
        doc = html.fromstring(fh.read())
    doc.make_links_absolute(url)

    for item in doc.xpath(".//div[contains(@class, 'views-field-body')]/div/p"):
        names = item.text_content()
        detail_url = item.xpath(".//a/@href")
        parts = REGEX_NUM_NAME.split(names, 1)
        authority_id = parts[1].strip()
        names = parts[2].rstrip(".")
        crawl_common(context, "Person", names, program, authority_id, url, detail_url)


def get_link_by_label(doc: _Element, label: str) -> Optional[str]:
    label_xpath = f".//td[contains(text(), '{label}')]"
    label_cells = doc.xpath(label_xpath)
    assert len(label_cells) == 1

    anchors = label_cells[0].xpath("./following-sibling::td//a")
    assert len(anchors) == 1

    link = anchors[0]
    return link.get("href")


def parse_names(field: str) -> List[str]:
    names: List[str] = []
    for value in field.split(";"):
        value = value.strip()
        if len(value):
            names.append(value)
    return names


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.dataset.url, cache_days=1)
    doc.make_links_absolute(context.dataset.url)

    associations_url = get_link_by_label(doc, ASSOCIATIONS_LABEL)
    crawl_organisations(
        context, associations_url, "associations.html", ASSOCIATIONS_LABEL
    )

    url = get_link_by_label(doc, ORGANISATIONS_LABEL)
    crawl_organisations(context, url, "organisations.html", ORGANISATIONS_LABEL)

    url = get_link_by_label(doc, INDIVIDUALS_LABEL)
    crawl_individuals(context, url, "individuals.html", INDIVIDUALS_LABEL)

    # Temporarily also emit the manually-curated CSV
    path = context.fetch_resource("source.csv", context.data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    named_ids: Dict[str, str] = {}
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            entity = context.make(row.pop("Type", "LegalEntity"))
            name = row.pop("Name")
            aliases = row.pop("Aliases")
            weak_aliases = row.pop("Weak")
            source_url = row.pop("SourceURL")
            if name is None:
                context.log.warn("No name", row=row)
                continue
            entity.id = context.make_id(name, aliases, weak_aliases)
            assert entity.id is not None, row
            named_ids[name] = entity.id
            entity.add("name", name)
            entity.add("notes", row.pop("Notes"))
            entity.add("topics", "sanction")
            entity.add("sourceUrl", source_url)
            entity.add("alias", parse_names(aliases))
            entity.add("alias", parse_names(weak_aliases))

            id_ = row.pop("ID")
            sanction = h.make_sanction(context, entity, id_)
            sanction.add("program", row.pop("Designation"))
            sanction.add("authorityId", id_)

            linked = row.pop("Linked", "").strip()
            if len(linked) and linked in named_ids:
                rel = context.make("UnknownLink")
                rel.id = context.make_id(linked, "linked", entity.id)
                rel.add("subject", named_ids[linked])
                rel.add("object", entity.id)
                context.emit(rel)

            context.emit(entity, target=True)
            context.emit(sanction)
