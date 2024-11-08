from lxml.etree import _Element
from typing import List, Dict, Set, Optional
from rigour.mime.types import HTML
import re

from zavod import Context
from zavod import helpers as h


ASSOCIATIONS_LABEL = "UNLAWFUL ASSOCIATIONS UNDER SECTION 3 OF UNLAWFUL ACTIVITIES (PREVENTION) ACT, 1967"
ORGANIZATIONS_LABEL = "TERRORIST ORGANISATIONS LISTED IN THE FIRST SCHEDULE OF THE UNLAWFUL ACTIVITIES (PREVENTION) ACT, 1967"
INDIVIDUALS_LABEL = "INDIVIDUALS TERRORISTS LISTED IN THE FOURTH SCHEDULE OF THE UNLAWFUL ACTIVITIES (PREVENTION) ACT, 1967"

REGEX_NAME = re.compile(r"^(?P<name>.+?)(?P<acronym>\(\s+[A-Z-]+)?$")
REGEX_WEAK_ALIAS = re.compile(r"^\s*\(?([A-Z-]+)\)?$")

COMPLEX_TERMS = {
    "wing",
    "associate",
    "affiliate",
    "namely",
    "factions",
    "/",
    "manifestation",
    "formations",
    "front organizations",
    "security council",
    " un ",
}


def crawl_entity(
    context: Context,
    schema: str,
    name: str,
    aliases_string: str | None,
    program: str,
    authority_id: str,
    source_url: str,
    detail_url: str | None,
) -> None:
    entity = context.make(schema)
    entity.id = context.make_id(name, aliases_string)
    entity.add("name", name)

    aliases = h.multi_split(aliases_string, ";")
    weak_alias_matches = [REGEX_WEAK_ALIAS.match(a) for a in aliases]
    print(weak_alias_matches)
    weak_aliases = [m.group(1) for m in weak_alias_matches if m]
    aliases = [a for a in aliases if not REGEX_WEAK_ALIAS.match(a)]
    entity.add("alias", aliases)
    entity.add("weakAlias", weak_aliases)

    entity.add("sourceUrl", source_url)
    entity.add("sourceUrl", detail_url)

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
    names_match = REGEX_NAME.match(names)
    name = names_match.group("name").strip()
    assert name
    acronym = names_match.group("acronym")
    if any(term in name.lower() for term in COMPLEX_TERMS):
        res = context.lookup("names", name)
        if res is None:
            context.log.warn("Complex name needs cleaning", url=source_url, name=name)
            crawl_entity(
                context,
                schema,
                name,
                acronym,
                program,
                authority_id,
                source_url,
                detail_url,
            )
        else:
            for group in res.entities:
                entity = crawl_entity(
                    context,
                    schema,
                    group["main_name"],
                    group.get("main_aliases", None),
                    program,
                    authority_id,
                    source_url,
                    detail_url,
                )
                if group.get("rel_name", None):
                    related = crawl_entity(
                        context,
                        schema,
                        group["rel_name"],
                        group.get("rel_aliases", None),
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
            context,
            schema,
            name,
            acronym,
            program,
            authority_id,
            source_url,
            detail_url,
        )


def crawl_organizations(context: Context, url: str, program: str) -> None:
    doc = context.fetch_html(url, cache_days=1)
    doc.make_links_absolute(url)

    table = doc.xpath(".//table")[0]
    for row in h.parse_html_table(table):
        authority_id = row.pop("sr_no").text_content()
        names = row.pop("title").text_content()
        detail_url = row.pop("download_link").xpath(".//a/@href")
        crawl_common(
            context, "Organization", names, program, authority_id, url, detail_url
        )


def get_link_by_label(doc: _Element, label: str) -> Optional[str]:
    label_xpath = f".//td[contains(text(), '{label}')]"
    label_cells = doc.xpath(label_xpath)
    assert len(label_cells) == 1

    anchors = label_cells[0].xpath("./following-sibling::td//a")
    assert len(anchors) == 1

    link = anchors[0]
    return link.get("href")


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.dataset.url, cache_days=1)
    doc.make_links_absolute(context.dataset.url)

    associations_url = get_link_by_label(doc, ASSOCIATIONS_LABEL)
    crawl_organizations(context, associations_url, ASSOCIATIONS_LABEL)

    url = get_link_by_label(doc, ORGANIZATIONS_LABEL)
    crawl_organizations(context, url, ORGANIZATIONS_LABEL)

    url = get_link_by_label(doc, INDIVIDUALS_LABEL)
