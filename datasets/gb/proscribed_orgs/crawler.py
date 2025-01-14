"""
Crawl Schedule 2 of the Terrorism Act 2000 and output entities for
proscribed terrorist groups and organizations.
"""

import re
from typing import Optional

from zavod import Context
from zavod import helpers as h
from lxml.html import HtmlElement


ADDITION_RE = re.compile(r"(added|inserted)\s+\(([^)]+)\)", re.IGNORECASE)
INCLUDING1_RE = re.compile(r"(.*)\s*\(including\s+(.*)\)", re.IGNORECASE)
INCLUDING2_RE = re.compile(r"(.*),?\s*including\s+(.*)", re.IGNORECASE)
COMMA_AND_RE = re.compile(r",\s+|\s+and\s+", re.IGNORECASE)
ALIAS_RE = re.compile(r"\s+\(([^)]+)\)")
JUNK = " ,.;\t\r\n"


def parse_comment(text: str) -> Optional[str | None]:
    """
    Parse stack of  commentary from the act looking for the most
    recent date when groups were added.
    """
    m = ADDITION_RE.search(text)
    if m is None:
        return None
    return m.group(2)


def crawl_group(context: Context, text: str):
    """
    Process a group in the list along with associated stack of commentary.
    """
    text = text.strip(JUNK)
    if not text:
        return
    entity = context.make("Organization")
    entity.id = context.make_id(text)
    entity.add("topics", "crime.terror")
    names = []
    m = INCLUDING1_RE.match(text)
    if m:
        names.append(m.group(1))
        names.extend(COMMA_AND_RE.split(m.group(2)))
        context.log.debug(f"Including(1): {names}")
    else:
        m = INCLUDING2_RE.match(text)
        if m:
            names.append(m.group(1))
            names.extend(COMMA_AND_RE.split(m.group(2)))
            context.log.debug(f"Including(2): {names}")
        else:
            names.append(text)
    context.log.debug(f"Names and aliases: {names}")
    alt_names = []
    for name in names:
        aka = []
        for m in ALIAS_RE.finditer(name):
            alias = m.group(1)
            context.log.debug(f"alias: {alias}")
            # Special case for single nested parenthesis
            before, _, after = alias.partition("(")
            if after:
                aka.append(before.strip(JUNK))
                aka.append(after.strip(JUNK))
            else:
                aka.append(alias.strip(JUNK))
        aka.insert(0, ALIAS_RE.sub("", name).strip(JUNK))
        alt_names.append(aka)
    context.log.debug(f"Names and aliases: {alt_names}")
    for aka in alt_names:
        context.log.info(f"Adding primary name: {aka[0]}")
        h.apply_name(entity, aka[0])
        for alias in aka[1:]:
            context.log.info(f"Adding alias: {alias}")
            h.apply_name(entity, alias, alias=True)
    sanction = h.make_sanction(context, entity)
    sanction.set("authority", "UK Home Secretary")
    sanction.add("sourceUrl", context.data_url)
    context.emit(entity)
    context.emit(sanction)


def crawl(context: Context):
    context.log.info(f"Fetching legislation from {context.data_url}")
    page: HtmlElement = context.fetch_html(context.data_url, cache_days=1)
    ulists = page.find_class("LegUnorderedList")
    assert len(ulists) == 1, ("Expected exactly one list", len(ulists))

    for entry in ulists[0].find_class("LegListTextStandard"):
        for el in entry.xpath(
            ".//*[contains(@class, 'LegCommentaryLink') or contains(@class, 'LegChangeDelimiter')]"
        ):
            el.getparent().remove(el)
        crawl_group(context, entry.text_content())
