"""
Crawl Schedule 2 of the Terrorism Act 2000 and output entities for
proscribed terrorist groups and organizations.
"""

import re
from typing import List

from zavod import Context
from zavod import helpers as h
from lxml.html import HtmlElement


ADDITION_RE = re.compile(r"(added|inserted)\s+\(([^)]+)\)", re.IGNORECASE)
INCLUDING1_RE = re.compile(r"(.*)\s*\(including\s+(.*)\)", re.IGNORECASE)
INCLUDING2_RE = re.compile(r"(.*),?\s*including\s+(.*)", re.IGNORECASE)
COMMA_AND_RE = re.compile(r",\s+|\s+and\s+", re.IGNORECASE)
ALIAS_RE = re.compile(r"\s+\(([^)]+)\)")
JUNK = " ,.;\t\r\n"
DATE_FORMATS = [
    "%d.%m.%Y"
]


def parse_comment(context: Context, text: str) -> List[str]:
    """
    Parse stack of  commentary from the act looking for the most
    recent date when groups were added.
    """
    m = ADDITION_RE.search(text)
    if m is None:
        return []
    return h.parse_date(m.group(2), DATE_FORMATS)


def crawl_group(context: Context, text: str, change_stack: List[str]):
    """
    Process a group in the list along with associated stack of commentary.
    """
    text = text.strip(JUNK)
    if not text:
        return
    add_date = []
    for item in change_stack:
        add_date = parse_comment(context, item)
        if add_date:
            break
    context.log.info(f"Adding group: {text} (starting {add_date})")
    entity = context.make("Organization")
    entity.id = context.make_id(text)
    entity.add("topics", "crime.terror")
    context.log.debug(f"Unique ID {entity.id}")
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
    sanction.add("authority", "UK Home Secretary")
    if add_date:
        sanction.add("startDate", add_date)
    context.emit(entity, target=True)
    context.emit(sanction)


def crawl(context: Context):
    """
    Process Schedule 2 of the Terrorism Act 2000.
    """
    context.log.info(f"Fetching legislation from {context.data_url}")
    page: HtmlElement = context.fetch_html(context.data_url, cache_days=1)
    ulists = page.find_class("LegUnorderedList")
    if len(ulists) > 1:
        context.log.warning("Multiple [@class=LegUnorderedList] found in text")
    elif len(ulists) == 0:
        context.log.error("No [@class=LegUnorderedList] found in text")
        return
    change_stack = []
    for entry in ulists[0].find_class("LegListTextStandard"):
        if len(entry) == 0:
            context.log.debug(f"Original group: {entry.text.strip()}")
            crawl_group(context, entry.text.strip(), change_stack)
            continue
        text = []
        for el in entry:
            classnames = el.get("class").split()
            if "LegChangeDelimiter" in classnames:
                bracket = el.text.strip()
                if bracket == "[":
                    link = el.getnext()
                    comment_id = link.get("href")[1:]
                    commentary = page.find(f".//div[@id='{comment_id}']")
                    comment_text = commentary.text_content()
                    context.log.debug(f"Start subgroup: {comment_text}")
                    change_stack.append(comment_text)
                elif bracket == "]":
                    crawl_group(context, "".join(text), change_stack)
                    context.log.debug("End subgroup")
                    change_stack.pop()
                    text = []
            elif "LegAddition" in classnames:
                context.log.debug(f"Addition: {el.text}")
                if el.text is not None:
                    text.append(el.text)
            elif "LegSubstitution" in classnames:
                context.log.debug(f"Substitution: {el.text}")
                if el.text is not None:
                    text.append(el.text)
        if text:
            crawl_group(context, "".join(text), change_stack)
