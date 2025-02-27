"""
Crawl the website of the Legislative Herald of Georgia and extract
the individuals on the Otkhozoria–Tatunashvili List.
"""

import re
from typing import Dict
from lxml.html import HtmlElement

from zavod import Context, Entity
from zavod import helpers as h
from zavod.shed.trans import apply_translit_full_name

TARGET = "ბრალდებული/ მსჯავრდებული"
DEMOGRAPHICS = "დემოგრაფიული მონაცემები"
STATUS = "სტატუსი"
UNKNOWN = "უცნობია"
UNUSED_FIELDS = [
    "საქმის მდგომარეობა",
    "დანაშაულის ჩადენის ადგილი",
    "მოკლე ფაბულა",
    "შენიშვნა",
]
PATROYNMIC = re.compile(r"\b(\S+)\s+ძე\s+")
TRANSLIT_OUTPUT = {"eng": ("Latin", "English")}


def extract_name(context: Context, person: Entity, name: str):
    """Parse a Georgian name slightly and transliterate."""
    patronym = None
    m = PATROYNMIC.search(name)
    if m is not None:
        patronym = m.group(1)
        name = name[: m.start()] + name[m.end() :]
        context.log.debug(f"Patroynmic: {m.group(1)}")
    h.apply_name(person, full=name, patronymic=patronym, lang="kat")
    apply_translit_full_name(context, person, "kat", name, TRANSLIT_OUTPUT)


def crawl_row(context: Context, row: Dict[str, str]):
    """Process a row of the table in the OT list."""
    name = row.pop(TARGET)
    context.log.debug(f"Adding person: {name}")
    birth_date = row.pop(DEMOGRAPHICS).split(maxsplit=1)[0]
    status = row.pop(STATUS)
    person = context.make("Person")
    person.id = context.make_id(row.pop("index"), name, status)
    context.log.debug(f"Unique ID {person.id}")
    if birth_date != UNKNOWN:
        h.apply_date(person, "birthDate", birth_date)
    person.add("topics", "sanction")
    person.add("country", "ge")
    extract_name(context, person, name)
    context.audit_data(row, UNUSED_FIELDS)
    sanction = h.make_sanction(context, person)
    sanction.set("authority", "Georgian Ministry of Justice")
    sanction.add("sourceUrl", context.data_url)
    context.emit(person)
    context.emit(sanction)


def crawl_page(context: Context, page: HtmlElement):
    """Process the HTML format of the OT list."""
    maindoc = page.get_element_by_id("maindoc")
    table = maindoc.get_element_by_id("DOCUMENT:1;ENCLOSURE:1;POINT:1;_Content")
    # It is a table within a table, but it might not always be
    t = table.find("tr/td/table")
    if t is not None:
        table = t
    rows = table.iterfind(".//tr")
    header = next(rows)
    titles = [c.text_content().strip() for c in header.iterfind("td")]
    titles[0] = "index"
    for tr in rows:
        row = dict(
            (titles[idx], c.text_content().strip())
            for idx, c in enumerate(tr.iterfind("td"))
        )
        crawl_row(context, row)


def crawl(context: Context):
    """Retrieve the text of the Otkhozoria-Tatunashvili List and
    extract entities."""
    page = context.fetch_html(context.data_url, cache_days=1)
    crawl_page(context, page)
