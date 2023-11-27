import csv
from normality import collapse_spaces
from pantomime.types import CSV
from typing import Dict
import re
from lxml import etree

from zavod import Context
from zavod import helpers as h
from zavod.util import ElementOrTree


CLEAN_TITLE_RE = re.compile(r"((Cert. Hon.|KCMG|MBE|KC|JP|MP|MBE|NP)(, )?)+")
IGNORE_HEADINGS = {
    "About",
    "Members",
    "Business",
    "Hansard",
    "CPA",
    "PMC",
    "Media",
    "Official Term Photo",
}
KEEP_HEADINGS = {
    "Speaker": "Speaker",
    "Premier": "Premier of the Cayman Islands",
    "Cabinet Ministers": "Cabinet Minister",
    "Ex-Officio Members": "Ex-Officio Member of parliament",
    "Government Members": "Government Member of parliament",
    "Opposition Members": "Opposition Member of parliament",
}


def crawl_card(context: Context, position: str, el: ElementOrTree):
    name_el = el.find('.//p[@class="elementor-image-box-title"]')
    name = name_el.text_content()
    name = re.sub(r",.+", "", name)
    name = name.replace("Hon. ", "")
    name = name.replace("Mr. ", "")
    
    entity = context.make("Person")
    entity.id = context.make_id(name)
    entity.add("name", name)

    position_el = name_el.getparent().find('.//p[@class="elementor-image-box-description"]')
    if position_el is not None:
        position_detail = collapse_spaces(position_el.text_content())
        position_detail = CLEAN_TITLE_RE.sub("", position_detail)
        entity.add("position", position_detail)

    position = h.make_position(
        context,
        position,
        topics=["gov.national"],
        country="ky",
    )
    occupancy = h.make_occupancy(
        context,
        entity,
        position,
        start_date="2021",
        end_date="2025",
    )
    context.emit(entity, target=True)
    context.emit(position)
    context.emit(occupancy)

    

def crawl_row(context: Context, row: Dict[str, str]):
    # https://archives.gov.ky/sites/legislativeassembly/portal/page/portal/lglhome/members/elected-2021-2025.html
    inception_date = "2021"
    dissolution_date = "2025"

    entity = context.make("Person")
    name = row.pop("Name")
    entity.id = context.make_id(name)
    entity.add("name", name)
    entity.add("title", row.pop("Title"))

    position = h.make_position(
        context,
        row.pop("Position"),
        topics=["gov.national"],
        country="ky",
    )
    occupancy = h.make_occupancy(
        context,
        entity,
        position,
        start_date=inception_date,
        end_date=dissolution_date,
    )
    context.emit(entity, target=True)
    context.emit(position)
    context.emit(occupancy)


def crawl(context: Context):
    doc = context.fetch_html(context.data_url, cache_days=1)
    assert "2021-2025 Members" in doc.text_content()

    heading = None
    for section in doc.findall(".//section"):
        heading_el = section.xpath(".//h2[contains(@class, 'elementor-heading-title')]")
        if heading_el:
            heading = collapse_spaces(heading_el[0].text_content()).strip()
            if heading in IGNORE_HEADINGS:
                heading = None
                continue
            if heading in KEEP_HEADINGS:
                heading = KEEP_HEADINGS[heading]
            else:
                context.log.warn("unknown heading", heading=heading)
                heading = None
        else:
            if heading is None:
                continue
            for el in section.xpath(".//div[contains(@class, 'elementor-element-populated')]"):
                if el.find('.//p[@class="elementor-image-box-title"]') is not None:
                    crawl_card(context, heading, el)


    #path = context.fetch_resource("source.csv", context.data_url)
    #context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    #with open(path, "r") as fh:
    #    for row in csv.DictReader(fh):
    #        crawl_row(context, row)