from lxml import etree
from normality import slugify, collapse_spaces
from pantomime.types import HTML

from zavod import Context
from zavod import helpers as h
from zavod.shed.zyte_api import fetch_html


FORMATS = ["%d-%b-%Y"]


def parse_date(text):
    if text is None:
        return None
    text = text.replace("Sept", "Sep")
    return h.parse_date(text, FORMATS)


def unblock_validator(doc: etree._Element) -> bool:
    return doc.find(".//table[@id='datatable-1']") is not None


def crawl(context: Context):
    doc = fetch_html(
        context, context.data_url, unblock_validator, html_source="httpResponseBody"
    )

    table = doc.find('.//table[@id="datatable-1"]')
    headers = None
    for row in table.findall(".//tr"):
        if headers is None:
            headers = [slugify(c.text, "_") for c in row.findall("./th")]
            continue
        cells = [collapse_spaces(c.text) for c in row.findall("./td")]
        cells = dict(zip(headers, cells))

        # AfDB lists several individuals as firms in places where the IADB
        # shows them to be people (and they have normal personal names)

        # type_ = cells.pop("type")
        # schema = context.lookup_value("types", type_)
        # if schema is None:
        #     context.log.error("Unknown entity type", type=type_)
        #     continue
        name = cells.pop("name")
        country = cells.pop("nationality")
        entity = context.make("LegalEntity")
        entity.id = context.make_id(name, country)
        entity.add("name", name)
        entity.add("topics", "debarment")
        entity.add("country", country)

        sanction = h.make_sanction(context, entity)
        sanction.add("reason", cells.pop("basis"))
        sanction.add("startDate", parse_date(cells.pop("from")))
        sanction.add("endDate", parse_date(cells.pop("to")))

        context.emit(entity, target=True)
        context.emit(sanction)
