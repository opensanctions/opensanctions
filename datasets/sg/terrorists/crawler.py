import re
from csv import DictReader

from lxml import html
from rigour.mime.types import HTML
from zavod.shed import zyte_api

from zavod import Context
from zavod import helpers as h

IN_BRACKETS = re.compile(r"\(([^\)]*)\)")
PROGRAM_KEY = "SG-TSFA2002"
DOB = "Date of Birth:"
PASSPORT = "Passport No."
ADDITIONAL_LISTS_PAGE_URL = "https://www.mas.gov.sg/regulation/anti-money-laundering/targeted-financial-sanctions"
ADDITIONAL_LISTS_DATA_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRb11upZ07FLqPyMrglwkgBFfnBUaRgzmSS6m4l7jKRzvsEcYfikz7tdZb-NmeA-1Eh4p1-Ls2-lc-D/pub?gid=0&single=true&output=csv"


def crawl_terrorism_act(context: Context):
    _, _, _, html_source = zyte_api.fetch_text(context, context.data_url)
    doc = html.fromstring(html_source)

    html_resource_path = context.get_resource_path("source.html")
    html_resource_path.write_text(html_source)
    context.export_resource(html_resource_path, HTML, title=context.SOURCE_TITLE)

    for node in doc.findall(".//td[@class='tailSTxt']"):
        if not node.text_content().startswith("2."):
            continue
        for item in node.findall(".//tr"):
            number = item.find(".//td[@class='sProvP1No']").text_content()
            text = item.find(".//td[@class='sProvP1']").text_content()
            if text.startswith("[Deleted"):
                continue
            text = text.strip().rstrip(";").rstrip(".")
            name, _ = text.split("(", 1)
            names = h.multi_split(name, ["s/o", "@"])

            entity = context.make("Person")
            entity.id = context.make_slug(number, name)
            entity.add("name", names)
            entity.add("topics", "sanction")

            sanction = h.make_sanction(
                context,
                entity,
                program_key=PROGRAM_KEY,
            )

            for match in IN_BRACKETS.findall(text):
                # match = match.replace("\xa0", "")
                res = context.lookup("props", match)
                if res is not None:
                    for prop, value in res.props.items():
                        entity.add(prop, value)
                    continue
                if match.endswith("citizen"):
                    cit = match.replace("citizen", "")
                    entity.add("citizenship", cit)
                    continue
                if match.startswith(DOB):
                    dob = match.replace(DOB, "").strip()
                    h.apply_date(entity, "birthDate", dob)
                    continue
                if match.startswith(PASSPORT):
                    passport = match.replace(PASSPORT, "").strip()
                    entity.add("passportNumber", passport)
                    continue
                context.log.warn("Unparsed bracket term", term=match)

            context.emit(entity)
            context.emit(sanction)


def crawl_additional_lists(context: Context):
    # Check if they've added any amendments
    validator = ".//*[contains(text(), 'Targeted Financial Sanctions')]"
    doc = zyte_api.fetch_html(context, ADDITIONAL_LISTS_PAGE_URL, validator)
    container = doc.xpath(".//main")
    assert len(container) == 1, len(container)
    h.assert_dom_hash(
        node=container[0],
        hash="7d0d3d29e74cc7e9e64f30e736cf2a3ab096c079",
        raise_exc=False,
        text_only=True,
    )
    path = context.fetch_resource("source.csv", ADDITIONAL_LISTS_DATA_URL)
    with open(path, "r") as f:
        for row in DictReader(f):
            name = row.pop("name")
            country = row.pop("country")
            entity = context.make(row.pop("schema"))
            entity.id = context.make_id(name, country)
            entity.add("name", name)
            entity.add("country", country)

            sanction = h.make_sanction(
                context, entity, program_key=row.pop("program_key")
            )
            h.apply_date(sanction, "startDate", row.pop("start_date"))
            h.apply_date(sanction, "endDate", row.pop("end_date"))
            sanction.add("sourceUrl", row.pop("source_url"))
            if h.is_active(sanction):
                entity.add("topics", "sanction")

            context.emit(entity)
            context.emit(sanction)
            context.audit_data(row)


def crawl(context: Context):
    crawl_terrorism_act(context)
    crawl_additional_lists(context)
