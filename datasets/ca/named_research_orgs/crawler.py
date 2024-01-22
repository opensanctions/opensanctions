from lxml import html
import re

from zavod.context import Context
from zavod import helpers as h


COUNTRY_REGEX = re.compile(r"\((.*?)\)")


def parse_html(doc):
    for element in doc.find('.//div[@class="content"]').findall(".//ul/li/strong"):
        nro_element = element.getparent()

        country_str = COUNTRY_REGEX.search(nro_element.text_content()).group(1).strip()

        aliases_el = nro_element.find(".//ul")
        if aliases_el is None:
            aliases_str = ""
        else:
            aliases_str = (
                aliases_el.text_content().replace("Known alias(es):", "").strip()
            )
        aliases = [x.strip() for x in aliases_str.split(";") if not x.isupper()]
        weak_aliases = [x for x in aliases_str.split(";") if x.isupper()]

        yield {
            "name": nro_element.find("strong").text,
            "country": country_str,
            "aliases": aliases,
            "weak_aliases": weak_aliases,
        }


def emit_nro(context, nro):
    entity = context.make("Organization")
    entity.id = context.make_id("nro", nro["name"], nro["country"])
    entity.add("name", nro["name"])
    entity.add("alias", nro["name"])
    entity.add("country", nro["country"])
    entity.add("topics", "export.control")
    for alias in nro["aliases"]:
        entity.add("alias", alias)

    for alias in nro["weak_aliases"]:
        entity.add("weakAlias", alias)

    context.emit(entity, target=True)

    sanction = h.make_sanction(context, entity)
    sanction.add(
        "program", "Policy on Sensitive Technology Research and Affiliations of Concern"
    )
    context.emit(sanction)


def crawl(context: Context):
    doc = context.fetch_html(context.dataset.data.url, cache_days=1)
    for nro in parse_html(doc):
        emit_nro(context, nro)
