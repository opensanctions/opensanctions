import re

from zavod.context import Context
from zavod import helpers as h
from zavod.helpers.xml import ElementOrTree


COUNTRY_REGEX = re.compile(r"\((.*?)\)")


def parse_html(doc: ElementOrTree):
    content = doc.find('.//div[@class="content"]')
    if content is None:
        raise ValueError("Cannot find content div")
    for element in content.findall(".//ul/li/strong"):
        nro_element = element.getparent()
        if nro_element is None:
            continue

        country_match = COUNTRY_REGEX.search(nro_element.text_content())
        if country_match is None:
            continue
        country_str = country_match.group(1).strip()

        aliases_el = nro_element.find(".//ul")
        if aliases_el is None:
            aliases_str = ""
        else:
            aliases_str = (
                aliases_el.text_content().replace("Known alias(es):", "").strip()
            )

        # FIX: it turns out that the listed aliases are in fact often subsidiaries
        # so we're going to emit them separately from the main entity.
        name = nro_element.find("strong").text
        aliases = aliases_str.split(";")
        weak_aliases = []
        for alias in aliases:
            if alias.isupper() or len(alias) < 5:
                weak_aliases.append(alias)
            else:
                yield {
                    "name": alias,
                    "country": country_str,
                    "notes": f"see: {name}",
                }

        yield {
            "name": name,
            "country": country_str,
            "weak_aliases": weak_aliases,
        }


def emit_nro(context: Context, nro):
    entity = context.make("Organization")
    entity.id = context.make_id("nro", nro["name"], nro["country"])
    entity.add("name", nro["name"])
    entity.add("country", nro["country"])
    entity.add("notes", nro.get("notes"))
    entity.add("topics", "export.control")

    for alias in nro.get("weak_aliases", []):
        entity.add("weakAlias", alias)

    context.emit(entity, target=True)

    sanction = h.make_sanction(context, entity)
    sanction.add(
        "program", "Policy on Sensitive Technology Research and Affiliations of Concern"
    )
    context.emit(sanction)


def crawl(context: Context):
    doc = context.fetch_html(context.data_url, cache_days=1)
    for nro in parse_html(doc):
        emit_nro(context, nro)
