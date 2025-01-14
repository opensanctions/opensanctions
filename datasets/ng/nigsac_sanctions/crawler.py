from lxml import html
from typing import Dict
from rigour.mime.types import HTML
from normality import collapse_spaces, slugify

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity


def format_birth_place(city, country):
    if city and country:
        return f"{city}, {country}"
    if city:
        return city
    if country:
        return country
    return None


def parse_page(context: Context, url) -> Dict[str, str]:
    doc = context.fetch_html(url, cache_days=1)
    data = dict()
    for dt in doc.findall(".//dt"):
        key = slugify(dt.text_content())
        next = dt.getnext()
        if next is None:
            value = None
        elif next.tag == "dt":
            value = None
        elif next.tag == "dd":
            value = collapse_spaces(next.text_content())
        else:
            context.log.warning("Unexpected tag after key", key=key, tag=next, url=url)
            value = None
        data[key] = value
    return data


def crawl_common(context: Context, url: str, entity: Entity, sanction: Entity, data):
    entity.add("sourceUrl", url)
    entity.add("topics", "sanction")
    entity.add("notes", data.pop("narrative-summary"))
    h.apply_date(sanction, "startDate", data.pop("sanction-date"))
    sanction.add("reason", data.pop("reason-for-designation"))
    h.apply_date(sanction, "listingDate", data.pop("record-date"))
    sanction.add("description", data.pop("press-release"))

    context.emit(entity)
    context.emit(sanction)
    context.audit_data(data)


def crawl_individual(context: Context, url: str, data: Dict[str, str]):
    first_name = data.pop("first-name")
    middle_name = data.pop("middlename")
    last_name = data.pop("surname")

    full_name = h.make_name(
        first_name=first_name, middle_name=middle_name, last_name=last_name
    )
    res = context.lookup("entities_as_individuals", full_name)
    if res is not None:
        for key, value in res.props.items():
            data[key] = value
        data["incorporation-number"] = None
        data["incorporation-date"] = None
        data["referance-number"] = None
        data.pop("nationality")
        crawl_entity(context, url, data)
        return

    entity = context.make("Person")
    birth_place = format_birth_place(data.pop("birth-city"), data.pop("birth-country"))
    birth_date = h.extract_date(context.dataset, data.pop("date-of-birth"))
    entity.id = context.make_id(
        first_name, middle_name, last_name, birth_place, birth_date
    )
    h.apply_name(
        entity, first_name=first_name, middle_name=middle_name, last_name=last_name
    )
    entity.add("alias", data.pop("aliases"))
    entity.add("birthDate", birth_date)
    entity.add("birthPlace", birth_place)
    entity.add("nationality", data.pop("nationality"))
    entity.add("address", data.pop("address"))
    entity.add("notes", data.pop("comments"))
    phone = data.pop("phone-number")
    phone = phone.replace("- ", "") if phone else phone
    entity.add("phone", phone)

    sanction = h.make_sanction(context, entity)
    crawl_common(context, url, entity, sanction, data)


def crawl_entity(context: Context, url: str, data: Dict[str, str]):
    entity = context.make("LegalEntity")
    name = data.pop("entity-name")
    entity.id = context.make_id(name)
    entity.add("name", name)
    entity.add("alias", data.pop("aliases", None))
    h.apply_date(entity, "incorporationDate", data.pop("incorporation-date"))
    entity.add("registrationNumber", data.pop("incorporation-number"))
    entity.add("country", data.pop("country", None))

    sanction_ref = data.pop("referance-number")
    sanction = h.make_sanction(context, entity, key=sanction_ref)
    sanction.add("authorityId", sanction_ref)
    crawl_common(context, url, entity, sanction, data)


def crawl(context: Context):
    path = context.fetch_resource("source.html", context.data_url)
    context.export_resource(path, HTML, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        doc = html.fromstring(fh.read())
    doc.make_links_absolute(context.data_url)

    individual_tables = doc.xpath(
        ".//h6[text() = 'Individual']/following-sibling::table[1]"
    )
    assert len(individual_tables) == 1, individual_tables
    for row in individual_tables[0].xpath(".//tr"):
        if row.find(".//th") is not None:
            continue
        url = row.xpath(".//a[text() = 'Details']/@href")[0]
        data = parse_page(context, url)
        crawl_individual(context, url, data)

    entity_tables = doc.xpath(".//h6[text() = 'Entity']/following-sibling::table[1]")
    assert len(entity_tables) == 1, entity_tables
    for row in entity_tables[0].xpath(".//tr"):
        if row.find(".//th") is not None:
            continue
        url = row.xpath(".//a[text() = 'Details']/@href")[0]
        data = parse_page(context, url)
        crawl_entity(context, url, data)
