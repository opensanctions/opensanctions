from urllib.parse import urljoin
from lxml.html import HtmlElement

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise
from zavod.extract import zyte_api

# "Wybrany dnia:" or "Wybrana dnia:" means "Elected on:"
START_DATE_XPATH = ".//ul[@class='data']/li[p[@class='left']='Wybrany dnia:' or p[@class='left']='Wybrana dnia:']/p[@class='right']/text()"


def split_dob_pob(dob_pob: str) -> tuple[str, str]:
    # Split on the first comma: '12-09-1960, Bytom'
    parts = dob_pob.split(",", 1)
    dob = parts[0].strip()
    pob = parts[1].strip() if len(parts) > 1 else ""
    return dob, pob


def extract_party_name(context, doc: HtmlElement, label_id: str) -> str | None:
    el = doc.xpath(f".//p[@id='{label_id}']/following-sibling::p[@class='right']")
    if el is not None and len(el) == 1:
        return el[0].text_content().strip()
    else:
        context.log.warning("Missing party affiliation.", doc=doc)
        return None


def crawl_person(context: Context, url: str) -> None:
    name_xpath = ".//div[@id='title_content']/h1/text()"
    pep_doc = zyte_api.fetch_html(
        context, url, name_xpath, html_source="httpResponseBody", cache_days=1
    )
    name = h.xpath_string(pep_doc, name_xpath)
    start_date = h.xpath_string(pep_doc, START_DATE_XPATH)
    dob_pob = h.xpath_string(pep_doc, ".//p[@id='urodzony']/text()")
    dob, pob = split_dob_pob(dob_pob)

    entity = context.make("Person")
    entity.id = context.make_id(name, dob, pob)
    entity.add("name", name)
    entity.add("citizenship", "pl")
    entity.add("topics", "role.pep")
    entity.add("sourceUrl", url)
    entity.add("birthPlace", pob)
    # 'lblLista' is the party during the elections; 'lblKlub' is the cyrrent party
    entity.add("political", extract_party_name(context, pep_doc, "lblLista"))
    entity.add("political", extract_party_name(context, pep_doc, "lblKlub"))
    h.apply_date(entity, "birthDate", dob)

    position = h.make_position(
        context,
        name="Member of the Sejm",
        wikidata_id="Q19269361",
        country="pl",
        topics=["gov.legislative", "gov.national"],
        lang="eng",
    )
    categorisation = categorise(context, position, is_pep=True)
    if not categorisation.is_pep:
        return

    occupancy = h.make_occupancy(
        context,
        person=entity,
        position=position,
        start_date=start_date,
        categorisation=categorisation,
    )
    if occupancy is not None:
        context.emit(occupancy)
        context.emit(position)
        context.emit(entity)


def crawl(context: Context) -> None:
    deputies_xpath = ".//ul[@class='deputies']/li"
    doc = zyte_api.fetch_html(
        context,
        context.data_url,
        unblock_validator=deputies_xpath,
        html_source="httpResponseBody",
        cache_days=1,
    )
    deputies = doc.findall(deputies_xpath)
    for deputy in deputies:
        if (link := deputy.find(".//a")) is None:
            continue
        href = link.get("href")
        assert href is not None, "Missing href"
        url = urljoin(context.data_url, href)
        crawl_person(context, url)
