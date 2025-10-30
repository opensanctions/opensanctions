from typing import cast
from urllib.parse import urljoin

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise

# "Wybrany dnia:" or "Wybrana dnia:" means "Elected on:"
START_DATE_XPATH = ".//ul[@class='data']/li[p[@class='left']='Wybrany dnia:' or p[@class='left']='Wybrana dnia:']/p[@class='right']/text()"


def split_dob_pob(dob_pob: str) -> tuple[str, str]:
    # Split on the first comma: '12-09-1960, Bytom'
    parts = dob_pob.split(",", 1)
    dob = parts[0].strip()
    pob = parts[1].strip() if len(parts) > 1 else ""
    return dob, pob


def crawl_person(context: Context, url: str) -> None:
    pep_doc = context.fetch_html(url, cache_days=1)
    name = pep_doc.findtext(".//div[@id='title_content']/h1")
    if not name:
        context.log.warning(f"Missing name for: {url}")
        return
    start_date = cast(str, pep_doc.xpath(START_DATE_XPATH)[0])
    if not start_date:
        context.log.warning(f"Missing start date for: {url}")
        return
    dob_pob = pep_doc.findtext(".//p[@id='urodzony']")
    assert dob_pob is not None, f"Missing date and place of birth for: {url}"
    dob, pob = split_dob_pob(dob_pob)

    entity = context.make("Person")
    entity.id = context.make_id(name, dob, pob)
    entity.add("name", name)
    entity.add("citizenship", "pl")
    entity.add("topics", "role.pep")
    entity.add("sourceUrl", url)
    entity.add("birthPlace", pob)
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
    doc = context.fetch_html(context.data_url, cache_days=1)
    deputies = doc.findall(".//ul[@class='deputies']/li")
    for deputy in deputies:
        if (link := deputy.find(".//a")) is None:
            continue
        href = link.get("href")
        assert href is not None, "Missing href"
        url = urljoin(context.data_url, href)
        crawl_person(context, url)
