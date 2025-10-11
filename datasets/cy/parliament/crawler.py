from urllib.parse import urljoin

from zavod import Context, Entity
from zavod import helpers as h
from zavod.shed.zyte_api import fetch_html


def crawl_person(context: Context, position: Entity, url: str) -> None:
    doc = fetch_html(context, url, ".//div[@id='fullpage']", cache_days=7)
    person = context.make("Person")
    person.id = context.make_id(url)
    article = doc.find('.//section[@id="cv_article"]')
    if article is None:
        article = doc.find('.//section[@id="generic_article"]')
    assert article is not None, ("Article section not found", url)
    name = article.findtext(".//h1")
    context.log.info(f"Crawling person: {name} ({url})")
    person.add("name", name)
    person.add("citizenship", "cy")
    person.add("sourceUrl", url)
    person.add("topics", "role.pep")

    content = article.find('.//div[@class="contentdiv"]')
    assert content is not None, "Content div not found"
    for idx, para in enumerate(content.findall(".//p")):
        if idx == 0:
            notes = h.element_text(para.find(".//strong"))
            person.add("notes", notes)
        strong_text = h.element_text(para.find(".//strong"))
        value_text = h.element_text(para).replace(strong_text, "")
        if "date of birth" in strong_text.lower():
            pob, dob = value_text.rsplit(", ", 1)
            h.apply_date(person, "birthDate", dob)
            person.add("birthPlace", pob)
        elif "studies" in strong_text.lower():
            person.add("education", value_text)
        elif "Foreign languages" in strong_text.lower():
            person.add("spokenLanguages", value_text)
        # else:
        #     print("strong_text:", strong_text)

    occupancy = h.make_occupancy(context, person, position, no_end_implies_current=True)
    if occupancy is not None:
        context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of Parliament",
        country="cy",
        topics=["gov.legislative", "gov.national"],
    )
    context.emit(position)

    doc = fetch_html(context, context.data_url, ".//div[@id='fullpage']")
    for row in doc.findall('.//div[@class="greybox"]//a'):
        if not row.get("href"):
            continue
        url = urljoin(context.data_url, row.get("href"))
        crawl_person(context, position, url)
