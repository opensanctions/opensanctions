from urllib.parse import urljoin

from zavod import Context, Entity, settings
from zavod import helpers as h
from zavod.shed.zyte_api import fetch_html

LEGISLATURES = {
    "https://parlament.mt/en/14th-leg/political-groups/": ("2022", "2027"),
    "https://parlament.mt/en/13th-leg/political-groups/": ("2017", "2022"),
}


def crawl_person(
    context: Context,
    position: Entity,
    url: str,
    name: str,
    start: str,
    end: str,
) -> None:
    doc = fetch_html(context, url, ".//h2", cache_days=30)
    person = context.make("Person")
    person.id = context.make_id(url)
    context.log.info(f"Crawling person: {name} ({url})")
    if " - " in name:
        name, role = name.rsplit(" - ")
        person.add("position", role.strip())
    if ", " in name:
        last, first = name.split(", ", 1)
        person.add("firstName", first.strip())
        person.add("lastName", last.strip())
    person.add("name", name.strip())
    person.add("alias", doc.findtext(".//h2"))
    person.add("citizenship", "mt")
    person.add("topics", "role.pep")
    person.add("sourceUrl", url)
    person.add("political", doc.findtext(".//h3/a"))

    panel = doc.find('.//div[@class="panel"]')
    if panel is not None:
        for cell in panel.findall(".//td/a"):
            if cell.text is not None and "@parlament.mt" in cell.text:
                person.add("email", cell.text)

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        start_date=start,
        end_date=end,
        propagate_country=False,
    )
    if occupancy is not None:
        context.emit(occupancy)

    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        "Member of the House of Representatives of Malta",
        wikidata_id="Q19367406",
        topics=["gov.legislative", "gov.national"],
        country="mt",
    )
    context.emit(position)

    for url, (start, end) in LEGISLATURES.items():
        validator = ".//*[contains(text(), 'Parliamentary Groups')]"
        doc = fetch_html(context, url, validator, cache_days=5)
        # doc = context.fetch_html(url)
        for link in doc.findall('.//div[@class="rowdata row"]//a'):
            href = link.get("href")
            if not href:
                continue
            person_url = urljoin(url, href)
            if not person_url.startswith(url):
                context.log.warning(f"Skipping unexpected URL: {person_url}")
                continue
            assert link.text is not None, "Expected link text"
            crawl_person(context, position, person_url, link.text, start, end)

    if settings.RUN_TIME.year >= 2027:
        context.log.warning("New Maltese legislature? Please update the crawler.")
