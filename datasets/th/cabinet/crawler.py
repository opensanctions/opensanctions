from lxml.html import HtmlElement

from zavod.stateful.positions import categorise
from zavod.extract.zyte_api import fetch_html
from zavod import Context, helpers as h


NAME_XPATH = ".//div[contains(@class, '__title')]"
ROLE_XPATH = ".//p[contains(@class, 'fw-normal')]"


def crawl_prime_minister(
    context: Context, main_container: HtmlElement
) -> tuple[str, str] | None:
    name = h.xpath_string(main_container, ".//div[contains(@class, 'fs-3')]/text()")
    role = h.xpath_string(main_container, ".//p[contains(@class, 'fw-medium')]/text()")
    if not name or not role:
        context.log.warning("Could not extract PM name/role")
        return None
    print(name, role)
    return name, role


def crawl_person(context: Context, name: str, role: str) -> None:
    person = context.make("Person")
    person.id = context.make_id(name, role)
    person.add("name", name, lang="eng")
    person.add("topics", "role.pep")
    person.add("citizenship", "th")

    position = h.make_position(
        context,
        name=role,
        country="th",
        lang="eng",
        topics=["gov.executive", "gov.national"],
    )
    categorisation = categorise(context, position, is_pep=True)
    if not categorisation.is_pep:
        return

    occupancy = h.make_occupancy(context, person, position)
    if occupancy:
        context.emit(person)
        context.emit(position)
        context.emit(occupancy)


def crawl(context: Context):
    page = 1
    while True:
        url = f"{context.data_url}?page={page}"
        doc = fetch_html(context, url, NAME_XPATH, cache_days=1)
        main_container = h.xpath_element(
            doc, ".//div[@class='container']//div[@class='row']"
        )
        # crawl PM only once on the first page
        if page == 1:
            name, role = crawl_prime_minister(context, main_container)
            if name and role:
                crawl_person(context, name, role)

        persons = h.xpath_elements(main_container, ".//div[@class='col']")
        if page > 7 or not persons:
            break

        for person in persons:
            name = h.xpath_string(person, ".//div[contains(@class, '__title')]/text()")
            role = h.xpath_string(person, ".//p[contains(@class, 'fw-normal')]/text()")
            if not name or not role:
                context.log.warning("Name or role not found")
                continue
            crawl_person(context, name, role)

        page += 1
