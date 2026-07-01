import re

from zavod import Context, helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import categorise
from zavod.util import Element

# Canary for a new State Duma convocation: the listing page redirects to the
# current term automatically, so this text confirms the parsing below still
# applies to the term actually being crawled.
TERM_MARKER = "eighth convocation"
LISTING_ITEM_XPATH = './/li[@class="list-persons__item"]'
PERSON_ID_RE = re.compile(r"/persons/(\d+)/")


def extract_birth_date(doc: Element) -> str | None:
    for para in h.xpath_elements(doc, './/div[@id="person-about"]//div[@class="text"]/p'):
        text = h.element_text(para)
        if text.startswith("Дата рождения:"):
            return text.removeprefix("Дата рождения:").strip()
    # Prominent deputies (e.g. chamber leadership) carry a "Биография" section
    # instead, with the birth date as the first entry's date term.
    bio_sections = h.xpath_elements(
        doc,
        './/div[@id="person-about"]/section[h2[contains(normalize-space(.), "Биография")]]',
    )
    if not bio_sections:
        return None
    dates = h.xpath_elements(bio_sections[0], ".//dl/dt")
    descriptions = h.xpath_elements(bio_sections[0], ".//dl/dd")
    for date_el, description_el in zip(dates, descriptions):
        if "Родил" in h.element_text(description_el):
            return h.element_text(date_el)
    return None


def extract_took_office_date(doc: Element) -> str | None:
    for para in h.xpath_elements(doc, './/div[@id="person-about"]//div[@class="text"]/p'):
        text = h.element_text(para)
        if text.startswith("Дата вступления в полномочия:"):
            return text.removeprefix("Дата вступления в полномочия:").strip()
    return None


def extract_description_fields(doc: Element) -> dict[str, str]:
    grids = h.xpath_elements(doc, './/div[@class="person__description__grid"]')
    if not grids:
        return {}
    labels = h.xpath_strings(
        grids[0],
        './/div[contains(@class, "person__description__col--title")]//span/text()',
    )
    values = h.xpath_elements(grids[0], './/div[@class="person__description__col"]')
    fields = {}
    for label, value_el in zip(labels, values):
        fields[label.rstrip(":").strip()] = h.element_text(value_el)
    return fields


def crawl_person(
    context: Context,
    position: Entity,
    person_id: str,
    surname: str,
    given_patronymic: str,
) -> None:
    about_url = f"http://duma.gov.ru/duma/persons/{person_id}/"
    doc = context.fetch_html(about_url, cache_days=7)

    person = context.make("Person")
    person.id = context.make_slug(person_id)

    if " " in given_patronymic:
        given, _, patronymic = given_patronymic.rpartition(" ")
    else:
        given, patronymic = given_patronymic, None
    h.apply_name(
        person, first_name=given, patronymic=patronymic, last_name=surname, lang="eng"
    )
    title_els = h.xpath_elements(
        doc, './/h1[@class="article__title article__title--person"]'
    )
    if title_els:
        # The surname and given/patronymic name are separated by a <br/>, with
        # no whitespace between the text nodes, so join them explicitly.
        name_parts = h.xpath_strings(title_els[0], ".//text()")
        cyrillic_name = " ".join(part.strip() for part in name_parts if part.strip())
        if cyrillic_name:
            h.apply_name(person, full=cyrillic_name, lang="rus")

    h.apply_date(person, "birthDate", extract_birth_date(doc))

    # Russian citizenship is required for a State Duma seat, and holding foreign
    # citizenship or a foreign residence permit disqualifies a candidate, per
    # Article 97(1) of the Constitution of the Russian Federation:
    # https://constitutionrf.ru/rzd-1/gl-5/st-97-krf
    person.add("citizenship", "ru")
    person.add("sourceUrl", about_url)

    # IMPORTANT: all person props must be set before make_occupancy
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        start_date=extract_took_office_date(doc),
        categorisation=categorisation,
    )
    if occupancy is None:
        return

    fields = extract_description_fields(doc)
    if "Фракция" in fields:
        occupancy.add("politicalGroup", fields["Фракция"])
    if "Регион" in fields:
        occupancy.add("constituency", fields["Регион"])

    context.emit(position)
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, cache_days=1)
    if TERM_MARKER not in h.element_text(doc):
        raise RuntimeError(
            f"Could not find {TERM_MARKER!r} on the deputies listing page. The "
            "State Duma may have started a new convocation — verify the page "
            "structure still matches, then update the term marker and the "
            "assertions in the dataset metadata."
        )

    # Q17276321 — member of the State Duma, the lower house of the Federal
    # Assembly of Russia.
    position = h.make_position(
        context,
        name="Member of the State Duma",
        country="ru",
        wikidata_id="Q17276321",
        topics=["gov.national", "gov.legislative"],
    )

    for item in h.xpath_elements(doc, LISTING_ITEM_XPATH):
        hrefs = h.xpath_strings(item, './/a[@itemprop="url"]/@href')
        if not hrefs:
            raise ValueError("No profile link found for a deputy list item")
        match = PERSON_ID_RE.search(hrefs[0])
        if match is None:
            raise ValueError(f"Could not extract a person ID from href: {hrefs[0]}")
        surname = h.xpath_string(item, './/span[@itemprop="name"]/strong/text()')
        given_patronymic = h.xpath_string(
            item, './/span[@itemprop="name"]/span[@class="second-name"]/text()'
        )
        crawl_person(context, position, match.group(1), surname, given_patronymic)
