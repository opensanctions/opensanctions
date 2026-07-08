import re
from typing import Callable

from zavod import Context, helpers as h
from zavod.util import Element

ORGANIZERS_URL = "https://peps.dossier.center/types/oligarhi/"
ACCOMPLICES_URL = "https://peps.dossier.center/types/goschinovniki/"


def get_element_text(doc: Element, xpath_value: str, to_remove: list[str] = []) -> str:
    els = h.xpath_elements(doc, xpath_value)
    text = "".join(h.element_text(el) for el in els)
    for string in to_remove:
        text = text.replace(string, "")
    return text


def paginate_crawl(
    context: Context,
    func: Callable[[Context, str, bool], str | None],
    page_url: str,
    accomplices: bool = False,
) -> None:
    current_url: str | None = page_url
    page_number = 1
    while current_url:
        context.log.info(f"Crawling page {page_number}")
        current_url = func(context, current_url, accomplices)
        page_number += 1


def crawl_persons_list(
    context: Context, url: str, accomplices: bool = False
) -> str | None:
    doc = context.fetch_html(url, cache_days=1, absolute_links=True, encoding="utf-8")

    for anchor_url in h.xpath_strings(doc, '//div[@class="b-archive-item"]//a/@href'):
        crawl_person(context, anchor_url, accomplices)

    next_page = h.xpath_elements(
        doc, '//a[contains(@class,"next")][contains(@class,"page-numbers")]'
    )
    return next_page[0].get("href") if next_page else None


def crawl_person(context: Context, url: str, accomplice: bool = False) -> None:
    doc = context.fetch_html(url, cache_days=1, encoding="utf-8")

    person_name = get_element_text(
        doc, '//div[@class="b-pr-section__field bottom-gap p-compact"]//p[1]'
    )
    latinised_match = re.search(r"\((.*?)\)", person_name)
    latinised: str | None = latinised_match.group(1) if latinised_match else None
    person_name = re.sub(r"\(.*?\)", "", person_name)

    position_name = get_element_text(
        doc, '//div[@class="b-pr-section__field bottom-gap p-compact"]//p[2]'
    )

    birth_date_and_place = get_element_text(
        doc,
        '//div[@class="b-pr-section__label"][contains(.//text(), "Дата и место рождения")]//following-sibling::div[@class="b-pr-section__value"]',
    )
    date_pattern = r"\d{1,2}[./]\d{1,2}[./]\d{4}"

    date_of_birth_matches = re.findall(date_pattern, birth_date_and_place)
    date_of_birth: str | None = (
        date_of_birth_matches[0] if date_of_birth_matches else None
    )

    place_of_birth = re.sub(date_pattern, "", birth_date_and_place)
    place_of_birth = place_of_birth.strip(", ")

    citizenships = get_element_text(
        doc,
        '//div[@class="b-pr-section__label"][contains(.//text(), "Гражданство")]//following-sibling::div[@class="b-pr-section__value"]',
    ).split(",")

    reason_on_list = " | ".join(
        h.element_text(tag) or ""
        for tag in h.xpath_elements(
            doc,
            '//div[contains(@class,"b-pr-section")][contains(.//*//text(), "Почему")]/*',
        )
    )

    possible_violation = " | ".join(
        h.element_text(tag) or ""
        for tag in h.xpath_elements(
            doc,
            '//div[contains(@class,"b-pr-section")][contains(.//*//text(), "Возможные")]/*',
        )
    )

    person = context.make("Person")
    person.id = context.make_id(url)
    person.add("name", person_name, lang="rus")
    person.add("name", latinised)
    person.add("sourceUrl", url)
    person.add("topics", "poi")

    if date_of_birth:
        h.apply_date(person, "birthDate", date_of_birth)
    person.add("birthPlace", place_of_birth)
    person.add("citizenship", citizenships)
    person.add("notes", [reason_on_list, possible_violation])
    person.add("summary", "Probable Accomplice" if accomplice else "Probable Organizer")
    person.add("position", position_name)

    context.emit(person)


def crawl(context: Context) -> None:
    paginate_crawl(context, crawl_persons_list, ORGANIZERS_URL)
    paginate_crawl(context, crawl_persons_list, ACCOMPLICES_URL, accomplices=True)
