import re
from normality import squash_spaces
from xml.etree import ElementTree

from zavod import Context, helpers as h

ORGANIZERS_URL = "https://peps.dossier.center/types/oligarhi/"
ACCOMPLICES_URL = "https://peps.dossier.center/types/goschinovniki/"


def get_element_text(doc: ElementTree, xpath_value: str, to_remove=[], position=0):
    element_tags = doc.xpath(xpath_value)
    element_text = "".join([tag.text_content() for tag in element_tags])

    for string in to_remove:
        element_text = element_text.replace(string, "")

    return squash_spaces(element_text.strip())


def paginate_crawl(context: Context, func, page_url: str, accomplices: bool = False):
    page_number = 1
    while page_url:
        context.log.info(f"Crawling page {page_number}")

        page_url = func(context, page_url, accomplices)

        if not page_url:
            break

        page_number += 1


def crawl_persons_list(context: Context, url: str, accomplices: bool = False):
    doc = context.fetch_html(url, cache_days=1, absolute_links=True)

    for anchor in doc.xpath('//div[@class="b-archive-item"]//a'):
        anchor_url = anchor.get("href")
        crawl_person(context, anchor_url, accomplices)

    next_page = doc.xpath(
        '//a[contains(@class,"next")][contains(@class,"page-numbers")]'
    )
    next_page_url = next_page[0].get("href") if next_page else None
    return next_page_url


def crawl_person(context: Context, url: str, accomplice: bool = False):
    doc = context.fetch_html(url, cache_days=1)

    person_name = get_element_text(
        doc, '//div[@class="b-pr-section__field bottom-gap p-compact"]//p[1]'
    )
    latinised_match = re.search(r"\((.*?)\)", person_name)
    if latinised_match:
        latinised = latinised_match.group(1)
    else:
        latinised = None
    person_name = re.sub(r"\(.*?\)", "", person_name)

    position_name = get_element_text(
        doc, '//div[@class="b-pr-section__field bottom-gap p-compact"]//p[2]'
    )

    birth_date_n_palce = get_element_text(
        doc,
        '//div[@class="b-pr-section__label"][contains(.//text(), "Дата и место рождения")]//following-sibling::div[@class="b-pr-section__value"]',
    )
    date_pattern = r"\d{1,2}[./]\d{1,2}[./]\d{4}"

    date_of_birth = re.findall(date_pattern, birth_date_n_palce)
    date_of_birth = date_of_birth[0] if date_of_birth else None

    place_of_birth = re.sub(date_pattern, "", birth_date_n_palce)
    place_of_birth = place_of_birth.strip(", ")

    citizenships = get_element_text(
        doc,
        '//div[@class="b-pr-section__label"][contains(.//text(), "Гражданство")]//following-sibling::div[@class="b-pr-section__value"]',
    )
    citizenships = citizenships.split(",")

    reason_on_list = doc.xpath(
        '//div[contains(@class,"b-pr-section")][contains(.//*//text(), "Почему")]/*'
    )
    reason_on_list = " | ".join(
        [squash_spaces(tag.text_content()) for tag in reason_on_list]
    )

    possible_violation = doc.xpath(
        '//div[contains(@class,"b-pr-section")][contains(.//*//text(), "Возможные")]/*'
    )
    possible_violation = " | ".join(
        [squash_spaces(tag.text_content()) for tag in possible_violation]
    )

    person = context.make("Person")
    person.id = context.make_slug(person_name)
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


def crawl(context: Context):
    paginate_crawl(context, crawl_persons_list, ORGANIZERS_URL)
    paginate_crawl(context, crawl_persons_list, ACCOMPLICES_URL, accomplices=True)
