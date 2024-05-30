from zavod import Context
from zavod import helpers as h
from xml.etree import ElementTree
from normality import collapse_spaces


def get_element_text(doc: ElementTree, xpath_value: str, to_remove=[], position=0):
    element_tags = doc.xpath(xpath_value)

    tag_list = []
    for tag in element_tags:
        try:
            tag_list.append(tag.text_content())
        except Exception:
            tag_list.append(tag)
    element_text = "".join(tag_list)

    for string in to_remove:
        element_text = element_text.replace(string, "")

    return collapse_spaces(element_text.strip())


def crawl(context: Context):
    base_url = context.data_url

    doc = context.fetch_html(base_url, cache_days=1)
    doc.make_links_absolute(base_url)

    for person_link in doc.xpath(
        './/div[@id="block-af1-content"]//div[contains(@class, "grid-col")][contains(@class, "margin")]//a'
    ):

        url = person_link.get("href")
        crawl_person(context, url)


def crawl_person(context: Context, url: str):
    doc = context.fetch_html(url, cache_days=1)

    alias = get_element_text(
        doc,
        ".//strong[contains(text(),'ALIASES:')]/following-sibling::text()[1]",
        to_remove=["“", "”"],
    )

    date_of_birth = get_element_text(
        doc, ".//strong[contains(text(),'DOB')]/following-sibling::text()[1]"
    )
    nationality = get_element_text(
        doc, ".//strong[contains(text(),'NATION')]/following-sibling::text()[1]"
    )
    nationality = nationality.split("/")
    case_summary = get_element_text(
        doc, '//h2[contains(text(), "CASE")]//following-sibling::p'
    )

    links = [
        tag.get("href")
        for tag in doc.xpath(
            './/h2[contains(text(), "CASE")]//following-sibling::ul//a'
        )
    ]

    person_id = url.split("/")[-1]

    person = context.make("Person")
    person.id = context.make_slug(person_id)
    person.add("topics", "crime")
    person.add("sourceUrl", url)
    person.add("alias", alias.split(","))
    person.add("birthDate", h.parse_date(date_of_birth, ["%b %d, %Y", "%B %d, %Y"]))
    person.add("summary", case_summary)
    person.add("notes", f"Revelank links: {', '.join(links)}")
    person.add("nationality", nationality)

    context.emit(person, target=True)
