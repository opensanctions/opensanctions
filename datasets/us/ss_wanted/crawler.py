from xml.etree import ElementTree
from normality import collapse_spaces

from zavod import Context, helpers as h
from zavod.shed.zyte_api import fetch_html


def get_element_text(doc: ElementTree, xpath_value: str, to_remove=[]) -> str:
    """Extract text from from an xpath

    Args:
        doc (ElementTree): HTML Tree
        xpath_value (str):  xpath to extract text from
        to_remove (list, optional): string to remove in the extracted text.
    """
    element_tags = doc.xpath(xpath_value)

    tag_list = []
    for tag in element_tags:
        try:
            tag_list.append(tag.text_content())
        except AttributeError:  #  node is already a text content
            tag_list.append(tag)
    element_text = "".join(tag_list)

    for string in to_remove:
        element_text = element_text.replace(string, "")

    return collapse_spaces(element_text.strip())


def crawl(context: Context):
    person_xpath = './/div[@id="block-af1-content"]//div[contains(@class, "grid-col")][not(contains(@class, "hidden-card"))]//a'
    doc = fetch_html(
        context,
        context.data_url,
        person_xpath,
        html_source="httpResponseBody",
        cache_days=1,
    )
    doc.make_links_absolute(context.data_url)

    for person_node in doc.xpath(person_xpath):
        url = person_node.get("href")
        crawl_person(context, url)


def crawl_person(context: Context, url: str):
    name_xpath = '//h1[contains(@class, "page-title")]'
    doc = fetch_html(
        context, url, name_xpath, html_source="httpResponseBody", cache_days=1
    )

    name = get_element_text(doc, name_xpath)

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

    citizenship = get_element_text(
        doc, ".//strong[contains(text(),'CITIZENSHIP')]/following-sibling::text()[1]"
    )
    citizenship = citizenship.split("/")

    height = get_element_text(
        doc, ".//strong[contains(text(),'HEIGHT')]/following-sibling::text()[1]"
    )
    weight = get_element_text(
        doc, ".//strong[contains(text(),'WEIGHT')]/following-sibling::text()[1]"
    )
    hair = get_element_text(
        doc, ".//strong[contains(text(),'HAIR COLOR')]/following-sibling::text()[1]"
    )
    eyes = get_element_text(
        doc, ".//strong[contains(text(),'EYE COLOR')]/following-sibling::text()[1]"
    )
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
    person.add("name", name)
    person.add("topics", "crime")
    person.add("topics", "wanted")
    person.add("sourceUrl", url)
    person.add("alias", alias.split(","))
    h.apply_date(person, "birthDate", date_of_birth)
    person.add("summary", case_summary)
    person.add("notes", f"Relevant links: {', '.join(links)}")
    person.add("nationality", nationality)
    person.add("citizenship", citizenship)
    person.add("height", height)
    person.add("weight", weight)
    person.add("hairColor", hair)
    person.add("eyeColor", eyes)

    context.emit(person)
