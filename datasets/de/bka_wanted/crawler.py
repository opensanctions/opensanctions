from typing import List, cast

from lxml import html
from lxml.etree import _Element
from normality import collapse_spaces

from zavod import Context
from zavod import helpers as h


def get_element_text(doc: _Element, xpath_value: str, join_str: str = " ") -> List[str]:
    """Extract text from from an xpath

    Args:
        doc (ElementTree): HTML Tree
        xpath_value (str):  xpath to extract text from
        join_str (str): String to and join the extracted text
    """
    tag_list = []
    for tag in doc.xpath(xpath_value):
        try:
            content = tag.text_content()
            content = content.replace("\xad", "").strip()
            if len(content):
                tag_list.append(content)
        except AttributeError:  #  node is already a text content
            tag_list.append(tag)
    return tag_list


def info_xpath(field: str) -> str:
    return f'//li[@class="c-listitem-detailinfos"][.//span[contains(.//text(),"{field}")]]//span[contains(@class,"c-value-detailinfos")]'


def crawl(context: Context) -> None:
    additional_url = f"{context.data_url}?additionalHitsOnly=true"

    # The <base> HTML element specifies the base URL to use for all relative URLs in a document.
    # Why would you do that? I don't know. But they do.
    doc = context.fetch_html(context.data_url, cache_days=1, absolute_links=False)
    base_el = doc.find(".//head/base")
    assert base_el is not None, "<base> URL not found"
    base_url = base_el.get("href")

    for page_url in [context.data_url, additional_url]:
        doc = context.fetch_html(page_url, cache_days=1, absolute_links=False)
        # If this assert trips, they probably got rid of the madness again and we can just
        # set fetch_html's absolute_links to True (and get rid of the base_url logic above)
        cast(html.HtmlElement, doc).make_links_absolute(base_url)

        for person_node in doc.findall('.//li[@class="js-dynamiclist-element"]//a'):
            url = person_node.get("href")
            assert url is not None, "Person node in overview has no href"
            crawl_person(context, url)


def crawl_person(context: Context, url: str) -> None:
    doc = context.fetch_html(url, cache_days=1)

    names = doc.xpath('.//div[contains(@class, "headerContent")]//h1//text()')
    name = collapse_spaces(" ".join(names).replace("\xad", ""))
    if name is None or "Unbekannte Person" in name:
        return

    offense = ", ".join(get_element_text(doc, info_xpath("Delikt")))
    person = context.make("Person")
    person.id = context.make_id(name, offense)
    person.add("name", name)
    person.add("topics", "crime")
    person.add("topics", "wanted")
    person.add("sourceUrl", url)
    person.add("notes", f"Delikt: {offense}")

    first_name = get_element_text(doc, info_xpath("Vorname"))
    person.add("firstName", first_name)

    last_name = get_element_text(doc, info_xpath("Familienname"))
    person.add("lastName", last_name)

    summary = "\n".join(get_element_text(doc, '//div[@class="sachverhalt"]//p'))
    person.add("notes", summary)

    more_details_ = get_element_text(doc, '//div[@class="c-futherinfo-wrapper"]//p')
    more_details = "\n".join(more_details_)
    person.add("notes", more_details)

    for aliases in get_element_text(doc, info_xpath("Alias")):
        for alias in aliases.split(","):
            prop = "alias" if " " in alias else "weakAlias"
            person.add(prop, alias)

    for dob in get_element_text(doc, info_xpath("Geburtsdatum")):
        h.apply_date(person, "birthDate", dob)

    for pob in get_element_text(doc, info_xpath("Geburtsort")):
        person.add("birthPlace", pob)

    person.add("citizenship", get_element_text(doc, info_xpath("Staatsangehörigkeit")))
    person.add("gender", get_element_text(doc, info_xpath("Geschlecht")))

    for offense_time in get_element_text(doc, info_xpath("Zeit")):
        person.add("notes", f"Zeit: {offense_time}")

    place_xpath = '//span[text()[contains(.,"Tatort")]]//following-sibling::span'
    for place in get_element_text(doc, place_xpath):
        person.add("notes", f"Tatort: {place}")

    context.emit(person)
