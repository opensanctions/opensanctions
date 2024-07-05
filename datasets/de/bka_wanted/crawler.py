from zavod import Context
from zavod import helpers as h
from xml.etree import ElementTree
from normality import collapse_spaces


def get_element_text(
    doc: ElementTree, xpath_value: str, to_remove=[], join_str=""
) -> str:
    """Extract text from from an xpath

    Args:
        doc (ElementTree): HTML Tree
        xpath_value (str):  xpath to extract text from
        to_remove (list, optional): string to remove in the extracted text.
        join_str (str): String to and join the extracted text
    """
    element_tags = doc.xpath(xpath_value)

    tag_list = []
    for tag in element_tags:
        try:
            tag_list.append(tag.text_content())
        except AttributeError:  #  node is already a text content
            tag_list.append(tag)

    element_text = join_str.join(tag_list)

    for string in to_remove:
        element_text = element_text.replace(string, "")

    return collapse_spaces(element_text.strip())


def crawl(context: Context):
    base_url = context.data_url
    additional_url = f"{base_url}?additionalHitsOnly=true"

    for page_url in [base_url, additional_url]:
        doc = context.fetch_html(page_url, cache_days=1)
        doc.make_links_absolute("https://www.bka.de")

        for person_node in doc.xpath('.//li[@class="js-dynamiclist-element"]//a'):
            url = person_node.get("href")
            crawl_person(context, url)


def crawl_person(context: Context, url: str):
    doc = context.fetch_html(url, cache_days=1)

    name = get_element_text(doc, '//div[contains(@class, "headerContent")]//h1')
    last_name = get_element_text(
        doc,
        '//li[@class="c-listitem-detailinfos"][.//span[contains(.//text(),"Familienname")]]//span[contains(@class,"c-value-detailinfos")]',
    )
    first_name = get_element_text(
        doc,
        '//li[@class="c-listitem-detailinfos"][.//span[contains(.//text(),"Vorname")]]//span[contains(@class,"c-value-detailinfos")]',
    )

    summary = get_element_text(doc, '//div[@class="sachverhalt"]//p', join_str="\n")
    more_details = get_element_text(
        doc, '//div[@class="c-futherinfo-wrapper"]//p', join_str="\n"
    )

    alias = get_element_text(
        doc,
        './/li[@class="c-listitem-detailinfos"][.//span[contains(.//text(),"Alias")]]//span[contains(@class,"c-value-detailinfos")]',
    )
    alias = alias.split(",")

    date_of_birth = get_element_text(
        doc,
        '//li[@class="c-listitem-detailinfos"][.//span[contains(.//text(),"Geburtsdatum")]]//span[contains(@class,"c-value-detailinfos")]',
    )
    date_of_birth = h.parse_date(date_of_birth, ["%d.%m.%Y"])
    place_of_birth = get_element_text(
        doc,
        '//li[@class="c-listitem-detailinfos"][.//span[contains(.//text(),"Geburtsort")]]//span[contains(@class,"c-value-detailinfos")]',
    )
    nationality = get_element_text(
        doc,
        '//li[@class="c-listitem-detailinfos"][.//span[contains(.//text(),"Staatsangehörigkeit")]]//span[contains(@class,"c-value-detailinfos")]',
    )
    nationality = nationality.split(",")

    gender = get_element_text(
        doc,
        '//li[@class="c-listitem-detailinfos"][.//span[contains(.//text(),"Geschlecht")]]//span[contains(@class,"c-value-detailinfos")]',
    )

    offense = get_element_text(
        doc,
        '//li[@class="c-listitem-detailinfos"][.//span[contains(.//text(),"Delikt")]]//span[contains(@class,"c-value-detailinfos")]',
    )
    offense_time = get_element_text(
        doc,
        '//li[@class="c-listitem-detailinfos"][.//span[contains(.//text(),"Zeit")]]//span[contains(@class,"c-value-detailinfos")]',
    )
    crime_scene = get_element_text(
        doc,
        '//span[text()[contains(.,"Tatort")]]//following-sibling::span',
    )

    notes = [f"Delikt: {offense}", f"Zeit: {offense_time}", f"Tatort:{crime_scene}"]
    notes = "\n".join(notes)

    person = context.make("Person")
    person.id = context.make_slug(f"{name}-{offense_time}")
    person.add("name", name)
    person.add("firstName", first_name)
    person.add("secondName", last_name)
    person.add("topics", "crime")
    person.add("sourceUrl", url)
    person.add("alias", alias)
    person.add("gender", gender)
    person.add("birthPlace", place_of_birth)
    person.add("birthDate", date_of_birth)
    person.add("summary", summary)
    person.add("description", more_details)
    person.add("nationality", nationality)
    person.add("notes", notes)

    context.emit(person, target=True)
