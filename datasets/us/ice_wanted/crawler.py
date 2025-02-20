from xml.etree import ElementTree
from normality import collapse_spaces

from zavod import Context
from zavod.shed.zyte_api import fetch_html


def get_element_text(doc: ElementTree, xpath_value: str, to_remove=[]) -> str:
    """Extract text from each child nodes of an xpath and joins them together

    Args:
        doc (ElementTree): HTML Tree
        xpath_value (str):  xpath to extract text from
        to_remove (list, optional): string to remove in the extracted text.
    """
    element_tags = doc.xpath(xpath_value)

    tag_list = []
    for tag in element_tags:
        tag_list.append(tag.text_content())

    element_text = " ".join(tag_list)

    for string in to_remove:
        element_text = element_text.replace(string, "")

    return collapse_spaces(element_text.strip())


def crawl_person(context: Context, url: str, wanted_for: str):
    name_xpath = '//div[contains(@class, "field--name-field-most-wanted-name")]//div[contains(@class, "field__item")]'
    doc = fetch_html(
        context,
        url,
        name_xpath,
        html_source="httpResponseBody",
        cache_days=1,
    )

    name = get_element_text(doc, name_xpath)
    alias = get_element_text(
        doc,
        '//div[contains(@class, "field--name-field-most-wanted-alias")]//div[contains(@class, "field__item")]',
    )

    gender = get_element_text(
        doc,
        '//div[contains(@class, "field--name-field-gender")]//div[contains(@class, "field__item")]',
    )

    place_of_birth = get_element_text(
        doc,
        '//div[contains(@class, "field--name-field-place-of-birth")]//div[contains(@class, "field__item")]',
    )
    age = get_element_text(
        doc,
        '//div[contains(@class, "field--name-field-age")]//div[contains(@class, "field__item")]',
    )
    last_known_location = get_element_text(
        doc,
        '//div[contains(@class, "field--name-field-last-known-location")]//div[contains(@class, "field__item")]',
    )
    occupation = get_element_text(
        doc,
        '//div[contains(@class, "field--name-field-most-wanted-occupation")]//div[contains(@class, "field__item")]',
    )

    summary = get_element_text(
        doc,
        './/div[contains(@class, "mw-summary-wrapper ")]//p',
    )
    mw_category = get_element_text(doc, '//div[@class="mw-catergory"]//a')
    mw_status = get_element_text(
        doc, '//div[contains(@class, "field--name-field-captured-tag")]//span'
    )
    height = get_element_text(
        doc,
        '//div[contains(@class, "field--name-field-height")]//div[contains(@class, "field__item")]',
    )
    weight = get_element_text(
        doc,
        '//div[contains(@class, "field--name-field-weight")]//div[contains(@class, "field__item")]',
    )
    skin_tone = get_element_text(
        doc,
        '//div[contains(@class, "field--name-field-skin-tone")]//div[contains(@class, "field__item")]',
    )
    eyes = get_element_text(
        doc,
        '//div[contains(@class, "field--name-field-eyes")]//div[contains(@class, "field__item")]',
    )
    hair = get_element_text(
        doc,
        '//div[contains(@class, "field--name-field-hair")]//div[contains(@class, "field__item")]',
    )
    scars_marks = get_element_text(
        doc,
        '//div[contains(@class, "field--name-field-scars-marks")]//div[contains(@class, "field__item")]',
    )
    wanted_title = get_element_text(
        doc,
        './/div[contains(@class, "field--name-field-wanted-for")]',
    )

    person_id = url.split("/")[-1]

    person = context.make("Person")
    person.id = context.make_slug(person_id)
    person.add("name", name)
    person.add("alias", alias.split("; "))
    person.add("topics", "crime")
    person.add("topics", "wanted")
    person.add("sourceUrl", url)
    person.add("summary", summary)
    person.add("position", occupation)
    person.add("gender", gender)
    person.add("birthPlace", place_of_birth)
    person.add("address", last_known_location)
    person.add("height", height)
    person.add("weight", weight)
    person.add("eyeColor", eyes)
    person.add("hairColor", hair)
    person.add("appearance", scars_marks)

    person.add(
        "description",
        [
            f"Age: {age}",
            # f"Weight: {weight}",
            # f"Height: {height}",
            f"Skin tone: {skin_tone}",
            # f"Eyes: {eyes}",
            # f"Hair: {hair}",
        ],
    )
    person.add(
        "notes",
        [
            f"Wanted For: {wanted_for}"
            f"Wanted Category: {mw_category}"
            f"Wanted Status: {mw_status}"
            f"Crime Details: {wanted_title}"
        ],
    )

    context.emit(person)


def crawl(context: Context):
    wanted_xpath = './/div[@class="mw-wantfor"]'
    doc = fetch_html(
        context,
        context.data_url,
        wanted_xpath,
        html_source="httpResponseBody",
        cache_days=1,
    )
    doc.make_links_absolute(context.data_url)

    for person_node in doc.xpath('.//li[@class="grid"]//a'):
        url = person_node.get("href")
        wanted_for = person_node.xpath(wanted_xpath)[0].text_content()
        crawl_person(context, url, wanted_for)
