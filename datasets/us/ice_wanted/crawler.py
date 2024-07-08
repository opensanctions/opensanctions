from zavod import Context
from xml.etree import ElementTree
from normality import collapse_spaces


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

    element_text = "".join(tag_list)

    for string in to_remove:
        element_text = element_text.replace(string, "")

    return collapse_spaces(element_text.strip())


def generate_mw_notes(**kwargs):
    note_string = f"""Additional Information:
- Wanted For: {kwargs.get('crime_summary')}
- Wanted Heading: {kwargs.get('crime_details')}
- Wanted Category: {kwargs.get("category")}
- Wanted Status: {kwargs.get("status")}
- Age: {kwargs.get("age")}
- Height: {kwargs.get("height")}
- Weight: {kwargs.get("weight")}
- Skin tone: {kwargs.get("skin_tone")}
- Eyes: {kwargs.get("eyes")}
- Hair: {kwargs.get("hair")}
- Last Known Location: {kwargs.get('last_known_location')}"""

    return note_string


def crawl_person(context: Context, url: str, crime_summary: str):
    doc = context.fetch_html(url, cache_days=1)

    names = get_element_text(
        doc,
        '//div[contains(@class, "field--name-field-most-wanted-name")]//div[contains(@class, "field__item")]',
    )
    names_split = names.split(",")

    name = names_split[0]
    alias = names_split[1:]

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
    crime_header = get_element_text(
        doc,
        './/div[contains(@class, "field--name-field-wanted-for")]',
    )

    person_id = url.split("/")[-1]

    person = context.make("Person")
    person.id = context.make_slug(person_id)
    person.add("name", name)
    person.add("alias", alias)
    person.add("topics", "crime")
    person.add("sourceUrl", url)
    person.add("summary", summary)
    person.add("position", occupation)
    person.add("gender", gender)
    person.add("birthPlace", place_of_birth)
    person.add(
        "notes",
        generate_mw_notes(
            category=mw_category,
            status=mw_status,
            age=age,
            height=height,
            weight=weight,
            skin_tone=skin_tone,
            eyes=eyes,
            hair=hair,
            last_known_location=last_known_location,
            crime_summary=crime_summary,
            crime_details=crime_header,
        ),
    )

    context.emit(person, target=True)


def crawl(context: Context):
    doc = context.fetch_html(context.data_url, cache_days=1)
    doc.make_links_absolute(context.data_url)

    for person_node in doc.xpath('.//li[@class="grid"]//a'):
        url = person_node.get("href")
        crime_summary = person_node.xpath('.//div[@class="mw-wantfor"]')[
            0
        ].text_content()
        print(crime_summary)
        print(url)
        crawl_person(context, url, crime_summary)
