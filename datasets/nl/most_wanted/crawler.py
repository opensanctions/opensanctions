import itertools

from normality import slugify

from zavod import Context
from zavod import helpers as h

FUGITIVES_URL_PREFIX = "https://www.politie.nl/en/wanted/fugitives"

FIELDS = {
    "name": "name",
    "alias": "alias",
    "nicknames": "alias",
    "gender": "gender",
    "sex": "gender",
    "nationality": "nationality",
    "place_of_birth": "birthPlace",
    "other_physical_charateristics": None,
    "other_physical_characteristics": None,
    "length": "height",
    "lenght": "height",
    "height": "height",
    "other": None,
    "tattoo": "appearance",
    "build": None,
    "eye_colour": "eyeColor",
    "eye_color": "eyeColor",
    "eyes": "eyeColor",
    "skin_colour": None,
    "hair_colour": "hairColor",
    "hair": "hairColor",
    "hair_color": "hairColor",
    "haircolor": "hairColor",
    "case": None,
    "police_region": None,
    "speaks": None,
}


def crawl_person(context: Context, source_url: str) -> None:
    doc = context.fetch_html(source_url)

    facts = {}
    for fact_text in doc.xpath("//ul[@test-id='dossier-report-list']/li/text()"):
        if ": " not in fact_text:
            context.log.warn(
                f'Unparseable fact text "{fact_text}"',
                source_url=source_url,
            )
            continue
        key_text, value_text = fact_text.split(": ", 1)
        facts_key = slugify(key_text, sep="_")
        facts[facts_key] = value_text

    person = context.make("Person")
    name = doc.findtext(".//h1[@test-id='title']")

    person.id = context.make_slug(
        name,
        facts.get("place_of_birth", None),
        # Place of birth can be None, and that's fine
        strict=False,
    )
    person.add("topics", "crime")
    person.add("topics", "wanted")
    person.add("sourceUrl", source_url)

    person.add("name", name)

    intro_desc = doc.xpath("//p[contains(@class, 'p-intro')]/text()")
    other_descs = doc.xpath("//div[@test-id='html']/p/text()")

    descs = h.clean_note(intro_desc + other_descs)
    person.add("notes", "\n".join(descs))

    for field, value in facts.items():
        if field == "date_of_birth":
            h.apply_date(person, "birthDate", value.replace(" ", ""))
            continue

        if field not in FIELDS:
            context.log.warn("Unkown descriptor", field=field, value=value)
            continue

        prop = FIELDS.get(field)
        if prop is not None:
            person.add(prop, value)

    context.emit(person)


def crawl(context: Context) -> None:
    for page in itertools.count(start=1):
        doc = context.fetch_html(context.data_url, params={"page": page}, cache_days=1)
        doc.make_links_absolute(context.data_url)
        for detail_url in doc.xpath(
            "//a[contains(@class, 'wantedmissing-link')]/@href"
        ):
            # The website also contains some other search notices that we don't care about
            if detail_url.startswith(FUGITIVES_URL_PREFIX):
                crawl_person(context, detail_url)

        next_button = doc.find(".//button[@id='pagination-next-button']")
        assert next_button is not None, "Next page button not found in page"
        if "disabled" in next_button.attrib:
            break
