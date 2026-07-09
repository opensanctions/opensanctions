import re


from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

# Listing names sometimes run together without spaces (e.g. "давлаталӣСаид"); insert a
# space at each lower→upper case transition, then capitalise the first letter.
CASE_SPLIT = re.compile(r"(?<=[а-яёҷҳӣқғӯ])(?=[А-ЯЁҶҲӢҚҒӮ])")
# Detail pages give the birth date in a "Санаи/Соли таваллуд:" field, usually as
# dd.mm.yyyy but sometimes as Tajik text (year extracted as a fallback). Leadership
# role pages instead state the year in prose ("соли YYYY … таваллуд шудааст").
BIRTH_FIELD = re.compile(r"(?:Санаи|Соли) таваллуд:\s*([^*\n]+)")
NUM_DATE = re.compile(r"(\d{1,2})\.(\d{1,2})\.(\d{4})")
YEAR = re.compile(r"\d{4}")
BIRTH_YEAR_PROSE = re.compile(r"соли\s+(\d{4})[^.]{0,40}таваллуд")
BIRTH_PLACE = re.compile(r"Ҷойи таваллуд:\s*([^*\n]+)")


def extract_birth_date(text: str) -> str | None:
    field = BIRTH_FIELD.search(text)
    if field is not None:
        num = NUM_DATE.search(field.group(1))
        if num is not None:
            day, month, year = num.groups()
            return f"{year}-{int(month):02d}-{int(day):02d}"
        year = YEAR.search(field.group(1))
        if year is not None:
            return year.group(0)
    prose = BIRTH_YEAR_PROSE.search(text)
    return prose.group(1) if prose is not None else None


def clean_name(raw: str) -> str:
    name = CASE_SPLIT.sub(" ", " ".join(raw.split())).strip()
    return name[:1].upper() + name[1:]


def crawl_member(
    context: Context,
    name: str,
    url: str,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    person = context.make("Person")
    # Leadership cards link to a role page (one holder each), members to a personal
    # page; the final path segment is a stable per-person key either way.
    person.id = context.make_slug(url.rstrip("/").split("/")[-1])
    person.add("name", name, lang="tgk")

    text = h.element_text(context.fetch_html(url, cache_days=7))
    h.apply_date(person, "birthDate", extract_birth_date(text))
    place_match = BIRTH_PLACE.search(text)
    if place_match is not None:
        person.add("birthPlace", place_match.group(1).strip(), lang="tgk")
    # Members must hold only Tajik citizenship (Constitution art. 49).
    # https://www.constituteproject.org/constitution/Tajikistan_2016
    person.add("citizenship", "tj")
    person.add("sourceUrl", url)

    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Majlisi Milli of Tajikistan",
        country="tj",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q21296003",
        lang="eng",
    )
    categorisation = categorise(context, position)
    context.emit(position)

    doc = context.fetch_html(context.data_url, cache_days=1)
    seen: set[str] = set()
    for box in h.xpath_elements(
        doc, "//div[contains(@class, 'elementor-image-box-content')]"
    ):
        titles = h.xpath_elements(
            box, ".//*[contains(@class, 'elementor-image-box-title')]"
        )
        links = h.xpath_elements(box, ".//a[@href]")
        if not titles or not links:
            continue
        url = links[0].get("href")
        # Skip the page-title card (no member link) and dedupe the chairman, who is
        # listed both as a header and as a member.
        if url is None or url in seen or context.data_url.rstrip("/") in url:
            continue
        seen.add(url)
        crawl_member(
            context,
            clean_name(h.element_text(titles[0])),
            url,
            position,
            categorisation,
        )
