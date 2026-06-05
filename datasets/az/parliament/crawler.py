import re
from urllib.parse import urljoin

from zavod import Context
from zavod import helpers as h
from zavod.constants import ORIGIN_INFERRED
from zavod.stateful.positions import categorise
from zavod.util import Element

# The Milli Majlis has 125 single-mandate seats. The listing occasionally drops to ~124
# when a seat is vacant. If the count leaves this band, the site structure or the
# convocation (pinned via cat=51 in data.url) has likely changed and the crawler should
# be revisited rather than silently emit a stale or truncated roster.
MIN_MEMBERS = 100
MAX_MEMBERS = 130

# Azerbaijani month names -> month number, for dates like "24 may 1967-ci il".
AZ_MONTHS = {
    "yanvar": 1,
    "fevral": 2,
    "mart": 3,
    "aprel": 4,
    "may": 5,
    "iyun": 6,
    "iyul": 7,
    "avqust": 8,
    "sentyabr": 9,
    "oktyabr": 10,
    "noyabr": 11,
    "dekabr": 12,
}
DATE_RE = re.compile(r"(\d{1,2})\s+([a-zçğıişöü]+)\s+(\d{4})", re.IGNORECASE)

# Patronymic suffix -> inferred gender. Azerbaijani full names end in a patronymic
# "<father> oğlu" (son of) or "<father> qızı" (daughter of).
GENDERS = {"oğlu": "male", "qızı": "female"}

# Bio table (table.dep_stil) labels.
BIRTH_DATE = "Doğulduğu tarixi"
BIRTH_PLACE = "Doğulduğu yer"
EDU_INSTITUTION = "Bitirdiyi təhsil müəssisəsinin adı"
SPECIALTY = "İxtisası"
ELECTED_DATE = "Deputat seçildiyi tarix"
# Bio fields we deliberately do not emit (no clean FTM target).
BIO_IGNORE = [
    "Təhsili",  # education level ("Ali" = higher)
    "Elmi dərəcəsi və elmi adı",  # academic degree / title
    "Bildiyi xarici dillər",  # foreign languages
    "Ailə vəziyyəti",  # family status
]

# Affiliation block (div.deputat-da / div.deputat-kom) labels.
PARTY = "Partiya mənsubiyyəti:"
PHONE = "Telefon:"
EMAIL = "E-mail:"


def parse_az_date(value: str) -> str | None:
    """Convert an Azerbaijani long-form date to ISO, or None if it doesn't match.

    Source values look like "24 may 1967-ci il"; the "-ci il" ordinal/year suffix is
    discarded once the day, month name and year have been pulled out.
    """
    match = DATE_RE.search(value)
    if match is None:
        return None
    day, month_name, year = match.groups()
    month = AZ_MONTHS.get(month_name.lower())
    if month is None:
        return None
    return f"{int(year):04d}-{month:02d}-{int(day):02d}"


def parse_bio_table(table: Element) -> dict[str, list[str]]:
    """Parse the label/value bio table into {label: [value lines]}.

    Multi-line cells (e.g. several universities) are split into separate values.
    """
    bio: dict[str, list[str]] = {}
    for row in h.xpath_elements(table, "./tbody/tr | ./tr"):
        cells = row.findall("./td")
        if len(cells) != 2:
            continue
        label = h.element_text(cells[0])
        lines = [s for s in (str(t).strip() for t in cells[1].itertext()) if s]
        if label and lines:
            bio[label] = lines
    return bio


def crawl_member(context: Context, url: str, name: str) -> None:
    doc = context.fetch_html(url, cache_days=6)

    person = context.make("Person")
    # The numeric profile id is an opaque source identifier (no PII).
    member_id = url.split("id=")[1].split("&")[0]
    person.id = context.make_slug(member_id)
    person.add("name", name)
    person.add("sourceUrl", url)
    # National parliament: deputies must be citizens of Azerbaijan per the Constitution
    # of the Republic of Azerbaijan, Article 85(I).
    # https://president.az/en/pages/view/azerbaijan/constitution
    person.add("citizenship", "az")

    # Infer gender from the patronymic suffix; record the suffix as the original value.
    suffix = name.strip().split()[-1].lower()
    gender = GENDERS.get(suffix)
    if gender is not None:
        person.add("gender", gender, origin=ORIGIN_INFERRED, original_value=suffix)

    tables = h.xpath_elements(doc, ".//table[contains(@class, 'dep_stil')]")
    bio: dict[str, list[str]] = {}
    if len(tables) > 0:
        bio = parse_bio_table(tables[0])

    birth_date = bio.pop(BIRTH_DATE, [])
    if len(birth_date) > 0:
        h.apply_date(person, "birthDate", parse_az_date(birth_date[0]))
    for place in bio.pop(BIRTH_PLACE, []):
        person.add("birthPlace", place, lang="aze")
    for institution in bio.pop(EDU_INSTITUTION, []):
        person.add("education", institution, lang="aze")
    for specialty in bio.pop(SPECIALTY, []):
        person.add("education", specialty, lang="aze")
    elected = bio.pop(ELECTED_DATE, [])
    start_date = parse_az_date(elected[0]) if len(elected) > 0 else None

    context.audit_data(bio, ignore=BIO_IGNORE)

    affiliations = h.xpath_elements(
        doc, ".//div[contains(@class, 'deputat-da') or contains(@class, 'deputat-kom')]"
    )
    for div in affiliations:
        strong = div.find("strong")
        if strong is None:
            continue
        label = h.element_text(strong)
        value = (strong.tail or "").strip()
        if not value:
            continue
        if label == PARTY:
            person.add("political", value, lang="aze")
        elif label == PHONE:
            person.add("phone", value)
        elif label == EMAIL:
            person.add("email", value)

    position = h.make_position(
        context,
        name="Member of the National Assembly of Azerbaijan",
        country="az",
        wikidata_id="Q21269547",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        no_end_implies_current=True,
        start_date=start_date,
        categorisation=categorisation,
    )
    if occupancy is None:
        return

    context.emit(person)
    context.emit(position)
    context.emit(occupancy)


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, absolute_links=True, cache_days=6)
    anchors = h.xpath_elements(doc, ".//a[contains(@href, 'news-dep.php')]")

    members: dict[str, str] = {}
    for anchor in anchors:
        href = anchor.get("href")
        if href is None:
            continue
        url = urljoin(context.data_url, href)
        name_el = h.xpath_elements(anchor, ".//div[contains(@class, 'author-name')]")
        if len(name_el) == 0:
            continue
        name = h.element_text(name_el[0])
        if name:
            members[url] = name

    count = len(members)
    if not (MIN_MEMBERS <= count <= MAX_MEMBERS):
        context.log.error(
            "Unexpected number of parliament members; site or term may have changed",
            count=count,
        )
        raise ValueError(f"Expected {MIN_MEMBERS}-{MAX_MEMBERS} members, found {count}")

    for url, name in members.items():
        crawl_member(context, url, name)
