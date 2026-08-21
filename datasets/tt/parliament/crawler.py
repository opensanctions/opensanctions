import re
from dataclasses import dataclass
from urllib.parse import urlencode

from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.util import Element

from zavod import Context
from zavod import helpers as h

# The parliament selector labels each term like "12th Republican Parliament
# (28 Aug 2020 - 18 Mar 2025)"; the sitting term is labelled "Current - ...".
TERM_RE = re.compile(r"\((\d{1,2} \w+ \d{4}) - (\d{1,2} \w+ \d{4})\)")


@dataclass
class Post:
    """A position together with its PEP categorisation, so both travel as a unit."""

    position: Entity
    categorisation: PositionCategorisation


def make_post(context: Context, name: str, wikidata_id: str | None = None) -> Post:
    position = h.make_position(
        context,
        name=name,
        country="tt",
        topics=["gov.national", "gov.legislative"],
        wikidata_id=wikidata_id,
        lang="eng",
    )
    return Post(position, categorise(context, position))


def parse_term(label: str) -> tuple[str | None, str | None]:
    """Extract the term's raw start and end date strings from a selector label.

    Returns the source date strings as-is (parsing is left to the date helpers),
    or `(None, None)` for the sitting parliament, which the source labels
    "Current" and leaves undated.
    """
    if label.startswith("Current"):
        return None, None
    match = TERM_RE.search(label)
    if match is None:
        raise ValueError(f"Cannot parse parliament term from label: {label!r}")
    return match.group(1), match.group(2)


def crawl_member(
    context: Context,
    row: Element,
    member: Post,
    period: tuple[str | None, str | None],
    speaker: Post | None = None,
) -> None:
    profile_url = h.xpath_string(row, "./@href")
    slug = profile_url.rstrip("/").rsplit("/", 1)[-1]

    raw_name = h.xpath_string(row, './/div[contains(@class, "member_name")]/text()')
    name = h.strip_name_titles(context, raw_name)
    assert name is not None

    person = context.make("Person")
    person.id = context.make_id(slug)
    person.add("name", name, original_value=raw_name if name != raw_name else None)

    constituencies = h.xpath_strings(
        row, './/div[contains(@class, "member_constituency")]/text()'
    )
    constituency = constituencies[0] if constituencies else None

    is_speaker = constituency == "Speaker of the House"
    if is_speaker:
        if speaker is None:
            raise ValueError(f"Unexpected Speaker row outside the House: {profile_url}")
        post = speaker
    else:
        post = member

    if not post.categorisation.is_pep:
        return

    parties = h.xpath_strings(
        row, './/div[starts-with(@class, "member_affiliation")]/text()'
    )
    if parties:
        party_res = context.lookup("party", parties[0])
        person.add("political", party_res.value if party_res else parties[0])
    # Constitution of the Republic of Trinidad and Tobago, ss. 41 (Senate) and 47
    # (House of Representatives), both requiring Trinidad and Tobago citizenship:
    # https://www.constituteproject.org/constitution/Trinidad_and_Tobago_2007
    person.add("citizenship", "tt")
    person.add("sourceUrl", profile_url)

    period_start, period_end = period
    occupancy = h.make_occupancy(
        context,
        person,
        post.position,
        period_start=period_start,
        period_end=period_end,
        categorisation=post.categorisation,
    )
    if occupancy is None:
        return
    if not is_speaker and constituency is not None:
        occupancy.add("constituency", constituency)
    context.emit(post.position)
    context.emit(occupancy)
    context.emit(person)


def crawl_chamber(
    context: Context,
    pid: str,
    house: str,
    member: Post,
    period: tuple[str | None, str | None],
    speaker: Post | None = None,
) -> None:
    query = urlencode(
        {"members_search": "1", "keywords": "", "pid": pid, "house": house}
    )
    url: str | None = f"{context.data_url}?{query}"
    seen: set[str] = set()
    while url is not None:
        if url in seen:
            raise ValueError(f"Pagination loop detected at {url}")
        seen.add(url)
        doc = context.fetch_html(url, cache_days=1)
        rows = h.xpath_elements(doc, './/div[contains(@class, "sf_result_list")]/a')
        for row in rows:
            crawl_member(context, row, member, period, speaker)
        # The last page has no rel="next" link, which terminates the loop.
        next_urls = h.xpath_strings(doc, './/a[@rel="next"]/@href')
        url = next_urls[0] if next_urls else None


def crawl(context: Context) -> None:
    house = make_post(
        context,
        "Member of the House of Representatives of Trinidad and Tobago",
        "Q18719159",
    )
    senate = make_post(
        context, "Member of the Senate of Trinidad and Tobago", "Q19319420"
    )
    # No Wikidata QID: the closest candidate, Q109538297, is `instance of` (P31)
    # "public office" rather than "position", and carries no `applies to
    # jurisdiction` (P1001) statement, so it fails the check for supplying a QID.
    speaker = make_post(
        context, "Speaker of the House of Representatives of Trinidad and Tobago"
    )

    doc = context.fetch_html(context.data_url, cache_days=1)
    for option in h.xpath_elements(doc, '//select[@name="pid"]/option'):
        pid = h.xpath_string(option, "./@value")
        if pid == "All":
            continue
        period = parse_term(h.xpath_string(option, "./text()"))
        period_end = period[1]
        if period_end is not None:
            end = h.extract_date(
                context.dataset, period_end, fallback_to_original=False
            )[0]
            if end < h.earliest_term_start(house.position.get("topics")):
                context.log.info(
                    "Skipping parliament term before the PEP-relevance window",
                    pid=pid,
                    period_end=end,
                )
                continue
        crawl_chamber(context, pid, "House of Representatives", house, period, speaker)
        crawl_chamber(context, pid, "Senate", senate, period)
