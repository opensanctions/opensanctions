import re
from collections import Counter
from datetime import timedelta

from lxml.etree import _Element

from zavod import Context, Entity, settings
from zavod import helpers as h
from zavod.extract.zyte_api import fetch_html
from zavod.stateful.positions import PositionCategorisation, categorise

# Dates are written as "12 июни соли 1988", "26-уми сентябри соли 1974",
# "14 апрели 1981" or "12.03.1988".
DATE = (
    r"(?P<date>(?P<day>\d{1,2})\s*-?\s*(?:[ую]ми?)?\s*-?\s*"
    r"(?P<month>(?:январ|феврал|март|апрел|май|июн|июл|август|сентябр|октябр|ноябр|декабр)и)"
    r"\s+(?:соли\s+)?(?P<year>\d{4})|(?P<numeric>\d{2}\.\d{2}\.\d{4}))"
)
# "Соли таваллуд: <date>", also spelled "таввалуд", or prose ending in
# "<date> дар ноҳияи … таваллуд шудааст".
BIRTH_DATE_LABEL = re.compile(r"тавв?алл?уд\s*[:\-–]*\s*" + DATE)
BIRTH_DATE_PROSE = re.compile(DATE + r"[^.]{0,150}?тавв?алл?уд")
BIRTH_PLACE = re.compile(
    r"Ҷой?и\s*[-–]?\s*тавв?алл?уд\s*[:\-–]*\s*(.+?)\s*[.,*]?\s*"
    r"(?:Миллат|Маълумот|Таҳсилот|, дар оилаи)"
)
# "2 марти соли 2025 вакили Маҷлиси намояндагони … интихоб шудааст"
ELECTION = re.compile(DATE + r"\s*[Вв]акил")
# "ИДИЗОДА ФАЙЗАЛӢ ФУЗАЙЛШО Аз 19 марти соли 2025 дар ҷаласаи якуми …"
LEADER = re.compile(
    r"(?P<name>[А-ЯЁӢҶҲҚҒӮ]{2,}(?:\s+[А-ЯЁӢҶҲҚҒӮ]{2,})+)\s+[Аа]з\s+"
    + DATE
    + r"\s+дар ҷаласаи"
)
# A five-year term, plus a year for the next roster to be published.
STALE_AFTER = timedelta(days=6 * 365)


def page_text(el: _Element) -> str:
    # Names and labels are split across inline tags (<strong>ФАЙЗАЛ</strong>
    # <strong>Ӣ</strong>), while paragraphs follow each other without whitespace.
    # Break only after block elements so that paragraphs don't run together.
    for block in el.iter("p", "div", "br", "td", "li", "h1"):
        block.tail = "\n" + (block.tail or "")
    return h.element_text(el)


def match_date(match: re.Match[str] | None) -> str | None:
    if match is None:
        return None
    if match.group("numeric") is not None:
        return match.group("numeric")
    return f"{match.group('day')} {match.group('month')} {match.group('year')}"


def apply_biography(person: Entity, text: str) -> None:
    birth = BIRTH_DATE_LABEL.search(text) or BIRTH_DATE_PROSE.search(text)
    if birth is not None:
        h.apply_date(
            person, "birthDate", match_date(birth), original_value=birth.group("date")
        )
    place = BIRTH_PLACE.search(text)
    if place is not None:
        person.add("birthPlace", place.group(1), lang="tgk")
    # Deputies must hold only the citizenship of Tajikistan: Constitution of
    # Tajikistan (2016), Art. 49. https://www.constituteproject.org/constitution/Tajikistan_2016
    person.add("citizenship", "tj")


def crawl_member(
    context: Context,
    url: str,
    member: Entity,
    member_categorisation: PositionCategorisation,
) -> str | None:
    """Emit the member and return the election date their page states, if any."""
    doc = fetch_html(
        context,
        url,
        unblock_validator="//div[contains(@class, 'wrap-deputat-page')]",
        html_source="httpResponseBody",
        cache_days=7,
    )
    page = h.xpath_element(doc, "//div[contains(@class, 'wrap-deputat-page')]")
    header = h.xpath_element(page, "./ul[@class='mini-info-deputat']/li[2]")
    name = h.element_text(h.xpath_element(header, "./span"))
    if name == "":
        context.log.warning("Member page has no name", url=url)
        return None
    election = match_date(ELECTION.search(h.element_text(header)))

    deputat_id = url.rsplit("/", 1)[-1]
    person = context.make("Person")
    person.id = context.make_slug(deputat_id)
    person.add("name", name, lang="tgk")
    # The English member pages often leave the name empty.
    en_doc = fetch_html(
        context,
        f"https://www.parlament.tj/en/deputats/{deputat_id}",
        unblock_validator="//div[contains(@class, 'wrap-deputat-page')]",
        html_source="httpResponseBody",
        cache_days=7,
    )
    en_name = h.element_text(
        h.xpath_element(en_doc, "//ul[@class='mini-info-deputat']/li[2]/span")
    )
    if en_name != name:
        person.add("name", en_name, lang="eng")
    person.add("sourceUrl", url)
    apply_biography(person, page_text(page))

    occupancy = h.make_occupancy(
        context,
        person,
        member,
        election_date=election,
        categorisation=member_categorisation,
    )
    if occupancy is None:
        return election
    context.emit(occupancy)
    context.emit(person)
    return election


def check_sitting_term(context: Context, elections: list[str]) -> str:
    """Return the sitting convocation's election date, as most members' pages state
    it. Warn when the pages disagree on it, or when that election is over a term old.
    A few pages keep the line from an earlier convocation, e.g. /deputats/24."""
    assert len(elections) > 0, "No member page states an election date"
    counts: Counter[str] = Counter()
    for election in elections:
        parsed = h.extract_date(context.dataset, election, fallback_to_original=False)
        assert len(parsed) == 1, ("Unparsed election date", election)
        counts[parsed[0]] += 1
    election_date, votes = counts.most_common(1)[0]
    if votes < 0.8 * len(elections):
        context.log.warning(
            "Members' pages disagree on the election date", dates=dict(counts)
        )
    if election_date < h.backdate(settings.RUN_TIME, STALE_AFTER):
        context.log.warning(
            "The roster's election is over a term old; is the listing stale?",
            election_date=election_date,
        )
    return election_date


def crawl_leaders(
    context: Context,
    url: str,
    holders: int,
    office: Entity,
    member: Entity,
    member_categorisation: PositionCategorisation,
    election_date: str,
) -> None:
    """Leadership pages hold free-form biographies of one or more office holders,
    each introduced by their name and the date they were elected to the office."""
    office_categorisation = categorise(context, office)
    doc = fetch_html(
        context,
        url,
        unblock_validator="//div[contains(@class, 'wrap-page-news')]",
        html_source="httpResponseBody",
    )
    text = page_text(h.xpath_element(doc, "//div[contains(@class, 'wrap-page-news')]"))
    matches = list(LEADER.finditer(text))
    # A holder whose name the regex misses would fold into the previous holder's
    # biography, so pin the number of holders each page lists.
    assert len(matches) == holders, (url, [m.group("name") for m in matches])
    for idx, match in enumerate(matches):
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
        name = match.group("name")
        person = context.make("Person")
        # The listing leaves the officers out and their pages don't link a
        # /deputats/<id> page, so they are keyed by name.
        person.id = context.make_id(name)
        person.add("name", name, lang="tgk")
        person.add("sourceUrl", url)
        apply_biography(person, text[match.end() : end])

        # The presiding officers are deputies elected from among the members. Their
        # biographies don't state when they were elected deputy, so they take the
        # sitting convocation's election date.
        membership = h.make_occupancy(
            context,
            person,
            member,
            election_date=election_date,
            categorisation=member_categorisation,
        )
        if membership is None:
            continue
        context.emit(membership)
        context.emit(person)
        if not office_categorisation.is_pep:
            continue
        occupancy = h.make_occupancy(
            context,
            person,
            office,
            start_date=match_date(match),
            categorisation=office_categorisation,
        )
        if occupancy is None:
            continue
        context.emit(occupancy)
        context.emit(office)


def crawl(context: Context) -> None:
    member = h.make_position(
        context,
        name="Member of the Majlisi Namoyandagon of Tajikistan",
        country="tj",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q21328584",
        lang="eng",
    )
    member_categorisation = categorise(context, member)
    if not member_categorisation.is_pep:
        context.log.warning("Position is not categorised as PEP", position=member.id)
        return
    context.emit(member)

    # The listing pages through the deputies, 12 at a time. Its card names are
    # abbreviated or missing, so only the links are used.
    elections: list[str] = []
    page = 1
    next_url: str | None = context.data_url
    while next_url is not None:
        doc = fetch_html(
            context,
            next_url,
            unblock_validator="//div[contains(@class, 'deputats-page')]",
            html_source="httpResponseBody",
            absolute_links=True,
        )
        links = h.xpath_strings(
            doc,
            "//div[contains(@class, 'deputats-page')]//li"
            "/a[contains(@href, '/deputats/')]/@href",
        )
        assert len(links) > 0, next_url
        next_links = h.xpath_strings(
            doc, "//ul[@class='pagination-standard']//a[text()='»']/@href"
        )
        assert len(next_links) <= 1, next_links
        next_url = next_links[0] if len(next_links) == 1 else None
        # The pagination shows a window of page numbers, not the last one, so
        # follow "»" but fail if it goes missing while a later page is listed.
        if next_url is None:
            numbers = h.xpath_strings(
                doc, "//ul[@class='pagination-standard']//a/text()"
            )
            later = [n for n in numbers if n.isdigit() and int(n) > page]
            assert len(later) == 0, ("No next link, but later pages listed", later)
        page += 1
        for url in links:
            election = crawl_member(context, url, member, member_categorisation)
            if election is not None:
                elections.append(election)
    election_date = check_sitting_term(context, elections)

    # The listing leaves out the chamber's presiding officers.
    for url, holders, name, wikidata_id in [
        (
            "https://www.parlament.tj/chairman",
            1,
            "Chairman of the Majlisi Namoyandagon of Tajikistan",
            "Q110161322",
        ),
        (
            "https://www.parlament.tj/first_deputy",
            1,
            "First Deputy Chairman of the Majlisi Namoyandagon of Tajikistan",
            None,
        ),
        (
            "https://www.parlament.tj/deputies",
            2,
            "Deputy Chairman of the Majlisi Namoyandagon of Tajikistan",
            None,
        ),
    ]:
        office = h.make_position(
            context,
            name=name,
            country="tj",
            topics=["gov.national", "gov.legislative"],
            wikidata_id=wikidata_id,
            lang="eng",
        )
        crawl_leaders(
            context,
            url,
            holders,
            office,
            member,
            member_categorisation,
            election_date,
        )
