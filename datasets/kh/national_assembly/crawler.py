import re
from dataclasses import dataclass, replace
from urllib.parse import urljoin

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.util import Element

TOPICS = ["gov.national", "gov.legislative"]
# Khmer digits U+17E0..U+17E9 -> ASCII, so row numbers and years can be read off the page
# without embedding Khmer numerals in the source.
KHMER_DIGITS = {0x17E0 + i: str(i) for i in range(10)}
# The site's navigation labels each legislature with its ordinal and years, e.g.
# "<Khmer for legislature no.>7 (2023-2028)".
LEGISLATURE_LABEL = re.compile(r"(\d+)\s*\(\s*(\d{4})\s*-\s*(\d{4})\s*\)\s*$")


@dataclass(frozen=True)
class Legislature:
    """One five-year legislature of the National Assembly.

    The source dates a legislature by year only and gives no dates for the individual
    member, so the period bounds are the same for everyone who served the legislature.
    """

    ordinal: int
    period_start: str
    period_end: str
    url: str
    sitting: bool
    """Whether this is the newest legislature listed, i.e. the one in session."""


def parse_legislatures(context: Context, doc: Element) -> list[Legislature]:
    """Read the legislature switcher out of the site navigation, newest first."""
    legislatures: dict[int, Legislature] = {}
    for link in h.xpath_elements(doc, '//a[starts-with(@href, "/group-article/")]'):
        href = link.get("href")
        assert href is not None, "Navigation link without a target"
        match = LEGISLATURE_LABEL.search(h.element_text(link).translate(KHMER_DIGITS))
        if match is None:
            continue
        ordinal = int(match.group(1))
        legislature = Legislature(
            ordinal=ordinal,
            period_start=match.group(2),
            period_end=match.group(3),
            url=urljoin(context.data_url, href.strip()),
            sitting=False,
        )
        if legislatures.setdefault(ordinal, legislature) != legislature:
            raise ValueError(f"Legislature {ordinal} is listed twice, differently")

    ordered = sorted(legislatures.values(), key=lambda leg: leg.ordinal, reverse=True)
    # The switcher covers every legislature since the first, so a navigation change that
    # drops or renames an entry fails here rather than quietly narrowing the crawl.
    ordinals = [leg.ordinal for leg in ordered]
    if not ordinals or ordinals != list(range(len(ordinals), 0, -1)):
        raise ValueError(f"Legislatures are not numbered 1..n: {ordinals}")
    # Each legislature gets its own group-article id, so a newly elected one shows up
    # under an id nobody has looked at yet. Fail until data.url names the newest.
    if ordered[0].url != context.data_url:
        raise ValueError(
            f"The newest legislature is {ordered[0].url}, not the configured "
            f"{context.data_url}. Review it, then update the dataset's data.url."
        )
    return [replace(ordered[0], sitting=True), *ordered[1:]]


def parse_roster(context: Context, doc: Element) -> list[tuple[str, str, str]] | None:
    """Read (name, constituency, party) per member from one list of members.

    Returns None when this revision of the list is published as a scanned image instead
    of as a table.
    """
    body = h.xpath_element(
        doc, '//span[@id="ContentPlaceHolder1_DataList5_FullTextLabel_0"]'
    )
    rows = h.xpath_elements(body, ".//tr[count(./td) = 4]")
    if not rows:
        return None
    headings = [h.element_text(cell) for cell in h.xpath_elements(rows[0], "./td")]
    if headings != context.dataset.config["roster_columns"]:
        raise ValueError(f"Unexpected roster columns: {headings}")

    members: list[tuple[str, str, str]] = []
    numbers: list[int] = []
    for row in rows[1:]:
        cells = h.xpath_elements(row, "./td", expect_exactly=4)
        number, name, constituency, party = [h.element_text(cell) for cell in cells]
        # Member rows are the ones numbered with a Khmer numeral.
        if not number.translate(KHMER_DIGITS).isdigit():
            continue
        assert name, f"Member {number} has no name"
        assert constituency, f"Member {number} has no constituency"
        assert party, f"Member {number} has no party"
        numbers.append(int(number.translate(KHMER_DIGITS)))
        members.append((name, constituency, party))

    # The list is numbered from 1 without gaps, so a dropped row, or a second list
    # concatenated onto this one, fails here.
    if not numbers or numbers != list(range(1, len(numbers) + 1)):
        raise ValueError(f"List of members is not numbered 1..n: {numbers}")
    return members


def clean_member_name(context: Context, name: str) -> str:
    """Strip the honorific a member is styled with.

    The honorific changes over a member's career, as they take a doctorate or are
    elevated to Samdech, so it has to come off for the name to identify them across
    legislatures.
    """
    clean_name = h.strip_name_titles(context, name)
    # strip_name_titles warns and returns None only for a value that is all honorifics.
    assert clean_name is not None, name
    roots = context.dataset.config["honorific_roots"]
    if any(clean_name.startswith(root) for root in roots):
        context.log.warning(
            "Honorific left on name; add its exact form to names.prefixes_strip",
            name=name,
        )
    return clean_name


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    legislature: Legislature,
    clean_name: str,
    name: str,
    constituency: str,
    party: str,
) -> None:
    person = context.make("Person")
    # Keyed on the name alone: a member's constituency changes between legislatures, as a
    # province is split or they stand elsewhere, so including it would split them into one
    # entity per legislature. Names are unique within each list of members, and the caller
    # fails if that stops holding, because two members of one legislature sharing a name
    # cannot be told apart by anything else the source publishes.
    person.id = context.make_id(clean_name)
    person.add(
        "name",
        clean_name,
        lang="khm",
        original_value=name if clean_name != name else None,
    )
    person.add("political", party, lang="khm")
    # Candidates for the National Assembly must hold Khmer nationality by birth
    # (Constitution of Cambodia, Article 76).
    # https://constitutionnet.org/sites/default/files/Cambodia%20Constitution.pdf
    person.add("citizenship", "kh")

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        period_start=legislature.period_start,
        period_end=legislature.period_end,
        # Per occupancy: only the sitting legislature's list is still revised as members
        # are replaced, so only there does the lack of an end date mean they still serve.
        no_end_implies_current=legislature.sitting,
    )
    if occupancy is None:
        return
    occupancy.add("constituency", constituency, lang="khm")
    context.emit(occupancy)
    context.emit(person)


def crawl_legislature(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    legislature: Legislature,
) -> None:
    doc = context.fetch_html(legislature.url, cache_days=1)
    listing = h.xpath_element(doc, '//table[@id="ContentPlaceHolder1_DListEmp"]')
    # A legislature's page lists revisions of its list of members, newest first, each
    # superseding the previous. Take the newest published as a table: some revisions carry
    # the list as a scanned image instead, e.g. https://nac.org.kh/article/6531.
    members: list[tuple[str, str, str]] | None = None
    for href in dict.fromkeys(h.xpath_strings(listing, ".//a/@href")):
        url = urljoin(legislature.url, href.strip())
        members = parse_roster(context, context.fetch_html(url, cache_days=1))
        if members is not None:
            break
    if members is None:
        raise ValueError(
            "No list of members is published as a table for legislature "
            f"{legislature.ordinal}: {legislature.url}"
        )

    names: set[str] = set()
    for name, constituency, party in members:
        clean_name = clean_member_name(context, name)
        if clean_name in names:
            raise ValueError(
                f"Two members of legislature {legislature.ordinal} are named {name}"
            )
        names.add(clean_name)
        crawl_member(
            context,
            position,
            categorisation,
            legislature,
            clean_name,
            name,
            constituency,
            party,
        )


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the National Assembly of Cambodia",
        country="kh",
        topics=TOPICS,
        wikidata_id="Q21295974",
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    doc = context.fetch_html(context.data_url, cache_days=1)
    cutoff = h.earliest_term_start(TOPICS)
    for legislature in parse_legislatures(context, doc):
        # ISO strings compare correctly here, including the year-only period bounds.
        if legislature.period_end < cutoff:
            context.log.info(
                "Legislature predates the PEP window; skipping",
                legislature=legislature.ordinal,
                url=legislature.url,
            )
            break
        if legislature.ordinal in context.dataset.config["legislatures_without_roster"]:
            context.log.info(
                "Legislature's members are not published as text; skipping",
                legislature=legislature.ordinal,
                url=legislature.url,
            )
            continue
        crawl_legislature(context, position, categorisation, legislature)
