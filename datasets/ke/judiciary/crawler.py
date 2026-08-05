from lxml.html import HtmlElement

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

# Judge cards live in the single team-member grid on each court's listing page.
# Scoping card selection to that grid keeps stray team-member widgets elsewhere
# on the page out, without hardcoding a per-court category slug.
CARDS_XPATH = (
    "//div[contains(@class, 'wpb-our-team-members')]"
    "//div[contains(@class, 'wpb-team-default-item')]"
)

# One listing page (relative to the dataset URL) and one court-level position
# per superior court.
COURTS = [
    {
        "path": "supreme-court-judges/",
        "position": "Justice of the Supreme Court of Kenya",
    },
    {
        "path": "court-of-appeal-judges/",
        "position": "Judge of the Court of Appeal of Kenya",
    },
    {
        "path": "high-court-judges/",
        "position": "Judge of the High Court of Kenya",
    },
    {
        "path": "employment-and-labour-relations-court-judges/",
        "position": "Judge of the Employment and Labour Relations Court of Kenya",
    },
    {
        "path": "environment-and-land-court-judges/",
        "position": "Judge of the Environment and Land Court of Kenya",
    },
]

# Card position titles marking a judicial leadership office. Holders receive an
# additional Position/Occupancy on top of their court-level one. Keys are the
# lower-cased, whitespace-normalised title as it appears on the card.
LEADERSHIP_POSITIONS = {
    "chief justice and president of the supreme court of kenya": (
        "Chief Justice of Kenya"
    ),
    "deputy chief justice and vice president of the supreme court of kenya": (
        "Deputy Chief Justice of Kenya"
    ),
    "principal judge, high court": "Principal Judge of the High Court of Kenya",
    "principal judge, elrc": (
        "Principal Judge of the Employment and Labour Relations Court of Kenya"
    ),
    "presiding judge, environment and land court": (
        "Presiding Judge of the Environment and Land Court of Kenya"
    ),
}


def make_pep_position(
    context: Context, name: str
) -> tuple[Entity, PositionCategorisation]:
    position = h.make_position(
        context,
        name=name,
        country="ke",
        topics=["gov.national", "gov.judicial"],
        lang="eng",
    )
    categorisation = categorise(context, position)
    context.emit(position)
    return position, categorisation


def parse_cards(doc: HtmlElement) -> list[tuple[str, str, str]]:
    """Return (profile_url, name, position_title) per judge card.

    Judges are rendered as WordPress team-member cards. Each card has one name
    anchor in `h5.wpb-otm-name` and one `span.designation` role label.
    """
    results: list[tuple[str, str, str]] = []
    for card in h.xpath_elements(doc, CARDS_XPATH):
        url = h.xpath_string(card, ".//h5[@class='wpb-otm-name']/a/@href")
        name = h.xpath_string(card, ".//h5[@class='wpb-otm-name']/a/text()")
        position_title = h.xpath_string(card, ".//span[@class='designation']/text()")
        results.append((url, name, position_title))
    return results


def crawl_judge(
    context: Context,
    profile_url: str,
    raw_name: str,
    positions: list[tuple[Entity, PositionCategorisation]],
) -> None:
    person = context.make("Person")
    person.id = context.make_id(profile_url.rstrip("/").split("/")[-1])

    clean_name = h.strip_name_titles(context, raw_name)
    original_name = raw_name if clean_name != raw_name else None
    person.add("name", clean_name, lang="eng", original_value=original_name)
    person.add("sourceUrl", profile_url)
    # Judges are State officers (Constitution of Kenya, art. 260); State
    # officers must be Kenyan citizens and may not hold dual citizenship
    # (arts. 78(1), 78(2)).
    # https://www.constituteproject.org/constitution/Kenya_2010
    person.add("citizenship", "ke")
    person.add("topics", "role.judge")

    for position, categorisation in positions:
        occupancy = h.make_occupancy(
            context, person, position, categorisation=categorisation
        )
        if occupancy is not None:
            context.emit(occupancy)
            context.emit(person)


def crawl(context: Context) -> None:
    # Leadership offices are held alongside a court seat; build them once.
    leadership: dict[str, tuple[Entity, PositionCategorisation]] = {}
    for label, office in LEADERSHIP_POSITIONS.items():
        leadership[label] = make_pep_position(context, office)

    for court in COURTS:
        court_position = make_pep_position(context, court["position"])
        doc = context.fetch_html(
            context.data_url + court["path"], cache_days=1, absolute_links=True
        )
        cards = parse_cards(doc)
        assert len(cards) > 0, f"No judges found for {court['path']}"
        for profile_url, raw_name, position_title in cards:
            positions = [court_position]
            label = " ".join(position_title.split()).casefold()
            if label in leadership:
                positions.append(leadership[label])
            crawl_judge(context, profile_url, raw_name, positions)
        context.log.info("Crawled court", position=court["position"], judges=len(cards))
