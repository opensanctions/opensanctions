from lxml.html import HtmlElement

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

# Each superior court has its own listing page and its own position. The
# category slug appears in the CSS classes of each judge card and is used as
# a guard against cross-category widgets; min_count guards against silent
# page-structure changes per court.
COURTS = [
    {
        "url": "https://judiciary.go.ke/supreme-court-judges/",
        "category": "supreme-court-judges",
        "position": "Justice of the Supreme Court of Kenya",
        "min_count": 5,
    },
    {
        "url": "https://judiciary.go.ke/court-of-appeal-judges/",
        "category": "court-of-appeal-judges",
        "position": "Judge of the Court of Appeal of Kenya",
        "min_count": 25,
    },
    {
        "url": "https://judiciary.go.ke/high-court-judges/",
        "category": "high-court-judges",
        "position": "Judge of the High Court of Kenya",
        "min_count": 70,
    },
    {
        "url": "https://judiciary.go.ke/employment-and-labour-relations-court-judges/",
        "category": "employment-and-labour-relations-court-judges",
        "position": "Judge of the Employment and Labour Relations Court of Kenya",
        "min_count": 10,
    },
    {
        "url": "https://judiciary.go.ke/environment-and-land-court-judges/",
        "category": "environment-and-land-court-judges",
        "position": "Judge of the Environment and Land Court of Kenya",
        "min_count": 35,
    },
]

# Card designations marking a judicial leadership office. Holders receive an
# additional Position/Occupancy on top of their court-level one.
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

# Designations that carry no extra office. "President-Elect, Court of Appeal"
# is deliberately here: the source marks the holder as elect, so they are
# modelled as a serving Court of Appeal judge only, not as court president.
PLAIN_DESIGNATIONS = {
    "judge of the supreme court of kenya",
    "judge, court of appeal",
    "president-elect, court of appeal",
    "judge, high court",
    "judge of the elrc",
    "judge, environment and land court",
}

# Kenyan honours and other post-nominals that follow a comma in the listed
# name (e.g. "Hon. Justice Martha Koome, EGH"). Only known post-nominals are
# stripped so that a data change upstream surfaces as a warning rather than
# silently truncating a name.
POST_NOMINALS = {
    "EGH",
    "MGH",
    "CBS",
    "EBS",
    "OGW",
    "HSC",
    "SC",
    "FCIArb",
    "FCPS(K)",
    "MBS",
    "PhD",
    "OLY",
}
POST_NOMINALS_NORMALIZED = {p.replace(".", "").casefold() for p in POST_NOMINALS}


def normalize_titles(raw: str) -> str:
    """Fix formatting artifacts seen on the site.

    - glued honorifics: "Hon.Lady Justice ..." -> "Hon. Lady Justice ..."
    - hyphen broken by a line wrap: "Onga- rora" -> "Onga-rora" (the site's
      own profile slugs write the unbroken form)
    - spaced en-dash in double-barrelled surnames: "Lagat – Korir" ->
      "Lagat-Korir"
    """
    name = " ".join(raw.replace("Hon.", "Hon. ").split())
    name = name.replace(" – ", "-").replace("–", "-")
    if "- " in name:
        name = name.replace("- ", "-")
    return name


def clean_post_nominals(context: Context, raw: str) -> str:
    parts = [part.strip() for part in raw.split(",")]
    name = parts[0]
    for extra in parts[1:]:
        if extra.replace(".", "").casefold() not in POST_NOMINALS_NORMALIZED:
            context.log.warning("Unknown post-nominal, keeping in name", name=raw)
            return raw
    return name


def crawl_judge(
    context: Context,
    profile_url: str,
    raw_name: str,
    positions: list[tuple[Entity, PositionCategorisation]],
) -> None:
    slug = profile_url.rstrip("/").split("/")[-1]

    person = context.make("Person")
    person.id = context.make_id(slug)
    raw_name = normalize_titles(raw_name)
    no_honours = clean_post_nominals(context, raw_name)
    clean_name = h.strip_name_titles(context, no_honours)
    original_name = raw_name if clean_name != raw_name else None
    person.add("name", clean_name, lang="eng", original_value=original_name)
    person.add("sourceUrl", profile_url)
    # Judges are State officers (Constitution of Kenya, art. 260); State
    # officers must be Kenyan citizens and may not hold dual citizenship
    # (arts. 78(1), 78(2)).
    # https://www.constituteproject.org/constitution/Kenya_2010
    person.add("citizenship", "ke")

    emitted = False
    for position, categorisation in positions:
        occupancy = h.make_occupancy(
            context, person, position, categorisation=categorisation
        )
        if occupancy is None:
            continue
        context.emit(occupancy)
        emitted = True
    if emitted:
        context.emit(person)


def parse_cards(doc: HtmlElement, category: str) -> list[tuple[str, str, str | None]]:
    """Return (profile_url, raw_name, designation) per judge card.

    Judges are rendered as WordPress team-member cards. Each card carries the
    court's category as a CSS class token, links to the judge's profile page
    (the anchor around the name has text; the photo anchor has none), and
    holds the role label in a `span.designation` element.
    """
    card_xpath = (
        "//*[contains(concat(' ', normalize-space(@class), ' '), "
        f"' wpb_team_member_category-{category} ')]"
    )
    results: list[tuple[str, str, str | None]] = []
    seen: set[str] = set()
    for card in h.xpath_elements(doc, card_xpath):
        url, name = None, ""
        for anchor in h.xpath_elements(card, ".//a[contains(@href, '/team_member/')]"):
            text = " ".join(anchor.text_content().split())
            href = anchor.get("href")
            if href and len(text) > len(name):
                url, name = href, text
        if url is None or len(name) < 5:
            continue
        url = url.split("#", 1)[0].split("?", 1)[0]
        if url in seen:
            continue
        seen.add(url)
        spans = h.xpath_elements(card, ".//span[@class='designation']")
        designation = h.element_text(spans[0]) if spans else None
        results.append((url, name, designation))
    return results


def make_pep_position(
    context: Context, name: str
) -> tuple[Entity, PositionCategorisation] | None:
    position = h.make_position(
        context,
        name=name,
        country="ke",
        topics=["gov.national", "gov.judicial"],
        lang="eng",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return None
    context.emit(position)
    return position, categorisation


def crawl(context: Context) -> None:
    leadership: dict[str, tuple[Entity, PositionCategorisation]] = {}
    for court in COURTS:
        court_position = make_pep_position(context, court["position"])
        if court_position is None:
            continue

        doc = context.fetch_html(court["url"], cache_days=1, absolute_links=True)
        cards = parse_cards(doc, court["category"])
        if len(cards) < court["min_count"]:
            context.log.warning(
                "Unexpectedly few judges on page",
                url=court["url"],
                found=len(cards),
                expected_at_least=court["min_count"],
            )
        for profile_url, raw_name, designation in cards:
            positions = [court_position]
            label = " ".join((designation or "").split()).casefold()
            if label in LEADERSHIP_POSITIONS:
                office = LEADERSHIP_POSITIONS[label]
                if office not in leadership:
                    office_position = make_pep_position(context, office)
                    if office_position is not None:
                        leadership[office] = office_position
                if office in leadership:
                    positions.append(leadership[office])
            elif label and label not in PLAIN_DESIGNATIONS:
                context.log.warning(
                    "Unknown designation, treating as plain judge",
                    designation=designation,
                    name=raw_name,
                )
            crawl_judge(context, profile_url, raw_name, positions)
        context.log.info("Crawled court", position=court["position"], judges=len(cards))
