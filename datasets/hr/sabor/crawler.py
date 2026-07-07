import re
from itertools import count


from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

# MP profile links look like /en/members-parliament/<name-slug>-<n>-term (the party and
# deputy-club links under /members-parliament/ are excluded).
MP_LINK = re.compile(r"/members-parliament/[^/]+-\d+-term/?$")
TERM_SUFFIX = re.compile(r"-\d+-term$")
# Biographies open with "Born on <d Month yyyy> in <place>."
BORN = re.compile(r"Born on (\d{1,2} \w+ \d{4})(?: in ([^.]+))?")
CONSTITUENCY = re.compile(r"Constituency:\s*(.+?)\s*(?:Overview|Education|Contact|$)")


def is_mp_link(href: str) -> bool:
    return (
        MP_LINK.search(href) is not None
        and "/party/" not in href
        and "/club/" not in href
    )


def crawl_member(
    context: Context,
    name: str,
    url: str,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    slug = url.rstrip("/").split("/")[-1]
    person = context.make("Person")
    # Drop the term suffix so a re-elected MP keeps one entity across terms.
    person.id = context.make_slug("mp", TERM_SUFFIX.sub("", slug))
    # Names are listed "Last, First" (the surname may be multi-word).
    last_name, _, first_name = name.partition(",")
    h.apply_name(
        person,
        first_name=first_name.strip() or None,
        last_name=last_name.strip(),
        lang="hrv",
    )

    doc = context.fetch_html(url, cache_days=7)
    text = h.element_text(doc)
    born = BORN.search(text)
    if born is not None:
        h.apply_date(person, "birthDate", born.group(1))
        if born.group(2) is not None:
            person.add("birthPlace", born.group(2).strip())
    for party in h.xpath_elements(
        doc, ".//a[contains(@href, '/members-parliament/party/')]"
    ):
        person.add("political", h.element_text(party))
        break
    # Suffrage in Croatian Parliament elections is reserved to Croatian citizens
    # (Constitution art. 45). https://www.constituteproject.org/constitution/Croatia_2013
    person.add("citizenship", "hr")
    person.add("sourceUrl", url)

    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return
    constituency = CONSTITUENCY.search(text)
    if constituency is not None:
        occupancy.add("constituency", constituency.group(1))
    for club in h.xpath_elements(
        doc, ".//a[contains(@href, '/members-parliament/club/')]"
    ):
        occupancy.add("politicalGroup", h.element_text(club))
        break

    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Croatian Parliament",
        country="hr",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q18643511",
        lang="eng",
    )
    categorisation = categorise(context, position)
    context.emit(position)

    seen: set[str] = set()
    for page in count(0):
        doc = context.fetch_html(
            context.data_url, params={"page": page}, cache_days=1, absolute_links=True
        )
        fresh = 0
        for link in h.xpath_elements(doc, "//a[@href]"):
            href = (link.get("href") or "").strip()
            if not is_mp_link(href) or href in seen:
                continue
            seen.add(href)
            fresh += 1
            crawl_member(context, h.element_text(link), href, position, categorisation)
        if fresh == 0:
            break
