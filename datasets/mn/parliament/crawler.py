import re

from zavod import Context
from zavod import helpers as h
from zavod.stateful.positions import categorise
from zavod.util import Element

# The roster is published in two languages with one CV record per language, under
# different member IDs. They are joined on the shared parliamentary email address;
# the ID is therefore derived from the email, so the Mongolian (Cyrillic) and English
# (Latin) records of one member collapse into a single entity. Records whose English
# CV omits the email get their own ID and are reconciled downstream by name.
LIST_MN = "https://www.parliament.mn/cv/"
LIST_EN = "https://www.parliament.mn/en/cv/"

MEMBER = "Member of the State Great Khural"


def member_ids(doc: Element) -> list[str]:
    """Member IDs from a roster page, scoped to the member-card grid.

    The left nav menu hardcodes the chairman's CV link on every page, so page-wide
    anchor selection would pollute the roster — select only the photo cards.
    """
    ids: list[str] = []
    for anchor in h.xpath_elements(doc, "//div[@class='entry-image mb-0']/a"):
        href = anchor.get("href")
        if href is None:
            continue
        match = re.search(r"/cv/(\d+)/", href)
        if match is not None and match.group(1) not in ids:
            ids.append(match.group(1))
    return ids


def member_email(doc: Element) -> str | None:
    """The member's own parliamentary email (excludes the shared secretariat inbox)."""
    for anchor in h.xpath_elements(doc, "//a[starts-with(@href, 'mailto:')]"):
        href = anchor.get("href")
        if href is None:
            continue
        email = href.split(":", 1)[1].strip().lower()
        if email and "secretariat" not in email:
            return email
    return None


def discover_party_filters(doc: Element) -> dict[str, str]:
    """Party filters from the roster's "filter by party" links: partyId -> label.

    Parties are read from the source rather than hardcoded, so a newly seated party is
    picked up automatically and the labels stay the source's own party names.
    """
    filters: dict[str, str] = {}
    for anchor in h.xpath_elements(doc, "//a[contains(@href, 'partyId=')]"):
        href = anchor.get("href")
        if href is None:
            continue
        match = re.search(r"partyId=(\d+)", href)
        label = h.element_text(anchor)
        if match is not None and label:
            filters[match.group(1)] = label
    return filters


def build_party_map(context: Context) -> dict[str, set[str]]:
    """Map each Mongolian-record member ID to the set of parties it is filed under.

    A member may appear under more than one party filter (the National Coalition is an
    electoral alliance), so parties are collected as a set.
    """
    roster = context.fetch_html(LIST_MN, cache_days=1, absolute_links=True)
    filters = discover_party_filters(roster)
    if not filters:
        raise RuntimeError("No party filters found on the roster page")
    party_map: dict[str, set[str]] = {}
    for party_id, party_name in filters.items():
        url = f"{LIST_MN}?partyId={party_id}"
        doc = context.fetch_html(url, cache_days=1, absolute_links=True)
        ids = member_ids(doc)
        if not ids:
            raise RuntimeError(
                f"Party filter {party_id} ({party_name}) returned no members"
            )
        for member_id in ids:
            party_map.setdefault(member_id, set()).add(party_name)
    return party_map


def crawl_member(
    context: Context,
    list_url: str,
    member_id: str,
    lang: str,
    parties: set[str],
) -> None:
    doc = context.fetch_html(
        f"{list_url}{member_id}/", cache_days=7, absolute_links=True
    )

    last = h.xpath_elements(doc, "//div[@class='lastname']")
    first = h.xpath_elements(doc, "//div[@class='firstname']")
    name = " ".join(part for el in (*last, *first) if (part := h.element_text(el)))
    if not name:
        context.log.warning("Member has no name", url=f"{list_url}{member_id}/")
        return

    email = member_email(doc)

    person = context.make("Person")
    # Email is the cross-language join key (see module docstring); fall back to the
    # opaque member ID, which is unique within a single language's roster.
    if email is not None:
        person.id = context.make_id("member", email)
    else:
        person.id = context.make_slug("member", lang, member_id)
    person.add("name", name, lang=lang)
    person.add("email", email)
    for party in parties:
        person.add("political", party, lang="mon")
    # Members of the State Great Khural must be citizens of Mongolia.
    # Constitution of Mongolia, Art. 21(3): "Any citizen of Mongolia, who have attained
    # the age of twenty five years and are qualified to vote, shall be eligible to be
    # elected to the State Great Hural (Parliament)."
    # https://www.constituteproject.org/constitution/Mongolia_2001
    person.add("citizenship", "mn")

    position = h.make_position(
        context, MEMBER, country="mn", topics=["gov.national", "gov.legislative"]
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        no_end_implies_current=True,
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    context.emit(person)
    context.emit(position)
    context.emit(occupancy)


def crawl_roster(
    context: Context, list_url: str, lang: str, party_map: dict[str, set[str]]
) -> None:
    doc = context.fetch_html(list_url, cache_days=1, absolute_links=True)
    ids = member_ids(doc)
    if not 100 <= len(ids) <= 140:
        context.log.warning(f"Unexpected member count in {lang} roster: {len(ids)}")
    for member_id in ids:
        crawl_member(
            context, list_url, member_id, lang, party_map.get(member_id, set())
        )


def crawl(context: Context) -> None:
    party_map = build_party_map(context)
    crawl_roster(context, LIST_MN, "mon", party_map)
    # English records add the official Latin name. Party is taken only from the
    # Mongolian roster; English-only records inherit it on downstream merge.
    crawl_roster(context, LIST_EN, "eng", {})
