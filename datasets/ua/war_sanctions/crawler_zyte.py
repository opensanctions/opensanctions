"""Public-website crawler for the GUR War & Sanctions dataset.

Sources data from the public site (https://war-sanctions.gur.gov.ua) via Zyte, rather
than the confidential JSON API. The site is server-rendered, so a plain HTTP fetch is
enough. Entity IDs use an IMO microformat (imo-vsl-<imo> for vessels, imo-org-<imo> for
ship-management orgs) so they don't depend on the source's internal numeric ids.
"""

import re
from collections.abc import Iterator
from typing import Optional
from urllib.parse import urljoin

from normality import squash_spaces

from zavod import Context, Entity, helpers as h
from zavod.util import Element
from zavod.extract.zyte_api import fetch_html

# Success marker for Zyte unblocking: the language switcher is on every real page.
UNBLOCK = ".//div[contains(@class, 'lang')]"
CACHE_DAYS = 7
LISTING_PER_PAGE = 12
SPLIT = " / "

# "Name (IMO / Country / Date)" — the format of owner/manager rows on a vessel page. The
# date is dot- or slash-separated (DD.MM.YYYY / DD/MM/YYYY); allowing both in the date group
# stops a slash-date's leading DD/MM from bleeding back into the country group.
PARTY_RE = re.compile(
    r"^(?P<name>.+?)\s*\((?P<imo>\d+)\s*/\s*(?P<country>.+?)\s*/\s*(?P<date>[\d./]+)\)\s*$"
)

# The vessel-page row whose value links to the associated ships-company (shadow operator).
OPERATOR_LABEL = "The person in connection with whomsanctions have been applied"

# Vessel-page labels we read elsewhere (links block) or intentionally don't emit. Anything
# left in the label map that isn't here trips a warning, so layout drift is caught loudly.
VESSEL_SKIP_LABELS = {
    "Category",  # free-text restating the sanction reason
    "Length (m)",  # no FtM vessel property
    "Sanctions",  # sanctioning country, already on the Sanction via publisher
    "Sanctions lifted",
    "Cases of AIS shutdown",  # behavioural flags — revisit if we want them as notes
    "Calling at russian ports",
    "Visited ports",
    "Builder (country)",  # shipyard — not modelled
}

# Entity-page (col-sm-8) labels we don't emit as properties. "Within the structure of
# Rostec" is handled separately (parsed into Ownership edges), not skipped.
COMPANY_SKIP_LABELS = {"Products"}

# Liquidated companies render a status badge inside the name label, so the label text reads
# "Full name of legal entity Liquidated 30.05.2025" rather than the bare field name. We match
# the name label by prefix and lift any trailing "Liquidated <date>" into a dissolution date.
COMPANY_NAME_LABEL = "Full name of legal entity"
LIQUIDATED_RE = re.compile(r"\bLiquidated\b\s*(?P<date>[\d.]*)")

# Person-page label aliases — they vary by section (war sections vs partner sanctions vs
# executives). Each FtM property is fed from any of its aliases.
PERSON_CITIZENSHIP_LABELS = ["Citizenship", "Jurisdiction"]
PERSON_DOB_LABELS = ["Date and place of birth", "DOB"]
PERSON_POSITION_LABELS = [
    "Position",
    "Positions or membership in the governance bodies of the russian MIC",
    "Other positions",
    "Former position in the management bodies of the Russian military-industrial complex",
]

PERSON_LINK_LABELS = ["Links", "Archive links"]

# "Go to site" is a footer navigation button, not entity data.
PERSON_SKIP_LABELS = {"Go to site", "Permission for illegal excavations"}


# Persons list their name forms in a fixed order (verified across sections): the site
# renders name_uk, then name_ru, then name_en (Latin). When exactly three forms are present
# this order is authoritative, so we tag languages by position. We deliberately don't infer
# language any other way — script detection can't tell a Ukrainian name from a Russian one
# once it lacks Ukrainian-only letters — so any other shape is emitted untagged.
PERSON_NAME_LANGS = ("ukr", "rus", "eng")


def add_person_names(person: Entity, lines: list[str]) -> None:
    """Add a person's name forms: positional uk/ru/en when all three are present, else
    untagged (lang=None)."""
    if len(lines) == len(PERSON_NAME_LANGS):
        for value, lang in zip(lines, PERSON_NAME_LANGS):
            person.add("name", value, lang=lang)
    else:
        person.add("name", lines)


def label_map(doc: Element) -> dict[str, Element]:
    """Map every visible 'label -> value' pair on a detail page to the value element.

    The site renders detail pages as label/value sibling pairs in a Bootstrap grid. The top
    spec table uses right-aligned label columns; the 'additional information' block uses bare
    label divs whose value sibling is the yellow-highlighted column. Restricting the second
    case to "next sibling is .yellow" avoids mistaking value divs (which are also classless)
    for labels. Labels are keyed by their text so nested tags don't truncate them.
    """
    pairs: dict[str, Element] = {}
    label_xpaths = [
        "//div[contains(@class,'col-lg-5') and contains(@class,'text-lg-right')]",
        "//div[not(@class) and following-sibling::*[1][contains(@class,'yellow')]]",
    ]
    for xp in label_xpaths:
        for el in h.xpath_elements(doc, xp):
            label = h.element_text(el)
            if not label or len(label) > 80:
                continue
            value_el = el.getnext()
            if value_el is None:
                continue
            pairs.setdefault(label, value_el)
    return pairs


def resource_links(doc: Element) -> list[str]:
    """External reference links from the page's 'Links' block."""
    return h.xpath_strings(
        doc, "//div[normalize-space(text())='Links']/following-sibling::div//a/@href"
    )


def parse_party(raw: Optional[str]) -> Optional[dict[str, str]]:
    """Parse a 'Name (IMO / Country / Date)' owner/manager row."""
    if raw is None:
        return None
    match = PARTY_RE.match(raw)
    if match is None:
        return None
    return match.groupdict()


def emit_party(
    context: Context,
    vessel: Entity,
    raw: Optional[str],
    *,
    role: str,
    schema: str,
    from_prop: str,
    to_prop: str,
) -> None:
    party = parse_party(raw)
    if party is None:
        return
    org = context.make("Company")
    org.id = h.make_org_imo_id(party["imo"])
    org.add("name", party["name"])
    org.add("imoNumber", party["imo"])
    org.add("country", party["country"])
    org.add("topics", "poi")
    org.add("sourceUrl", vessel.get("sourceUrl"))
    context.emit(org)

    rel = context.make(schema)
    rel.id = context.make_id(vessel.id, role, org.id)
    rel.add(from_prop, org.id)
    rel.add(to_prop, vessel.id)
    rel.add("role", role)
    h.apply_date(rel, "startDate", party["date"])
    context.emit(rel)


def url_id_of(url: str) -> str:
    """The trailing numeric id segment of a detail-page URL."""
    return url.rstrip("/").rsplit("/", 1)[-1]


def value_lines(el: Optional[Element]) -> list[str]:
    """Text runs of a value cell, split on <br> (one per text node), whitespace-squashed."""
    if el is None:
        return []
    texts = (t for t in el.itertext() if isinstance(t, str))
    return [s for s in (squash_spaces(t) for t in texts) if s]


def pop_text(pairs: dict[str, Element], label: str) -> Optional[str]:
    """Remove a label's value cell from the map and return its squashed text, if any."""
    el = pairs.pop(label, None)
    return h.element_text(el) or None if el is not None else None


def pop_prefixed(
    pairs: dict[str, Element], prefix: str
) -> tuple[Optional[Element], str]:
    """Remove the first label that starts with `prefix`; return its value cell and full label.

    Used where a label carries an appended status badge (e.g. a liquidation date) so its text
    no longer matches the bare field name exactly. The returned label lets the caller parse
    the badge; it is "" when nothing matched.
    """
    for label in list(pairs):
        if label.startswith(prefix):
            return pairs.pop(label), label
    return None, ""


def emit_succession(
    context: Context, predecessor_id: str, value_el: Element, page_url: str
) -> None:
    """Link a liquidated company to the legal successor named in its 'Assignee' row.

    The Assignee value links to another company page whose trailing id keys the successor the
    same way every company page does, so we reference it without a fetch: a stub carries the
    name (visible here) and merges by id with the successor's own crawl when a listing reaches
    it. Silently no-ops when the row has no link or name.
    """
    hrefs = h.xpath_strings(value_el, ".//a/@href")
    name = h.element_text(value_el)
    if not hrefs or name is None:
        return
    successor_id = context.make_slug("entity", url_id_of(hrefs[0]))
    if successor_id is None:
        return
    successor = context.make("LegalEntity")
    successor.id = successor_id
    successor.add("name", name)
    successor.add("sourceUrl", urljoin(page_url, hrefs[0]))
    context.emit(successor)

    rel = context.make("Succession")
    rel.id = context.make_id(predecessor_id, "succeeded by", successor_id)
    rel.add("predecessor", predecessor_id)
    rel.add("successor", successor_id)
    context.emit(rel)


def entity_label_map(doc: Element) -> dict[str, Element]:
    """Label -> value map for an entity (company) page: col-sm-8 value, prev-sibling label."""
    pairs: dict[str, Element] = {}
    for value_el in h.xpath_elements(doc, "//div[contains(@class,'col-sm-8')]"):
        label_el = value_el.getprevious()
        if label_el is None:
            continue
        label = h.element_text(label_el)
        if label:
            pairs.setdefault(label, value_el)
    return pairs


def crawl_entity_page(
    context: Context,
    url: str,
    *,
    program_key: Optional[str],
    topic: Optional[str],
) -> str:
    """Emit a LegalEntity from any company-type page (col-sm-8 layout); return its id.

    Shared by ships-company (descended inline from vessels) and every */companies-style
    listing section. Keyed `ua-ws-entity-<url_id>` to match the retiring API crawler.
    """
    doc = fetch_html(
        context, url, UNBLOCK, html_source="httpResponseBody", cache_days=CACHE_DAYS
    )
    pairs = entity_label_map(doc)

    entity_id = context.make_slug("entity", url_id_of(url))
    if entity_id is None:
        raise ValueError(f"Cannot build entity id from {url!r}")
    entity = context.make("LegalEntity")
    entity.id = entity_id
    name_el, name_label = pop_prefixed(pairs, COMPANY_NAME_LABEL)
    entity.add("name", value_lines(name_el))
    liquidated = LIQUIDATED_RE.search(name_label)
    if liquidated is not None and liquidated.group("date"):
        h.apply_date(entity, "dissolutionDate", liquidated.group("date"))
    entity.add(
        "alias", value_lines(pairs.pop("Abbreviated name of the legal entity", None))
    )
    entity.add("registrationNumber", pop_text(pairs, "Registration number"))
    entity.add("taxNumber", pop_text(pairs, "TIN"))
    entity.add("country", pop_text(pairs, "Country"))
    entity.add("address", pop_text(pairs, "Address"))
    if topic is not None:
        entity.add("topics", topic)
    entity.add("sourceUrl", url)

    # A page with no name/identifiers means the layout didn't match (e.g. a non-company
    # detail page or a dead id). Skip loudly rather than emit a hollow entity.
    if not entity.has("name") and not entity.has("registrationNumber"):
        context.log.warning("Entity page yielded no name/identifiers", url=url)
        return entity_id

    sanction = h.make_sanction(
        context, entity, key=program_key, program_key=program_key
    )
    sanction.set("programUrl", url)
    sanction.add("reason", pop_text(pairs, "Reasons"))
    context.emit(entity)
    context.emit(sanction)

    # Rostec holding chain: "Within the structure of Rostec" links self, then ancestry.
    # The 2nd link is the immediate parent → one Ownership edge (each company emits its own,
    # so the full tree is built incrementally), mirroring the API's rostec/structure.
    structure_el = pairs.pop("Within the structure of Rostec", None)
    if structure_el is not None:
        chain = h.xpath_strings(structure_el, ".//a/@href")
        parents = [
            m.group(1) for href in chain if (m := re.search(r"/rostec/(\d+)", href))
        ]
        if len(parents) >= 2:
            parent_id = context.make_slug("entity", parents[1])
            rel = context.make("Ownership")
            rel.id = context.make_id(parent_id, "subsidiary of", entity_id)
            rel.add("owner", parent_id)
            rel.add("asset", entity_id)
            rel.add("role", "subsidiary of")
            context.emit(rel)

    # Liquidated companies name their legal successor in an "Assignee" row.
    successor_el = pairs.pop("Assignee", None)
    if successor_el is not None:
        emit_succession(context, entity_id, successor_el, url)

    for label in pairs:
        if label not in COMPANY_SKIP_LABELS:
            context.log.warning("Unmapped entity label", label=label, url=url)
    return entity_id


def crawl_vessel_page(context: Context, url: str) -> None:
    doc = fetch_html(
        context, url, UNBLOCK, html_source="httpResponseBody", cache_days=CACHE_DAYS
    )
    pairs = label_map(doc)

    imo = pop_text(pairs, "IMO")
    vessel = context.make("Vessel")
    # Key by IMO; fall back to the source url id when there's no usable IMO at all, so a
    # missing/faulty IMO doesn't drop the vessel.
    vessel.id = h.make_vessel_imo_id(imo) or context.make_slug("vessel", url_id_of(url))
    vessel.add("imoNumber", imo)
    vessel.add("name", pop_text(pairs, "Vessel name"))
    vessel.add("flag", pop_text(pairs, "Flag (Current)"))
    vessel.add("mmsi", pop_text(pairs, "MMSI"))
    vessel.add("callSign", pop_text(pairs, "Call sign"))
    vessel.add("type", pop_text(pairs, "Vessel Type"))
    # The source relabelled the tonnage fields to carry the unit ("Gross tonnage (t)",
    # "DWT (t)"). Read both the old and unit-suffixed labels so neither drift trips the
    # unmapped-label warning; both feed the same numeric property.
    vessel.add("grossRegisteredTonnage", pop_text(pairs, "Gross tonnage"))
    vessel.add("grossRegisteredTonnage", pop_text(pairs, "Gross tonnage (t)"))
    vessel.add("deadweightTonnage", pop_text(pairs, "DWT"))
    vessel.add("deadweightTonnage", pop_text(pairs, "DWT (t)"))
    vessel.add("description", pop_text(pairs, "Vessel information"))
    for name in h.multi_split(pop_text(pairs, "Former ship names"), [SPLIT]):
        vessel.add("previousName", name)
    for flag in h.multi_split(pop_text(pairs, "Flags (former)"), [SPLIT]):
        vessel.add("pastFlags", flag)
    h.apply_date(vessel, "buildDate", pop_text(pairs, "Build year"))
    vessel.add("topics", "poi")
    vessel.add("sourceUrl", url)

    # ID continuity with the retiring API crawler is handled operationally: during the
    # transition both crawlers emit, and dedupe links old (ua-ws-*) and new (imo-*)
    # entities by their shared IMO / name. No programmatic rekey needed here.
    sanction = h.make_sanction(context, vessel, program_key="UA-WS-MARE")
    sanction.set("programUrl", url)
    sanction.add("sourceUrl", resource_links(doc))
    context.emit(vessel)
    context.emit(sanction)

    emit_party(
        context,
        vessel,
        pop_text(pairs, "Ship Owner (IMO / Country / Date)"),
        role="owner",
        schema="Ownership",
        from_prop="owner",
        to_prop="asset",
    )
    emit_party(
        context,
        vessel,
        pop_text(pairs, "Commercial ship manager (IMO / Country / Date)"),
        role="commerce manager",
        schema="UnknownLink",
        from_prop="subject",
        to_prop="object",
    )
    emit_party(
        context,
        vessel,
        pop_text(pairs, "Ship Safety Management Manager (IMO / Country / Date)"),
        role="security manager",
        schema="UnknownLink",
        from_prop="subject",
        to_prop="object",
    )

    pi_name = pop_text(pairs, "P&I Club")
    if pi_name is not None:
        club = context.make("Organization")
        club.id = context.make_slug("pi-club", pi_name)
        club.add("name", pi_name)
        club.add("sourceUrl", url)
        context.emit(club)
        rel = context.make("UnknownLink")
        rel.id = context.make_id(vessel.id, "P&I Club", club.id)
        rel.add("subject", club.id)
        rel.add("object", vessel.id)
        rel.add("role", "P&I Club")
        context.emit(rel)

    # The associated ships-company (shadow operator). Descend into it inline — companies
    # have no listing and are only reachable from the vessels that link to them.
    operator_el = pairs.pop(OPERATOR_LABEL, None)
    if operator_el is not None:
        for href in h.xpath_strings(operator_el, ".//a/@href"):
            match = re.search(r"/ships-company/(\d+)", href)
            if match is None:
                continue
            company_id = crawl_entity_page(
                context,
                f"{context.data_url}/transport/ships-company/{match.group(1)}",
                program_key="UA-WS-MARE",
                topic="poi",
            )
            rel = context.make("UnknownLink")
            rel.id = context.make_id(vessel.id, "operator", company_id)
            rel.add("subject", company_id)
            rel.add("object", vessel.id)
            context.emit(rel)

    # Fail loud if the page exposes labels we neither map nor knowingly skip.
    for label in pairs:
        if label not in VESSEL_SKIP_LABELS:
            context.log.warning("Unmapped vessel label", label=label, url=url)


def fetch_listing(context: Context, path: str, page: int) -> Element:
    url = f"{context.data_url}/{path}?page={page}&per-page={LISTING_PER_PAGE}"
    return fetch_html(
        context, url, UNBLOCK, html_source="httpResponseBody", cache_days=CACHE_DAYS
    )


def listing_max_page(doc: Element) -> int:
    """Highest page number linked from a listing's pagination control.

    Bounds the enumeration loop. Out-of-range pages do NOT return empty (they repeat
    content), so the upper bound must come from the pagination links, not from detecting
    an empty page.
    """
    pages = [1]
    for href in h.xpath_strings(doc, "//a/@href"):
        match = re.search(r"[?&]page=(\d+)", href)
        if match is not None:
            pages.append(int(match.group(1)))
    return max(pages)


def listing_detail_urls(doc: Element, base: str, path: str) -> list[str]:
    """The <base>/<path>/<id> detail URLs linked from one listing page, de-duplicated."""
    urls: list[str] = []
    seen: set[str] = set()
    pattern = re.compile(rf"/{re.escape(path)}/(\d+)")
    for href in h.xpath_strings(doc, "//a/@href"):
        match = pattern.search(href)
        if match is None:
            continue
        url = f"{base}/{path}/{match.group(1)}"
        if url not in seen:
            seen.add(url)
            urls.append(url)
    return urls


def crawl_listing(context: Context, path: str) -> Iterator[str]:
    """Yield every detail-page URL across a paginated listing section."""
    first = fetch_listing(context, path, 1)
    last = listing_max_page(first)
    context.log.info("Enumerating listing", path=path, pages=last)
    seen: set[str] = set()
    for page in range(1, last + 1):
        doc = first if page == 1 else fetch_listing(context, path, page)
        urls = listing_detail_urls(doc, context.data_url, path)
        if not urls:
            context.log.warning("Empty listing page", path=path, page=page)
        for url in urls:
            if url not in seen:
                seen.add(url)
                yield url


def person_label_map(doc: Element) -> dict[str, Element]:
    """Label -> value map for a person page; value is the label's next sibling.

    Two templates exist: the war sections use `col-md-4` label columns, the partner
    sanctions list uses `col-sm-4`. Both put the value in the next sibling.
    """
    pairs: dict[str, Element] = {}
    xpath = "//div[contains(@class,'col-md-4') or contains(@class,'col-sm-4')]"
    for label_el in h.xpath_elements(doc, xpath):
        label = h.element_text(label_el)
        value_el = label_el.getnext()
        if label and value_el is not None:
            pairs.setdefault(label, value_el)
    return pairs


def crawl_person_page(
    context: Context,
    url: str,
    *,
    program_key: Optional[str],
    topic: Optional[str],
) -> None:
    """Emit a Person from a persons-listing detail page (col-md-4 layout).

    The Name cell lists every script form (uk / ru / latin) as <br>-separated lines, so a
    single /en fetch yields all name variants — no per-language fetch needed.
    """
    doc = fetch_html(
        context, url, UNBLOCK, html_source="httpResponseBody", cache_days=CACHE_DAYS
    )
    pairs = person_label_map(doc)

    def take_lines(label: str) -> list[str]:
        return value_lines(pairs.pop(label, None))

    person_id = context.make_slug("person", url_id_of(url))
    if person_id is None:
        raise ValueError(f"Cannot build person id from {url!r}")
    person = context.make("Person")
    person.id = person_id
    add_person_names(person, take_lines("Name"))
    person.add("taxNumber", take_lines("TIN"))
    for label in PERSON_CITIZENSHIP_LABELS:
        person.add("citizenship", take_lines(label))
    for label in PERSON_POSITION_LABELS:
        for position in take_lines(label):
            person.add("position", position)

    # The birth field carries the date then the place, usually as separate <br> lines
    # ("30.11.1979" / "Moscow, RSFSR, USSR") but sometimes on one line. A line with a leading
    # date contributes birthDate (+ any trailing place); a line without one is a place —
    # except the first line, which is still the date slot (type.date lookups null odd values).
    for label in PERSON_DOB_LABELS:
        for index, line in enumerate(take_lines(label)):
            # A "DD.MM.YYYY - DD.MM.YYYY" range encodes birth and death dates.
            range_match = re.match(r"^([\d.]+)\s*[-–]\s*([\d.]+)$", line)
            if range_match is not None:
                h.apply_date(person, "birthDate", range_match.group(1))
                h.apply_date(person, "deathDate", range_match.group(2))
                continue
            match = re.match(r"^(\d{1,2}\.\d{1,2}\.\d{2,4}|\d{4})\b\s*(.*)$", line)
            if match is not None:
                h.apply_date(person, "birthDate", match.group(1))
                if match.group(2):
                    person.add("birthPlace", match.group(2))
            elif index == 0:
                h.apply_date(person, "birthDate", line)
            else:
                person.add("birthPlace", line)

    if topic is not None:
        person.add("topics", topic)
    person.add("sourceUrl", url)

    if not person.has("name"):
        context.log.warning("Person page yielded no name", url=url)
        return

    sanction = h.make_sanction(
        context, person, key=program_key, program_key=program_key
    )
    sanction.set("programUrl", url)
    sanction.add("reason", take_lines("Reasons"))

    for label in PERSON_LINK_LABELS:
        links_el = pairs.pop(label, None)
        if links_el is not None:
            hrefs = h.xpath_strings(links_el, ".//a/@href")
            sanction.add("sourceUrl", hrefs or value_lines(links_el))

    context.emit(person)
    context.emit(sanction)

    for label in pairs:
        if label not in PERSON_SKIP_LABELS:
            context.log.warning("Unmapped person label", label=label, url=url)


def crawl_shadow_fleet(context: Context) -> None:
    """Emit stub vessels tagged mare.shadow for every entry in the shadow-fleet listing.

    Each listing card carries the vessel's IMO right after its name, so membership is read
    from the listing alone (no per-vessel detail fetch). The stubs (id + imoNumber + topic)
    merge by IMO id with the full vessels from the ships pass — statements union — so no
    shadow state has to be threaded through the vessel parser.
    """
    path = "transport/shadow-fleet"
    first = fetch_listing(context, path, 1)
    last = listing_max_page(first)
    context.log.info("Enumerating shadow fleet", pages=last)
    for page in range(1, last + 1):
        doc = first if page == 1 else fetch_listing(context, path, page)
        cards = h.xpath_elements(
            doc,
            "//div[contains(@class,'col-md-6')][.//a[contains(@href,'/shadow-fleet/')]]",
        )
        for card in cards:
            labels = h.xpath_elements(card, ".//div[normalize-space(text())='IMO']")
            if not labels:
                continue
            value_el = labels[0].getnext()
            imo = h.element_text(value_el) if value_el is not None else ""
            # Stubs key purely by IMO to merge with the full vessel; skip cards with no IMO
            # (the shadow listing has no other id we could match on a ship page).
            vessel_imo_id = h.make_vessel_imo_id(imo)
            if vessel_imo_id is None:
                continue
            vessel = context.make("Vessel")
            vessel.id = vessel_imo_id
            vessel.add("imoNumber", imo)
            vessel.add("topics", "mare.shadow")
            context.emit(vessel)


def crawl_vessels(context: Context) -> None:
    for url in crawl_listing(context, "transport/ships"):
        crawl_vessel_page(context, url)


def crawl_tools(context: Context) -> None:
    """Tools factories (legal entities). The public /tools section lists equipment, not
    companies; each equipment page links its manufacturer at /tools/company/<id>. Descending
    into those is the only public path to the ~218 factory entities — the equipment itself is
    not modelled. The seen-set avoids re-descending shared manufacturers.
    """
    seen: set[str] = set()
    for equip_url in crawl_listing(context, "tools"):
        doc = fetch_html(
            context,
            equip_url,
            UNBLOCK,
            html_source="httpResponseBody",
            cache_days=CACHE_DAYS,
        )
        for href in h.xpath_strings(
            doc, "//a[contains(@href,'/tools/company/')]/@href"
        ):
            match = re.search(r"/tools/company/(\d+)", href)
            if match is None or match.group(1) in seen:
                continue
            seen.add(match.group(1))
            crawl_entity_page(
                context,
                f"{context.data_url}/tools/company/{match.group(1)}",
                program_key="UA-WS-MILIND",
                topic="poi",
            )


# (listing path, program_key, topic) for the company-type sections; keyed
# ua-ws-entity-<url_id>.
#
# The partner sanctions lists (sanctions/companies, sanctions/persons) get topic=None and
# program_key=None on purpose. They are GUR's re-publication of third-country sanctions
# (US/EU/UK, etc.), which we already ingest directly from those authorities. Tagging these
# entries with a risk topic would replicate that risk marking in a lagging form and from a
# less authoritative source, so we emit the entities (for cross-referencing) without
# re-asserting their sanctioned status.
ENTITY_SECTIONS = [
    ("kidnappers/companies", "UA-WS-KIDNAPPERS", "poi"),
    ("uav/companies", "UA-WS-UAVS", "poi"),
    ("stolen/companies", "UA-WS-STEALERS", "poi"),
    ("components/companies", "UA-WS-MILIND", "poi"),
    ("rostec", "UA-WS-MILIND", "poi"),
    ("sanctions/companies", None, None),
]
# Tools factories are crawled by crawl_tools (descended from equipment pages), not here,
# because the /tools listing is equipment rather than companies.

# (listing path, program_key, topic) for the person sections. sanctions/persons uses
# topic=None / program_key=None for the same reason as the entity partner-sanctions list
# above (re-published third-country sanctions, not re-marked here).
PERSON_SECTIONS = [
    ("kidnappers/persons", "UA-WS-KIDNAPPERS", "poi"),
    ("sport/persons", "UA-WS-ATHLETES", "poi"),
    ("propaganda/persons", "UA-WS-PROPAGANDISTS", "poi"),
    ("stolen/persons", "UA-WS-STEALERS", "poi"),
    ("executives", "UA-WS-EXECUTIVES", "poi"),
    ("sanctions/persons", None, None),
]


def crawl(context: Context) -> None:
    # Commit the cache after each section so the transaction doesn't have to run as long
    # as this long-running crawler.
    crawl_shadow_fleet(context)
    context.flush()
    crawl_vessels(context)
    context.flush()
    crawl_tools(context)
    context.flush()

    for path, program_key, topic in ENTITY_SECTIONS:
        for url in crawl_listing(context, path):
            crawl_entity_page(context, url, program_key=program_key, topic=topic)
        context.flush()

    for path, program_key, topic in PERSON_SECTIONS:
        for url in crawl_listing(context, path):
            crawl_person_page(context, url, program_key=program_key, topic=topic)
        context.flush()
