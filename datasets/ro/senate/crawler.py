import re
from urllib.parse import urljoin
from lxml.etree import _Element

from zavod import Context, helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

# A senator card from the no-photos layout, e.g.
# "ABRUDEAN Mircea Data nasterii: 23.07.1984 Circumscripţia electorală nr.13 Cluj ..."
DOB_RE = re.compile(r"Data nasterii:\s*(\d{2}\.\d{2}\.\d{4})")
PARLAMENTAR_ID_RE = re.compile(r"ParlamentarID=([a-f0-9-]+)")
# "Senator ales ... în data de 01.12.2024, validat în data de 21.12.2024".
# Replacement senators are not directly elected and show "în data de -, validat
# în data de 21.10.2024" - i.e. an election date may be absent but a validation
# date is present.
ELECTION_RE = re.compile(r"ales\b.*?în data de\s+(\d{2}\.\d{2}\.\d{4})\s*,", re.DOTALL)
VALIDATION_RE = re.compile(r"validat\b[^,]*?(\d{2}\.\d{2}\.\d{4})", re.DOTALL)
# Legislature labels look like "2024-2028".
LEGISLATURE_LABEL_RE = re.compile(r"^(\d{4})-(\d{4})$")

TOPICS = ["gov.legislative", "gov.national"]


def parse_form_fields(doc: _Element) -> dict[str, str]:
    """Collect the ASP.NET form state needed to round-trip a postback: all hidden
    inputs plus the currently selected (or first) option of every dropdown."""
    fields: dict[str, str] = {}
    for inp in h.xpath_elements(doc, "//input[@type='hidden']"):
        name = inp.get("name")
        if name is not None:
            fields[name] = inp.get("value") or ""
    for select in h.xpath_elements(doc, "//select"):
        name = select.get("name")
        if name is None:
            continue
        options = h.xpath_elements(select, "./option[@selected]")
        if not len(options):
            options = h.xpath_elements(select, "./option[1]")
        if len(options):
            fields[name] = options[0].get("value") or ""
    return fields


def post_form(context: Context, fields: dict[str, str], event_target: str) -> _Element:
    """Submit the senator-register form as a postback targeting `event_target`."""
    data = dict(fields)
    data["__EVENTTARGET"] = event_target
    data["__EVENTARGUMENT"] = ""
    # Force the no-photos layout so the roster carries birth dates.
    # Checked by default ("Fără poze" / without photos). It must be re-submitted on
    # every postback, otherwise the roster renders a photo layout that omits the
    # birth date and constituency we rely on.
    data["ctl00$B_Center$checkPoze"] = "on"
    # Postbacks are not cached: the VIEWSTATE is single-use and tied to the
    # session cookie established by the initial GET, so a stale cached copy
    # would be rejected on a later run.
    return context.fetch_html(context.data_url, method="POST", data=data)


def fetch_roster(
    context: Context, base_fields: dict[str, str], legislature_id: str
) -> dict[str, tuple[str, str | None]]:
    """Return the unique senators of one legislature as {guid: (name, dob)}.

    The site needs a two-step postback: first the legislature change (which
    repopulates the session dropdown), then the search that lists the roster.
    Each senator is listed twice on the page, so we deduplicate by GUID. The
    birth date is absent for some senators (the card shows the placeholder
    "dd/MM/yyyy"), in which case `dob` is None.
    """
    step1_fields = dict(base_fields)
    step1_fields["ctl00$B_Center$ddLegislatura"] = legislature_id
    doc1 = post_form(context, step1_fields, "ctl00$B_Center$ddLegislatura")

    step2_fields = parse_form_fields(doc1)
    step2_fields["ctl00$B_Center$ddLegislatura"] = legislature_id
    roster = post_form(context, step2_fields, "ctl00$B_Center$btnCauta")

    senators: dict[str, tuple[str, str | None]] = {}
    for card in h.xpath_elements(roster, "//div[@class='new-card-without-pics']"):
        links = h.xpath_elements(card, ".//a[contains(@href, 'FisaSenator')]")
        if not len(links):
            continue
        href = links[0].get("href")
        name = h.element_text(links[0])
        id_match = PARLAMENTAR_ID_RE.search(href or "")
        if id_match is None or not name:
            context.log.warning("Could not parse senator card", name=name, href=href)
            continue
        dob_match = DOB_RE.search(h.element_text(card))
        dob = dob_match.group(1) if dob_match is not None else None
        senators[id_match.group(1)] = (name, dob)
    return senators


def crawl_detail(
    context: Context, detail_url: str
) -> tuple[str | None, str | None, str | None]:
    """Fetch a per-term senator page; return (election_date, validation_date, party)."""
    doc = context.fetch_html(detail_url, cache_days=30)
    text = h.element_text(doc)
    election_match = ELECTION_RE.search(text)
    election_date = election_match.group(1) if election_match is not None else None
    validation_match = VALIDATION_RE.search(text)
    validation_date = (
        validation_match.group(1) if validation_match is not None else None
    )
    if election_date is None and validation_date is None:
        context.log.warning("No mandate date found on detail page", url=detail_url)

    party: str | None = None
    label_spans = h.xpath_elements(
        doc, "//span[contains(text(), 'Grupul parlamentar:')]"
    )
    if len(label_spans):
        tail = label_spans[0].tail
        party = tail.strip() if tail is not None else None
    return election_date, validation_date, party


def crawl_senator(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    term_start_year: str,
    term_end_year: str,
    is_current: bool,
    guid: str,
    name: str,
    dob: str | None,
) -> None:
    detail_url = urljoin(context.data_url, f"FisaSenator.aspx?ParlamentarID={guid}")
    election_date, validation_date, party = crawl_detail(context, detail_url)

    person = context.make("Person")
    person.id = context.make_id(name, dob, guid)
    person.add("name", name)
    h.apply_date(person, "birthDate", dob)
    # Senators must be Romanian citizens: Constitution of Romania Art. 16(3) and
    # Art. 37(1). https://www.wipo.int/wipolex/edocs/lexdocs/laws/en/ro/ro021en.html
    person.add("citizenship", "ro")
    person.add("sourceUrl", detail_url)

    # The mandate starts when validated (fall back to election or term start).
    start_date = validation_date or election_date
    if start_date is None:
        start_date = term_start_year

    end_date = None if is_current else term_end_year
    occupancy = h.make_occupancy(
        context,
        person=person,
        position=position,
        no_end_implies_current=True,
        start_date=start_date,
        end_date=end_date,
        election_date=election_date,
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    occupancy.add("politicalGroup", party)
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url)
    options = h.xpath_elements(doc, "//select[contains(@name, 'ddLegislatura')]/option")
    # Freshness guard: the site has exposed every legislature since 1990. A
    # shrinking list means the page structure changed and needs revisiting.
    if len(options) < 10:
        raise ValueError(f"Expected >= 10 legislatures, found {len(options)}")

    base_fields = parse_form_fields(doc)
    position = h.make_position(
        context,
        name="Member of the Senate of Romania",
        country="ro",
        topics=TOPICS,
        wikidata_id="Q19938957",
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    for option in options:
        legislature_id = option.get("value")
        label = h.element_text(option)
        if legislature_id is None or not label:
            context.log.warning("Skipping legislature without id/label", label=label)
            continue
        is_current = option.get("selected") is not None
        label_match = LEGISLATURE_LABEL_RE.match(label)
        if label_match is None:
            context.log.warning("Unexpected legislature label", label=label)
            continue
        term_start_year, term_end_year = label_match.group(1), label_match.group(2)
        if not is_current and f"{term_end_year}-12-31" < h.earliest_term_start(TOPICS):
            context.log.info(
                "Skipping legislature outside the coverage window", label=label
            )
            continue
        senators = fetch_roster(context, base_fields, legislature_id)
        context.log.info(
            "Crawling legislature", label=label, count=len(senators), current=is_current
        )
        if len(senators) < 50:
            raise ValueError(
                f"Implausibly few senators ({len(senators)}) for legislature {label}"
            )
        for guid, (name, dob) in senators.items():
            crawl_senator(
                context,
                position,
                categorisation,
                term_start_year,
                term_end_year,
                is_current,
                guid,
                name,
                dob,
            )
