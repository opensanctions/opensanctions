from collections import defaultdict
from datetime import datetime
import os
from pprint import pprint
import re
from urllib.parse import urlparse
from normality import collapse_spaces
from requests import HTTPError

from zavod import Context, helpers as h
from zavod.stateful.positions import OccupancyStatus, categorise

PARTY_NAMES = defaultdict(int)
PARTY_REGEX = re.compile(r"(\([\w ]+\)|, [\w ]+$)")
MAX_POSITION_NAME_LENGTH = 120


def extract_dates(context: Context, url, el):
    active_date_el = el.find('.//span[@class="aktiv"]')
    inactive_dates_el = el.find('.//span[@class="inaktiv"]')
    start_date = None
    end_date = None
    assume_current = False
    if active_date_el is not None:
        start_date = active_date_el.text_content().replace("seit ", "")
        assume_current = True
    elif inactive_dates_el is not None:
        inactive_dates = inactive_dates_el.text_content().replace("ab ", "")
        if " - " in inactive_dates:
            start_date, end_date = inactive_dates.split(" - ")
            start_date = start_date.strip()
            end_date = end_date.strip()
        elif inactive_dates:
            start_date = inactive_dates
            end_date = None
    else:
        # Some date elements are not semantically marked up with "aktiv" or "inaktiv".
        # Instead, they're nested in layout-only HTML structure.
        #
        # This fallback assumes the following structure inside the first child of `funktionszeile`:
        #   <div>
        #     <div>…</div>
        #     <div>…</div>
        #     <div>? - 2021</div>  ← date range
        #   </div>
        children = list(el)
        assert len(children) == 2, (
            f"Expected funktionseile to have 2 children, got {len(children)}, url: {url}"
        )
        # take the first child
        first_child = children[0]
        first_child_children = list(first_child)
        assert len(first_child_children) == 3, (
            f"Expected first child to have 3 children, got {len(first_child_children)}, url: {url}"
        )
        # take the third child (this has dates)
        third_child = first_child_children[2]
        date_string = third_child.text_content().strip()
        if " - " in date_string:
            start_date, end_date = date_string.split(" - ")
            start_date = start_date.strip()
            end_date = end_date.strip()
        elif date_string:
            start_date = date_string
            end_date = None
        else:
            context.log.warn(
                "Can't parse date for mandate", url=url, text=el.text_content().strip()
            )
    return start_date, end_date, assume_current


def strip_party_name(position_name):
    party_match = PARTY_REGEX.search(position_name)
    if party_match:
        party_name = party_match.group(0).strip()
        if (
            len(party_name) <= 19
            or "team" in party_name.lower()
            or "partei" in party_name.lower()
        ):
            position_name = position_name.replace(party_name, "").strip()
            PARTY_NAMES[party_name] += 1
    return collapse_spaces(position_name)


def crawl_sources(context, entity, el):
    for source_el in el.xpath('.//p[contains(@class, "source")]'):
        text = collapse_spaces(source_el.text_content())
        entity.add("description", text)
        for link in source_el.xpath(".//a"):
            entity.add("sourceUrl", link.get("href"))


def crawl_mandate(context, url, person, el):
    """Returns true if dates could be parsed for a PEP position."""
    start_date, end_date, assume_current = extract_dates(context, url, el)

    position_name_el = el.xpath('.//div[contains(@class, "funktionsText")]')
    if position_name_el:
        position_name_el = position_name_el[0]
    else:
        # I think this is a copy of the markup for mobile
        return

    # Remove source and keep it, we'll use it later
    source_el = position_name_el.xpath('.//div[contains(@class, "source")]')[0]
    position_name_el.remove(source_el)

    # Add line breaks so we can split on this
    for br in position_name_el.xpath(".//br"):
        br.tail = br.tail + "\n" if br.tail else "\n"
    position_name = position_name_el.text_content().strip()
    position_parts = position_name.split("\n")
    position_name = position_parts[0]
    position_name = strip_party_name(position_name)
    res = context.lookup("position", position_name)
    if res:
        position_name = res.name

    if len(position_name) > MAX_POSITION_NAME_LENGTH and not res:
        context.log.warning(
            "Unexpectedly long position name, possibly capturing more than the position name.",
            url=url,
            name=position_name,
        )
        return

    position = h.make_position(context, position_name, country="at", lang="deu")
    categorisation = categorise(context, position, is_pep=True)
    if not categorisation.is_pep:
        return

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        start_date=start_date,
        end_date=end_date,
        no_end_implies_current=assume_current,
        categorisation=categorisation,
    )

    if occupancy is not None:
        crawl_sources(context, occupancy, source_el)
        occupancy.add("description", position_parts[1:], lang="deu")
        context.emit(person)
        context.emit(position)
        context.emit(occupancy)

        return start_date or end_date


def crawl_title(context, url, person, el):
    h1 = el.find(".//h1")
    position_name = h1.getnext().text_content().strip()
    position = h.make_position(context, position_name, country="at", lang="deu")
    categorisation = categorise(context, position, is_pep=None)
    if not categorisation.is_pep:
        if categorisation.is_pep is None:
            context.log.warning(
                "Uncategorised position", position=position_name, url=url
            )
        return
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        no_end_implies_current=False,
        categorisation=categorisation,
        status=OccupancyStatus.UNKNOWN,
    )
    context.log.info("Using position from title section", url=url)
    if occupancy is not None:
        context.emit(person)
        context.emit(position)
        context.emit(occupancy)


def crawl_item(url_info_page: str, context: Context):
    try:
        info_page = context.fetch_html(url_info_page, cache_days=1, absolute_links=True)
    except HTTPError as e:
        if e.response.status_code == 503:
            context.log.info("HTTP 503 error, skipping", url=url_info_page)
            # Rely on data assertions to prevent export if there are too many of these.
            return
        raise

    first_name = info_page.findtext(".//span[@itemprop='http://schema.org/givenName']")
    last_name = info_page.findtext(".//span[@itemprop='http://schema.org/familyName']")

    person = context.make("Person")
    id = os.path.basename(urlparse(url_info_page).path)
    person.id = context.make_slug(id)

    h.apply_name(person, first_name=first_name, last_name=last_name, lang="deu")
    person.add("sourceUrl", url_info_page)
    birth_date_in_german = info_page.findtext(".//span[@itemprop='birthDate']")
    if birth_date_in_german:
        h.apply_date(person, "birthDate", birth_date_in_german)
    person.add(
        "birthPlace", info_page.findtext(".//span[@itemprop='birthPlace']"), lang="deu"
    )
    email = info_page.findtext(".//a[@itemprop='http://schema.org/email']")
    person.add("email", email.strip() if email else None)
    person.add(
        "phone", info_page.findtext(".//a[@itemprop='http://schema.org/telephone']")
    )

    parsed_some_mandatee_date = False
    # Only parse the mandate rows that are shown on desktop
    # i.e. skip the rows with same content but different layout for mobile ("d-block d-lg-none")
    for row in info_page.xpath(
        '//div[@id="mandate"]//div[contains(@class, "funktionszeile") and not(contains(@class, "d-block d-lg-none"))]'
    ):
        if crawl_mandate(context, url_info_page, person, row):
            parsed_some_mandatee_date = True
    if not parsed_some_mandatee_date:
        # Fall back to parsing a position from their title in the header
        header_el = info_page.xpath('//div[contains(@class, "dossierKopf")]')[0]
        crawl_title(context, url_info_page, person, header_el)


def crawl(context: Context):
    response = context.fetch_html(context.data_url)

    if datetime.now().isoformat() > "2026-05-21":
        # Since this is an unofficial source, check periodically whether the site is maintained.
        # As of 2025-05-22, there is a "NEUES AUS DEN DOSSIERS" section on the front page
        # https://www.meineabgeordneten.at/ that showcases recently updated entities.
        # As of 2026-05-22, the showcased dossier contains an update from 2025-03-03, so this site
        # is still being maintained.
        context.log.warning("Verify again that the site is kept up to date")

    # XPath to the url for the pages of each politician
    xpath_politician_page = (
        '//div[contains(@class, "abgeordneter")][contains(@class, "row")]/*/a/@href'
    )

    for item in response.xpath(xpath_politician_page):
        crawl_item(item, context)

    pprint(PARTY_NAMES)
