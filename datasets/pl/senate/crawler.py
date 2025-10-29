import re
from typing import cast
from urllib.parse import urljoin

from datapatch import Lookup
from lxml.html import HtmlElement
from normality import squash_spaces
from zavod.shed import zyte_api
from zavod.stateful.positions import OccupancyStatus, categorise

from zavod import Context
from zavod import helpers as h

# These entries are explicitly skipped because they currently contain no details.
# However, the skip list is maintained so we can verify each skipped case,
# in case the relevant information exists elsewhere.
SKIP_DETAILS = [
    "https://www.senat.gov.pl/en/senators/lista-senatorow/senator,1075,11,magdalena-kochan.html"
]
# Born on 30 March 1973 in
DOB_REGEX = re.compile(r"\b[Bb]orn\s+on\s+(\d{1,2}\s+[A-Z][a-z]+\s+\d{4})\s+in\b")


def extract_dob(context, lookup: Lookup, text):
    """Try to extract a date from text using regex, fallback to context lookup, log if missing."""
    match = DOB_REGEX.search(text)
    if match:
        return match.group(1)
    # Fallback to context lookup
    res = lookup.get_value(text)
    if res is not None:
        return res

    context.log.warning(
        f"DoB regex lookup from biography failed and not found in {lookup}.",
        biography=text,
    )
    return None


def crawl(context: Context) -> None:
    senator_container_xpath = ".//div[@class='senator-kontener']"
    doc = zyte_api.fetch_html(
        context,
        context.data_url,
        unblock_validator=senator_container_xpath,
        cache_days=1,
    )
    for row in doc.findall(senator_container_xpath):
        link = row.find(".//a")
        if link is None:
            continue
        assert link.text is not None, f"Missing name for: {link.get('href')}"
        name = link.text.strip()
        href = link.get("href")
        assert href is not None, "Missing href"
        assert name, f"Missing name for: {href}"
        url = urljoin(context.data_url, href)
        pep_doc = zyte_api.fetch_html(
            context,
            url,
            unblock_validator=".//div[@class='description']",
            cache_days=1,
        )
        description_el = cast(HtmlElement, pep_doc.find(".//div[@class='description']"))
        description = squash_spaces(description_el.text_content().strip())
        if not description and url not in SKIP_DETAILS:
            context.log.warning(f"Missing biography for {name}.", url=url)
        if description:
            dob = extract_dob(context, context.get_lookup("birth_dates"), description)

        entity = context.make("Person")
        entity.id = context.make_id(name, dob)
        entity.add("name", name)
        entity.add("sourceUrl", url)
        entity.add("notes", description)
        h.apply_date(entity, "birthDate", dob)

        position = h.make_position(
            context,
            name="Member of the Senate of Poland",
            wikidata_id="Q81747225",
            country="pl",
            topics=["gov.legislative", "gov.national"],
            lang="eng",
        )
        categorisation = categorise(context, position, is_pep=True)
        if not categorisation.is_pep:
            continue

        occupancy = h.make_occupancy(
            context,
            person=entity,
            position=position,
            no_end_implies_current=False,
            categorisation=categorisation,
            # Data source indicates they hold the position now
            status=OccupancyStatus.CURRENT,
        )
        if occupancy is not None:
            context.emit(occupancy)
            context.emit(position)
            context.emit(entity)
