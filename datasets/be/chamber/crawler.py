from datetime import datetime

from zavod import Context, helpers as h
from zavod.extract import zyte_api
from zavod.stateful.positions import categorise, EXTENDED_AFTER_OFFICE

CUTOFF_YEAR = datetime.now().year - (EXTENDED_AFTER_OFFICE // 365)


def get_latest_terms(context: Context, term: str) -> tuple[int | None, int | None]:
    res = context.lookup("term_dates", term, warn_unmatched=True)
    if res:
        return int(res.start_date), int(res.end_date) if res.end_date else None
    else:
        return None, None


def crawl_persons(context: Context, row, start_date: int, end_date: int) -> None:
    cells = h.xpath_elements(row, ".//td[@class='td1' or @class='td0']")
    name = h.xpath_strings(cells[0], ".//b/text()", expect_exactly=1)[0].strip()
    profile_url = h.xpath_strings(cells[0], ".//a/@href", expect_exactly=1)[0]
    member_key = (
        profile_url.split("key=")[1].split("&")[0]
        if "key=" in profile_url
        else None
    )

    group_texts = h.xpath_strings(cells[1], ".//a/text()")
    political_group = group_texts[0].strip() if group_texts else ""

    entity = context.make("Person")
    entity.id = context.make_id(name, member_key)
    entity.add("name", name)
    entity.add("political", political_group)
    entity.add("sourceUrl", profile_url)
    entity.add("citizenship", "be")

    position = h.make_position(
        context,
        name="Member of the Chamber of Representatives of Belgium",
        wikidata_id="Q15705021",
        country="be",
        topics=["gov.legislative", "gov.national"],
        lang="eng",
    )
    categorisation = categorise(context, position, is_pep=True)
    if not categorisation.is_pep:
        return
    
    occupancy = h.make_occupancy(
        context,
        person=entity,
        position=position,
        start_date=str(start_date),
        end_date=str(end_date),
        categorisation=categorisation,
    )
    if occupancy is not None:
        context.emit(entity)
        context.emit(position)
        context.emit(occupancy)


def crawl(context: Context) -> None:
    unblock_validator = "//table[@width='100%']"
    # Fetch the main page with all legislature terms
    doc = zyte_api.fetch_html(
        context,
        context.data_url,
        unblock_validator=unblock_validator,
        absolute_links=True,
        cache_days=4,
    )
    # Extract legislature terms from menu
    terms = []
    seen_urls = set()
    for link in doc.findall('.//div[@class="menu"]//a'):
        url = link.get('href')
        text = h.element_text(link).strip()
        
        # Only keep entries with explicit date format "XX (YYYY-YYYY)"
        # The first link "Actuels" (current members) duplicates the most recent term,
        # so we filter for entries with parentheses to get unique dated terms.
        if text and '(' in text and ')' in text and url not in seen_urls:
            seen_urls.add(url)
            terms.append({'url': url, 'text': text}) 
    # Process each legislature term
    for term in terms:
        start_date, end_date = get_latest_terms(context, term['text'])
        # Skip terms that ended before our cutoff year
        if not (start_date and end_date and end_date >= CUTOFF_YEAR):
            context.log.info(f"Skipping old term {term['text']} (ended {end_date})")
            continue
        # Fetch the member list page for this term
        term_doc = zyte_api.fetch_html(
            context,
            term['url'],
            unblock_validator=unblock_validator,
            absolute_links=True,
            cache_days=4,
        )
        # Extract and process all members from the table
        table = h.xpath_elements(term_doc, "//table[@width='100%']", expect_exactly=1)[0]
        for row in h.xpath_elements(table, ".//tr[td[@class='td1' or @class='td0']]"):
            crawl_persons(context, row, start_date, end_date)
