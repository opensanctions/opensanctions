from zavod import Context, helpers as h
from zavod.extract import zyte_api
from zavod.stateful.positions import categorise


def extract_term_dates(context: Context, profile_url: str) -> tuple:
    term_dates = "//h4[contains(text(), 'CV :')]"
    pep_doc = zyte_api.fetch_html(
        context,
        profile_url,
        unblock_validator=term_dates,
        absolute_links=True,
        cache_days=4,
    )
    term_elem = h.xpath_elements(pep_doc, term_dates, expect_exactly=1)[0]
    term = h.element_text(term_elem)
    res = context.lookup("term_dates", term, warn_unmatched=True)
    if res:
        return res.start_date, res.end_date
    else:
        return None, None


def crawl(context: Context) -> None:
    unblock_validator = "//table[@width='100%']"
    doc = zyte_api.fetch_html(
        context,
        context.data_url,
        unblock_validator=unblock_validator,
        absolute_links=True,
        cache_days=4,
    )
    table = h.xpath_elements(doc, "//table[@width='100%']", expect_exactly=1)[0]

    for row in h.xpath_elements(table, ".//tr[td[@class='td1' or @class='td0']]"):
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

        start_date, end_date = extract_term_dates(context, profile_url)
        occupancy = h.make_occupancy(
            context,
            person=entity,
            position=position,
            start_date=start_date,
            end_date=end_date,
            categorisation=categorisation,
        )
        if occupancy is not None:
            context.emit(entity)
            context.emit(position)
            context.emit(occupancy)
