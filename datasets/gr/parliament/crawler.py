from urllib.parse import urljoin

from zavod import Context, helpers as h
from zavod.extract import zyte_api
from zavod.stateful.positions import categorise


def lookup_term_dates(context: Context, term: str) -> tuple[str | None, str | None]:
    dates_lookup_result = context.lookup("term", term)
    if dates_lookup_result is None:
        context.log.warning("Term not found in lookup", term=term)
        return None, None
    dates = dates_lookup_result.dates[0]
    start_date = dates.get("start_date")
    end_date = dates.get("end_date")
    return start_date, end_date


def crawl_row(
    context: Context, str_row: dict[str, str | None], id: str, name: str, url: str
) -> None:
    term = str_row.pop("term")
    assert term is not None, "Term is missing"
    start_date, end_date = lookup_term_dates(context, term)

    person = context.make("Person")
    person.id = context.make_slug(id)
    person.add("name", name)
    person.add("political", str_row.pop("party"))
    person.add("sourceUrl", url)
    # Being a Greek citizen is a constitutional requirement to be elected an MP.
    # Constitution of Greece, Article 55(1):
    # https://www.hellenicparliament.gr/UserFiles/f3c70a23-7696-49db-9148-f24dce6a27c8/001-156%20aggliko.pdf
    person.add("citizenship", "gr")

    position = h.make_position(
        context,
        name="Member of the Hellenic Parliament",
        country="gr",
        topics=["gov.legislative", "gov.national"],
        lang="eng",
        wikidata_id="Q18915989",
    )
    categorisation = categorise(context, position, default_is_pep=True)

    if categorisation.is_pep:
        occupancy = h.make_occupancy(
            context,
            person,
            position,
            start_date=start_date,
            end_date=end_date,
            categorisation=categorisation,
        )
        if occupancy:
            context.emit(position)
            context.emit(occupancy)
            context.emit(person)

    # The "description" column contains information about the date in the row, not the MP's role.
    context.audit_data(str_row, ["date", "costituency", "description"])


def crawl(context: Context) -> None:
    # The member dropdown is present on both the index and every detail page, so it
    # doubles as the Zyte unblock validator (a page that rendered has these options).
    mp_list_xpath = ".//select[@id='ctl00_ContentPlaceHolder1_dmps_mpsListId']/option"
    doc = zyte_api.fetch_html(
        context,
        context.data_url,
        unblock_validator=mp_list_xpath,
        html_source="httpResponseBody",
        cache_days=1,
    )
    # Get all <option> elements (skip the first 'Select Member')
    mp_option_els = h.xpath_elements(doc, mp_list_xpath)[1:]
    for el in mp_option_els:
        mp_id = el.get("value")
        mp_name = el.text
        assert mp_id is not None and mp_name is not None, "MP ID or name is missing"
        url = urljoin(context.data_url, f"?MpId={mp_id}")
        mp_page = zyte_api.fetch_html(
            context,
            url,
            unblock_validator=mp_list_xpath,
            html_source="httpResponseBody",
            cache_days=7,
        )
        tables = h.xpath_elements(mp_page, ".//table[@class='grid']")
        # The source renders a full page but omits the term-history table for MPs
        # with no record yet (e.g. elected but not yet seated). This is expected and
        # self-correcting: the table appears once the source populates it, so we log
        # at info level rather than warn, and move on without emitting the MP.
        if not tables:
            context.log.info("No table found for MP", mp_id=mp_id, url=url)
            continue
        assert len(tables) == 1, len(tables)
        table = tables[0]
        # The last row in the table has a colspan=5 and contains no data.
        # ignore_colspan={"5"} ensures it's skipped during parsing.
        for row in h.parse_html_table(table, ignore_colspan={"5"}):
            str_row = h.cells_to_str(row)
            crawl_row(context, str_row, mp_id, mp_name.strip(), url)
