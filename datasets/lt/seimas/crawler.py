from datetime import datetime
import re
from normality import collapse_spaces
from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.extract import zyte_api
from zavod.stateful.positions import categorise, get_after_office
from zavod.util import Element

POSITION_TOPICS = ["gov.legislative", "gov.national"]
CUTOFF_DATE = (datetime.now() - get_after_office(POSITION_TOPICS)).year


def make_seimas_position(context: Context) -> Entity:
    # A single shared Position for the whole dataset: every member holds the same
    # role. Passing the Wikidata QID makes it the position's entity ID, so this
    # must be identical across the current and historical crawlers or they'd
    # split into two entities for the same role.
    return h.make_position(
        context,
        name="Member of the Seimas",
        wikidata_id="Q18507240",
        country="lt",
        topics=POSITION_TOPICS,
        lang="eng",
    )


URL_PREV_SEIMAS = "https://www.lrs.lt/sip/portal.show?p_r=35357&p_k=2"


def get_element_text(
    doc: Element,
    xpath_value: str,
    to_remove: list[str] | None = None,
    position: int = 0,
) -> str:
    elements = h.xpath_elements(doc, xpath_value)
    element_text = h.element_text(elements[position]) if elements else ""

    for string in to_remove or []:
        element_text = element_text.replace(string, "")

    return collapse_spaces(element_text.strip()) or ""


def get_occupany_dates(tenure: str) -> tuple[str, str]:
    tenure_year = tenure.split()[-1]
    start_year, end_year = tenure_year.split("-")

    return start_year, end_year


def cell_lines(cell: Element) -> list[str]:
    """Split a table cell (or row) into its non-empty text lines.

    The biography tables are Word-exported, so labels/values are separated
    inconsistently by block boundaries (<p>/<div>) or <br> tags. Both produce
    separate text nodes, so reading text nodes in document order recovers the
    lines regardless of which separator a particular member's page used. We
    join on these rather than the cell's string value because text_content()
    concatenates block/column text without a separator (e.g. "2005–2009" would
    glue onto the following text).
    """
    return [line.strip() for line in h.xpath_strings(cell, ".//text()") if line.strip()]


def crawl_member_bio(context: Context, url: str) -> bool:
    doc = zyte_api.fetch_html(
        context,
        url,
        unblock_validator='//div[@class="sn_narys_vardas_title"]',
        html_source="httpResponseBody",
        cache_days=1,
        absolute_links=True,
    )

    # "frakcija" is the member's single current parliamentary group. This page
    # always renders the person's latest term, so the faction lines up with the
    # (latest-term) occupancy we build below and belongs there as politicalGroup
    # — a faction, not general party membership. Empty once they stop sitting,
    # hence the None check.
    group_list = h.xpath_strings(
        doc, '//div[@class="frakcija"]/a[contains(@class, "smn-frakcija link")]/text()'
    )
    political_group = group_list[0] if group_list else None

    # The biography table's first two rows always repeat the birth date and
    # place, which we capture as their own properties. Skip them so the
    # biography starts at the narrative content instead of a redundant prefix.
    bio_lines: list[str] = []
    for row in h.xpath_elements(doc, '//div[@id="sn_vidines_biografija"]//table//tr'):
        lines = cell_lines(row)
        if lines and lines[0].lower() in ("date of birth", "place of birth"):
            continue
        bio_lines.extend(lines)
    bio = collapse_spaces(" ".join(bio_lines))

    person_name = get_element_text(doc, '//div[@class="sn_narys_vardas_title"]')
    date_of_birth = get_element_text(
        doc,
        '//tr[.//*[contains(.//text(), "Date of birth")]]//td//p|//p[.//*[contains(.//text(), "Date of birth")]]',
        to_remove=["Date of birth", ","],
        position=-1,
    )
    place_of_birth = get_element_text(
        doc,
        '//tr[.//*[contains(.//text(), "Place")]][.//*[contains(.//text(), "birth")]]//td//p|//p[.//*[contains(.//text(), "Place of birth")]]',
        to_remove=["Place of birth"],
        position=-1,
    )

    tenure = get_element_text(doc, '//div[@class="kadencija"]')
    # Parliamentary term dates are not necessarily the same as candidate's occupancy dates
    period_start, period_end = get_occupany_dates(tenure)

    person = context.make("Person")
    person.id = context.make_slug(person_name)
    person.add("citizenship", "lt")
    person.add("name", person_name)
    person.add("biography", bio)
    person.add("sourceUrl", url)

    if date_of_birth:
        h.apply_date(person, "birthDate", date_of_birth)
    person.add("birthPlace", place_of_birth)

    position = make_seimas_position(context)

    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return False

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        period_start=period_start,
        period_end=period_end,
        categorisation=categorisation,
    )
    if occupancy is None:
        return False
    occupancy.add("politicalGroup", political_group)
    context.emit(person)
    context.emit(position)
    context.emit(occupancy)
    return True


# Occupancy tenure line, e.g. "Member of the Seimas from 2012-11-16 till
# 2016-11-14." (recent legacy pages, ISO dates) or "... from 15/11/2004 till
# 17/11/2008." (older ones, DD/MM/YYYY). Dates are parsed via dataset formats.
TENURE_RE = re.compile(r"Member of the Seimas from\s+([\d/-]+)\s+till\s+([\d/-]+)")


def get_birth_details(doc: Element) -> tuple[str | None, str | None]:
    """Extract (birth date, birth place) from a legacy member's biography table.

    The table is a single two-column row: the first cell stacks field labels,
    the second the corresponding values in the same order. Date and place of
    birth are always the first two entries, so we read them positionally, but
    only after confirming the first two labels are indeed date/place of birth
    — otherwise we'd risk pairing values against the wrong labels.
    """
    labels = h.xpath_elements(doc, '//b[normalize-space(.)="Date of birth"]')
    if not labels:
        return None, None

    label_cell = h.xpath_element(labels[0], "ancestor::td[1]")
    value_cells = h.xpath_elements(label_cell, "following-sibling::td[1]")
    if not value_cells:
        return None, None

    label_lines = cell_lines(label_cell)
    value_lines = cell_lines(value_cells[0])
    if len(label_lines) < 2 or len(value_lines) < 2:
        return None, None
    assert label_lines[0] == "Date of birth", label_lines
    # Capitalisation of "Place of birth" varies between members.
    assert label_lines[1].lower() == "place of birth", label_lines
    return value_lines[0], value_lines[1]


def crawl_old_member_bio(context: Context, url: str) -> bool:
    doc = zyte_api.fetch_html(
        context,
        url,
        # Match on the tenure line (present on every bio) rather than a static
        # container, which differs between the legacy layouts.
        unblock_validator='//*[contains(normalize-space(.), "Member of the Seimas from")]',
        html_source="httpResponseBody",
        cache_days=1,
        absolute_links=True,
    )

    # The rendered page title is "Members of the Seimas - <name>".
    title = h.element_text(h.xpath_element(doc, "//title"))
    person_name = title.rsplit(" - ", 1)[-1].strip()
    assert person_name, url

    tenure_match = TENURE_RE.search(h.element_text(doc))
    assert tenure_match is not None, url
    start_date, end_date = tenure_match.groups()

    # Birth details are absent on the oldest legislatures' pages, so extract
    # them leniently and emit the member even when they're missing.
    date_of_birth, place_of_birth = get_birth_details(doc)

    person = context.make("Person")
    person.id = context.make_slug(person_name)
    person.add("name", person_name)
    person.add("birthPlace", place_of_birth)
    if date_of_birth is not None:
        h.apply_date(person, "birthDate", date_of_birth)

    # Parliamentary group memberships for this term, read as
    # "<group>, <role> ( <dates> )"; keep the group name. These are factions
    # within the body, not general party membership, so they go on the
    # occupancy as politicalGroup below.
    political_groups = set()
    group_items = h.xpath_elements(
        doc,
        '//b[normalize-space(.)="Political Groups of the Seimas"]/following-sibling::ul[1]/li',
    )
    for item in group_items:
        group = h.element_text(item).split(",")[0].strip()
        if group:
            political_groups.add(group)

    person.add("sourceUrl", url)
    person.add("citizenship", "lt")

    position = make_seimas_position(context)

    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return False

    occupancy = h.make_occupancy(
        context,
        person=person,
        position=position,
        start_date=start_date,
        end_date=end_date,
        categorisation=categorisation,
    )
    if occupancy is None:
        return False
    occupancy.add("politicalGroup", sorted(political_groups))
    context.emit(person)
    context.emit(position)
    context.emit(occupancy)
    return True


def crawl(context: Context) -> None:
    ### === crawl current legislature === ###
    members_list_validator = (
        '//div[contains(@class,"list-member")]//a[contains(@class, "smn-name")]'
    )
    doc = zyte_api.fetch_html(
        context,
        context.data_url,
        unblock_validator=members_list_validator,
        html_source="httpResponseBody",
        cache_days=1,
        absolute_links=True,
    )

    current_member_urls = h.xpath_strings(doc, members_list_validator + "/@href")
    context.log.info("Current legislature", member_link_count=len(current_member_urls))
    emitted = 0
    for anchor_url in current_member_urls:
        if crawl_member_bio(context, anchor_url):
            emitted += 1
    context.log.info("Current legislature", emitted_count=emitted)

    ### === crawl older legislatures === ###
    # navigate the landing page that contains a table with older seimas
    doc_landing_older_seimas = zyte_api.fetch_html(
        context,
        URL_PREV_SEIMAS,
        unblock_validator='//div[contains(@class, "rubrika-kvadratai-item")]',
        html_source="httpResponseBody",
        cache_days=1,
        absolute_links=True,
    )
    older_seimas_table = h.xpath_elements(
        doc_landing_older_seimas, '//div[contains(@class, "rubrika-kvadratai-item")]'
    )
    assert older_seimas_table is not None

    for seimas in older_seimas_table:
        seimas_el = h.xpath_element(seimas, ".//a")
        seimas_label = h.element_text(seimas_el)
        seimas_dates_match = re.search(r"\(\d{4}[–-]\d{4}\)", seimas_label)

        assert seimas_dates_match is not None
        seimas_dates = seimas_dates_match.group(0).strip("()")
        start_year, end_year = seimas_dates.split("–")

        # don't collect seimas data beyond the CUTOFF_DATE
        if int(end_year) < CUTOFF_DATE:
            continue

        # visit the url of an older legislature landing page
        seimas_url = seimas_el.get("href")
        assert seimas_url is not None, "Coundn't fetch URL for the legislature"

        # the overview page layout differs between modern (>=2016) and older legislatures,
        # so the link to the members list — and thus the unblock validator — differs too.
        if int(start_year) >= 2016:
            members_link_xpath = '//div[contains(@class,"rubrika-kvadratai-item")]//a[@title="Members of the Seimas"]'
        else:
            # Legacy overview pages come in a couple of markup variants (e.g. the
            # 2004-2008 layout drops both the "td_kaire" id and the "medis" link
            # class that the 2012-2016 one still uses), but all wrap the menu link
            # in <div class="med"><a>...</a></div>, so match on that plus the text.
            members_link_xpath = (
                '//div[@class="med"]/a[normalize-space(.)="Members of the Seimas"]'
            )

        doc_seimas_overview = zyte_api.fetch_html(
            context,
            seimas_url,
            unblock_validator=members_link_xpath,
            html_source="httpResponseBody",
            cache_days=1,
            absolute_links=True,
        )

        members_url = h.xpath_string(doc_seimas_overview, members_link_xpath + "/@href")

        # the seimas webpage is similar to the current seimas for years starting with 2016, recycle the function:
        if int(start_year) >= 2016:
            # visit the older legislature page listing its members
            doc_seimas = zyte_api.fetch_html(
                context,
                members_url,
                unblock_validator=members_list_validator,
                html_source="httpResponseBody",
                cache_days=1,
                absolute_links=True,
            )

            member_urls = h.xpath_strings(doc_seimas, members_list_validator + "/@href")
            context.log.info(seimas_label, member_link_count=len(member_urls))
            emitted = 0
            for anchor_url in member_urls:
                if crawl_member_bio(context, anchor_url):
                    emitted += 1
            context.log.info(seimas_label, emitted_count=emitted)

        # The older legislatures' layouts vary (they don't share the modern
        # list markup), but all link to each member via a "p_asm_id" query
        # parameter, so select on that.
        else:
            old_members_validator = '//a[contains(@href, "p_asm_id")]'
            doc_seimas = zyte_api.fetch_html(
                context,
                members_url,
                unblock_validator=old_members_validator,
                html_source="httpResponseBody",
                cache_days=1,
                absolute_links=True,
            )

            # A member can appear more than once in the list, so dedupe URLs.
            old_member_urls = set(
                h.xpath_strings(doc_seimas, old_members_validator + "/@href")
            )
            context.log.info(seimas_label, member_link_count=len(old_member_urls))
            emitted = 0
            for old_member_url in sorted(old_member_urls):
                if crawl_old_member_bio(context, old_member_url):
                    emitted += 1
            context.log.info(seimas_label, emitted_count=emitted)
