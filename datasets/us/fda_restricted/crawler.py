import re
from typing import Dict

from zavod import Context
from zavod import helpers as h

REGEX_TITLE = re.compile("(?:, (MD|PhD|DO|DVM))")
REGEX_SUFFIX = re.compile(r",? (Jr|Sr|II|III|IV).?,")


def crawl_item(context: Context, row: Dict[str, str], row_elements):

    raw_name = row.pop("name")
    state = row.pop("state")

    entity = context.make("Person")
    entity.id = context.make_id(raw_name, state)

    namestr = raw_name
    title = None
    suffix = None
    # Extract title(s) from end or inbetween last and forenames
    if title := REGEX_TITLE.findall(namestr):
        namestr = REGEX_TITLE.sub("", namestr)
    # Extract suffix from in between last and forenames
    if match := REGEX_SUFFIX.search(namestr):
        suffix = match.group(1)
        namestr = REGEX_SUFFIX.sub(",", namestr, 1)
    parts = namestr.split(", ", 1)
    forenames = parts[1].split(" ")
    lastname = parts[0]

    h.apply_name(
        entity,
        first_name=forenames[0],
        middle_name=forenames[1] if len(forenames) > 1 else None,
        name3=forenames[2] if len(forenames) > 2 else None,
        last_name=lastname,
        suffix=suffix,
    )
    if len(forenames) > 3:
        context.log.warning(f"Name has more than 3 forenames: {raw_name}")
    entity.add("title", title)

    entity.add("country", "us")
    address = h.make_address(
        context, city=row.pop("city"), state=state, country_code="us"
    )
    h.copy_address(entity, address)

    sanction = h.make_sanction(context, entity)
    # NIDPOE = Notice of Initiation of Disqualification Proceedings and Opportunity to Explain
    # NOOH = Notice of Opportunity for Hearing
    h.apply_date(sanction, "date", row.pop("date_nidpoe_issued"))
    h.apply_date(sanction, "date", row.pop("date_nooh_issued"))
    h.apply_date(sanction, "date", row.pop("date_of_presiding_officer_report"))
    h.apply_date(sanction, "startDate", row.pop("date_of_commissioner_s_decision"))
    h.apply_date(sanction, "modifiedAt", row.pop("date_of_status"))
    sanction.add("authority", row.pop("center"))
    for link_key in [
        "link_to_nidpoe_letter",
        "link_to_nooh_letter",
        "link_to_presiding_officer_report",
        "link_to_commissioner_s_decision",
    ]:
        # The PDF links all seem broken
        urls = row_elements.pop(link_key).xpath(".//a[text()='Text']/@href")
        sanction.add("sourceUrl", urls)

    status = row.pop("status")
    res = context.lookup("is_debarred_status", status)
    if res:
        is_debarred = res.is_debarred
    else:
        is_debarred = True
        context.log.warning(f"Unknown status: {status}")
    sanction.add("status", status)

    if is_debarred:
        entity.add("topics", "debarment")

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(
        row,
        ignore=[
            "link_to_nidpoe_letter",
            "link_to_nooh_letter",
            "link_to_presiding_officer_report",
            "link_to_commissioner_s_decision",
        ],
    )


def crawl(context: Context) -> None:

    response = context.fetch_html(context.data_url)

    for row_elements in h.parse_html_table(response):
        row = h.cells_to_str(row_elements)
        crawl_item(context, row, row_elements)
