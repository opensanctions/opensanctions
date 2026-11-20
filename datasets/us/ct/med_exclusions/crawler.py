import re
from datetime import date

from zavod import Context
from zavod import helpers as h
from zavod.extract import zyte_api

HEADERS = [
    "Name",
    "Business",
    "Specialty",
    "Address",
    "Effective Date",
    "Period",
    "Administrative Action",
]
REGEX_PERIOD = re.compile(r"^(?P<years>\d+) years?$")


def clean(value: str) -> str | None:
    value = " ".join(value.split())
    return None if value in ("", "N/A", ",") else value


def add_years(start: date, years: int) -> date:
    try:
        return start.replace(year=start.year + years)
    except ValueError:
        # 29 February in a year that has none.
        return start.replace(year=start.year + years, day=28)


def crawl_row(context: Context, row: dict[str, str]) -> None:
    raw_name = row.pop("Name")
    raw_business = row.pop("Business")
    effective_date = row.pop("Effective Date")
    name = clean(raw_name)
    business = clean(raw_business)
    assert name is not None or business is not None, row

    entity = context.make("Person" if name is not None else "LegalEntity")
    entity.id = context.make_id(raw_name, raw_business, effective_date)
    if name is not None:
        res = context.lookup("names", name)
        if res is not None:
            h.apply_name(entity, first_name=res.first_name, last_name=res.last_name)
            entity.add("alias", res.alias)
        elif name.count(",") == 1 and "(" not in name:
            last_name, first_name = name.split(",")
            h.apply_name(entity, first_name=first_name, last_name=last_name)
        else:
            context.log.warning("Irregular person name, add a lookup", name=name)
            entity.add("name", name)
    else:
        entity.add("name", business)
    entity.add("sector", clean(row.pop("Specialty")))
    entity.add("country", "us")
    address = clean(row.pop("Address"))
    if address is not None:
        h.copy_address(entity, h.make_address(context, full=address, country_code="us"))

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "startDate", effective_date, two_digit_year_base=1977)
    sanction.add(
        "provisions",
        context.lookup_value(
            "action", row.pop("Administrative Action"), warn_unmatched=True
        ),
    )
    period = row.pop("Period")
    if period != "Indefinite":
        match = REGEX_PERIOD.match(period)
        start = sanction.first("startDate")
        if match is None or start is None or len(start) != 10:
            context.log.warning(
                "Unexpected exclusion period", period=period, start=start
            )
        else:
            end = add_years(date.fromisoformat(start), int(match.group("years")))
            sanction.add("endDate", end.isoformat())
    if h.is_active(sanction):
        entity.add("topics", "debarment")

    # The business is the practice the person worked at; the source doesn't say
    # whether the action extends to it, so it gets no topic.
    if name is not None and business is not None:
        practice = context.make("LegalEntity")
        practice.id = context.make_id("practice", raw_business, address)
        practice.add("name", business)
        practice.add("country", "us")
        link = context.make("UnknownLink")
        link.id = context.make_id(entity.id, practice.id)
        link.add("subject", entity)
        link.add("object", practice)
        context.emit(practice)
        context.emit(link)

    context.emit(entity)
    context.emit(sanction)
    context.audit_data(row)


def crawl(context: Context) -> None:
    # The DSS page frames this legacy app, which rejects non-browser clients.
    doc = zyte_api.fetch_html(
        context,
        context.data_url,
        unblock_validator="//table//tr/td",
        html_source="httpResponseBody",
        geolocation="US",
    )
    rows = h.xpath_elements(doc, "//tr")
    header = [h.element_text(cell) for cell in h.xpath_elements(rows[0], "./th|./td")]
    assert header == HEADERS, header
    assert len(rows) > 50, len(rows)
    for tr in rows[1:]:
        cells = [h.element_text(cell) for cell in h.xpath_elements(tr, "./td")]
        assert len(cells) == len(HEADERS), cells
        crawl_row(context, dict(zip(HEADERS, cells)))
