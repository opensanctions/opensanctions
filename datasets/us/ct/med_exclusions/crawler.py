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


def crawl_row(context: Context, row: dict[str, str]) -> None:
    raw_name = row.pop("Name")
    raw_business = row.pop("Business")
    effective_date = row.pop("Effective Date")
    name = clean(raw_name)
    business = clean(raw_business)
    if name is None and business is None:
        # Test rows ("Test a11y").
        context.log.info("Skipping row without name or business", row=row)
        return

    entity = context.make("Person" if name is not None else "LegalEntity")
    entity.id = context.make_id(raw_name, raw_business, effective_date)
    if name is not None:
        res = context.lookup("names", name)
        if res is not None:
            h.apply_name(entity, first_name=res.first_name, last_name=res.last_name)
            entity.add("alias", res.alias)
        # "(" and "/" mark an alias.
        elif name.count(",") == 1 and "(" not in name and "/" not in name:
            last_name, first_name = name.split(",")
            h.apply_name(entity, first_name=first_name, last_name=last_name)
        else:
            context.log.warning("Irregular person name, add a lookup", name=name)
            entity.add("name", name)
    else:
        entity.add("name", business)
    entity.add("sector", clean(row.pop("Specialty")))
    entity.add("country", "us")
    # Drop blank parts, e.g. ", , CT, 06120".
    parts = [" ".join(part.split()) for part in row.pop("Address").split(",")]
    address = clean(", ".join(part for part in parts if part))
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
    sanction.add("duration", period)
    start = sanction.first("startDate")
    assert start is not None, effective_date
    # Raises on a partial date, which would make the end date meaningless.
    start_date = date.fromisoformat(start)
    match = REGEX_PERIOD.match(period)
    if match is not None:
        years = int(match.group("years"))
        h.apply_date(
            sanction, "endDate", start_date.replace(year=start_date.year + years)
        )
    elif period != "Indefinite":
        context.log.warning("Unexpected exclusion period", period=period)
    if h.is_active(sanction):
        entity.add("topics", "debarment")

    # The business is the practice the person worked at; the source doesn't say
    # whether the action extends to it, so it gets no topic.
    if name is not None and business is not None:
        # Some Business cells hold a role.
        role = context.lookup_value("roles", business)
        if role is not None:
            entity.add("position", role)
        else:
            practice = context.make("LegalEntity")
            practice.id = context.make_id("practice", raw_business)
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
