from typing import Dict

from zavod import Context, helpers as h
import re

NAME_PATTERN = re.compile(
    r"""
    ^\s*([A-Z][a-zA-Z' -]+)          # Last name(s)
    (?:,\s*(Jr\.?|Sr\.?|II|III|IV))? # Optional suffix
    (?:,\s*(MD|PhD|DO|DVM))?         # Optional pre-first-name credential
    ,\s*([A-Z][a-zA-Z]+)             # First given name
    (?:\s+([A-Z](?:\.)?|[A-Z][a-zA-Z]+(?:\s+[A-Z](?:\.)?|[A-Z][a-zA-Z]+)*))? # Additional given names/initials
    (?:,\s*(.*))?                    # Remaining credentials (best effort)
    \s*$""",
    re.VERBOSE,
)


def crawl_item(row: Dict[str, str], context: Context):

    raw_name = row.pop("name")
    address = h.make_address(
        context, city=row.pop("city"), state=row.pop("state"), country_code="us"
    )

    entity = context.make("Person")
    entity.id = context.make_id(raw_name)

    match = NAME_PATTERN.match(raw_name)
    if not match:
        context.log.warning(f"Unable to parse {raw_name}")
        entity.add("name", raw_name)
    else:
        last, suffix, precred, first, middle, postcreds = match.groups()
        h.apply_name(
            entity,
            last_name=last,
            suffix=suffix,
            first_name=first,
            middle_name=middle,
            prefix=postcreds,
        )

    entity.add("country", "us")
    entity.add("address", address)
    entity.add("sector", row.pop("center"))

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "startDate", row.pop("date_of_status"))
    sanction.add("status", row.pop("status"))

    # In those cases the sanction is not active anymore
    target = sanction.get("status") not in [
        "Restrictions Removed",
        "Not Disqualified",
        "Reinstated pursuant to Agreement",
        "Not Disqualified – Adequate Assurances 1",
        "Restrictions Removed (reinstated by correspondence)",
    ]

    if target:
        entity.add("topics", "debarment")

    context.emit(entity, target=target)
    context.emit(sanction)

    # NIDPOE = Notice of Initiation of Disqualification Proceedings and Opportunity to Explain
    # NOOH = Notice of Opportunity for Hearing
    context.audit_data(
        row,
        ignore=[
            "date_nidpoe_issued",
            "date_nooh_issued",
            "link_to_nidpoe_letter",
            "link_to_nooh_letter",
            "date_of_presiding_officer_report",
            "link_to_presiding_officer_report",
            "date_of_commissioner_s_decision",
            "link_to_commissioner_s_decision",
        ],
    )


def crawl(context: Context) -> None:

    response = context.fetch_html(context.data_url)

    for item in h.parse_html_table(response):
        item = h.cells_to_str(item)
        crawl_item(item, context)
