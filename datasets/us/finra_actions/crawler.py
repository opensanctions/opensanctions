"""Import FINRA actions from its XLSX export.

The export corrupts commas within names as ``"; "``. Entity IDs must be slugged
from each raw split part before name lookups are applied to preserve production ID
stability.
"""

import re
from typing import cast

from lxml import html
from lxml.html import HtmlElement
from openpyxl import load_workbook
from rigour.mime.types import XLSX

from zavod import Context, helpers as h
from zavod.stateful.review import assert_all_accepted

ORGANIZATION_SUFFIXES = (
    "inc",
    "incorporated",
    "llc",
    "l.l.c",
    "ltd",
    "llp",
    "l.l.p",
    "lp",
    "l.p",
    "corp",
    "corporation",
    "co",
    "company",
    "plc",
)
ALIAS_PREFIXES = (
    "aka ",
    "a/k/a",
    "a.k.a",
    "or ",
    "formerly",
    "also known as",
    "now known as",
    "n/k/a",
    "f/k/a",
    "d/b/a",
    "dba ",
)


def continuation_separator(part: str, suffix_tokens: set[str]) -> str | None:
    part_tokens = part.replace(",", "").split()
    if len(part_tokens) > 0 and part_tokens[0].casefold() in suffix_tokens:
        return " "
    if part.casefold().removesuffix(".") in ORGANIZATION_SUFFIXES:
        return ", "
    if part.startswith(("(", '"')) or part.casefold().startswith(ALIAS_PREFIXES):
        return " "
    return None


def split_individual_names(
    context: Context,
    raw_name: str,
    raw_crd: str | None,
    suffix_tokens: set[str],
    case_id: str,
) -> list[tuple[str, str | None]]:
    """Split names and re-attach suffix or alias continuation parts."""
    names: list[str] = []
    for part in raw_name.split(";"):
        part = part.strip()
        if part == "":
            continue

        separator = continuation_separator(part, suffix_tokens)
        if separator is not None:
            if len(names) == 0:
                context.log.warning(
                    "Individual name continuation has no preceding name",
                    continuation=part,
                    individual_name=raw_name,
                )
                continue
            names[-1] = f"{names[-1]}{separator}{part}"
            continue
        names.append(part)

    if raw_crd is None:
        return [(name, None) for name in names]

    crd_parts = [c.strip() for c in raw_crd.split(";")]
    if len(crd_parts) == len(names):
        return list(zip(names, crd_parts))
    if len(names) == 1 and len(set(crd_parts)) == 1:
        return [(names[0], crd_parts[0])]

    context.log.warning(
        "Individual name/CRD count mismatch — CRDs not assigned",
        name=raw_name,
        name_count=len(names),
        crd_count=len(crd_parts),
        case_id=case_id,
    )
    return [(name, None) for name in names]


def split_firm_names(
    context: Context, raw_name: str, raw_crd: str | None, case_id: str
) -> list[tuple[str, str | None]]:
    """Split multi-firm rows where the CRD column lists one number per firm."""
    if raw_crd is None or ";" not in raw_crd:
        return [(raw_name, raw_crd)]
    crd_parts = [part.strip() for part in raw_crd.split(";")]
    if not all(part.isdigit() for part in crd_parts):
        # e.g. a name spilled into the CRD column — resolved via the crd lookup.
        return [(raw_name, raw_crd)]
    # A ";" directly followed by text is a genuine separator, unlike the
    # "; " comma corruption.
    name_parts = [part.strip() for part in re.split(r";(?=\S)", raw_name)]
    if len(name_parts) != len(crd_parts):
        context.log.warning(
            "Firm name/CRD count mismatch — CRDs not assigned",
            name=raw_name,
            name_count=len(name_parts),
            crd_count=len(crd_parts),
            case_id=case_id,
        )
        return [(raw_name, None)]
    return list(zip(name_parts, crd_parts))


def crawl_item(
    context: Context, row: dict[str, str | None], suffix_tokens: set[str]
) -> None:
    case_id = row.pop("case_id")
    assert case_id is not None, "Missing case ID"
    date = row.pop("action_date")
    individual_name = row.pop("individual_name")
    individual_crd = row.pop("individual_crd")
    firm_name = row.pop("firm_name")
    firm_crd = row.pop("firm_crd")
    summary = row.pop("summary")
    document_link = row.pop("document_link")
    context.audit_data(row, ignore=["title", "document_type", "has_related_cases"])
    if summary is not None and "<" in summary:
        summary = cast(HtmlElement, html.fromstring(summary)).text_content()

    names: list[tuple[str, str | None]] = []
    if individual_name is not None:
        names.extend(
            split_individual_names(
                context, individual_name, individual_crd, suffix_tokens, case_id
            )
        )
    if firm_name is not None:
        names.extend(split_firm_names(context, firm_name, firm_crd, case_id))

    if len(names) == 0:
        context.log.warning("Row has no individual or firm name", case_id=case_id)
        return

    for raw_part, crd in names:
        entity = context.make("LegalEntity")
        # ID stability invariant: slug the raw split part before resolving lookups.
        entity.id = context.make_slug(raw_part)
        display = raw_part.replace("; ", ", ")
        name = context.lookup_value("type.name", display, default=display)
        assert name is not None
        if display.isdigit() and name == display:
            context.log.warning(
                "Numeric name has no type.name lookup (bare CRD number?)",
                name=display,
            )
        h.apply_reviewed_name_string(context, entity, string=name, llm_cleaning=True)
        entity.add("topics", "reg.action")
        entity.add("country", "us")

        crd_values: list[str] = []
        if crd is not None:
            res = context.lookup("crd", crd)
            crd_values = res.values if res is not None else [crd]
        for crd_value in crd_values:
            if crd_value.isdigit():
                entity.add("idNumber", crd_value)
            else:
                context.log.warning(
                    "Non-numeric CRD, not applied", value=crd_value, case_id=case_id
                )
        context.emit(entity)

        sanction = h.make_sanction(context, entity, key=case_id)
        if summary is not None:
            sanction.add("description", f"{date}: {summary}")
        sanction.add("authorityId", case_id)
        sanction.add("sourceUrl", document_link)
        h.apply_date(sanction, "date", date)
        context.emit(sanction)


def crawl(context: Context) -> None:
    path = context.fetch_resource("source.xlsx", context.data_url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    wb = load_workbook(path)
    ws = wb.worksheets[0]
    suffix_tokens = {
        token.casefold()
        for suffix in context.dataset.names.suffixes_strip
        for token in suffix.removeprefix(", ").split()
    }

    for row in h.parse_xlsx_sheet(context, ws):
        crawl_item(context, row, suffix_tokens)

    assert_all_accepted(context, raise_on_unaccepted=False)
