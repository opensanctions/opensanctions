import re
from typing import cast

from lxml import html
from lxml.html import HtmlElement
from openpyxl import load_workbook
from rigour.mime.types import XLSX

from zavod import Context, helpers as h
from zavod.stateful.review import assert_all_accepted

# A ";" directly followed by text separates name parts; "; " is a mangled comma.
NAME_SEPARATOR = re.compile(r";(?=\S)")
# Central Registration Depository (CRD) numbers have at most 7 digits
# https://www.finra.org/registration-exams-ce/classic-crd
MAX_CRD_DIGITS = 7


def restore_commas(cell: str) -> str:
    return cell.replace("; ", ", ").strip()


def split_parts(
    context: Context, raw_names: str, raw_crds: str | None, case_id: str
) -> list[tuple[str, str | None]]:
    """Split a name cell and its Central Registration Depository (CRD) cell
    into one (name, CRD) pair per part.

    A single name keeps its CRD cell whole so that the ``crd`` lookup can repair
    mangled cells such as ``"CRD 6972542"`` or a name that spilled into the CRD
    column. A single CRD means a single name: any ";" in the name cell is junk
    (``"Axos Clearing LLC;1"``, a stray ``;"`` in a quoted alias), so the cell
    is kept whole for name review.
    """
    names = [restore_commas(part) for part in NAME_SEPARATOR.split(raw_names)]
    if raw_crds is None:
        return [(name, None) for name in names]
    if len(names) == 1:
        return [(names[0], raw_crds.strip())]
    crds = [part.strip() for part in raw_crds.split(";")]
    if len(crds) == 1:
        return [(restore_commas(raw_names), crds[0])]
    if len(names) == len(crds):
        return list(zip(names, crds))

    context.log.warning(
        "Name/CRD count mismatch, CRDs not assigned",
        names=raw_names,
        crds=raw_crds,
        case_id=case_id,
    )
    return [(name, None) for name in names]


def crawl_item(context: Context, row: dict[str, str | None]) -> None:
    case_id = row.pop("case_id")
    assert case_id is not None, "Missing case ID"
    title = row.pop("title")
    individual_name = row.pop("individual_name")
    individual_crd = row.pop("individual_crd")
    firm_name = row.pop("firm_name")
    firm_crd = row.pop("firm_crd")
    summary = row.pop("summary")
    document_type = row.pop("document_type")
    document_link = row.pop("document_link")
    action_date = row.pop("action_date")
    context.audit_data(row, ignore=["has_related_cases"])

    if summary is not None and "<" in summary:
        summary = cast(HtmlElement, html.fromstring(summary)).text_content()

    name_parts: list[tuple[str, str | None]] = []
    for name, crd, other_crd in (
        (individual_name, individual_crd, firm_crd),
        (firm_name, firm_crd, individual_crd),
    ):
        if name is not None:
            name_parts.extend(split_parts(context, name, crd, case_id))
        elif crd is not None and crd not in (other_crd or "").split(";"):
            # A CRD with no name of its own. The orphan_crd lookup names the
            # entity it belongs to, or drops it with value: null.
            res = context.lookup("orphan_crd", crd)
            if res is None:
                context.log.warning(
                    "CRD without a name, discarded", crd=crd, case_id=case_id
                )
            elif res.value is not None:
                name_parts.append((res.value, crd))

    if len(name_parts) == 0:
        # A few rows leave both name columns empty; the subject is then only in
        # the document title (name and CRD) or the summary. Pinned by exact title.
        res = context.lookup("title", title)
        if res is None:
            context.log.warning(
                "Row has no individual or firm name", case_id=case_id, title=title
            )
            return
        assert isinstance(res.name, str), res
        assert res.crd is None or isinstance(res.crd, str), res
        name_parts.append((res.name, res.crd))

    for raw_name, crd in name_parts:
        entity = context.make("LegalEntity")
        # ID stability invariant: slug the raw split part before resolving lookups.
        entity.id = context.make_slug(raw_name)
        name = context.lookup_value("type.name", raw_name, default=raw_name)
        assert name is not None
        if raw_name.isdigit() and name == raw_name:
            context.log.warning(
                "Numeric name has no type.name lookup (bare CRD number?)",
                name=raw_name,
            )
        h.apply_reviewed_name_string(context, entity, string=name, llm_cleaning=True)
        entity.add("notes", document_type)
        entity.add("topics", "reg.action")
        entity.add("country", "us")

        crd_values: list[str] = []
        if crd is not None:
            res = context.lookup("crd", crd)
            crd_values = res.values if res is not None else [crd]
        for crd_value in crd_values:
            if not crd_value.isdigit():
                context.log.warning(
                    "Non-numeric CRD, not applied", value=crd_value, case_id=case_id
                )
            elif len(crd_value) > MAX_CRD_DIGITS:
                context.log.info(
                    "CRD has more digits than expected, not applied",
                    value=crd_value,
                    case_id=case_id,
                )
            else:
                entity.add("idNumber", crd_value)
        context.emit(entity)

        sanction = h.make_sanction(context, entity, key=case_id)
        if summary is not None:
            sanction.add("description", summary)
        sanction.add("authorityId", case_id)
        sanction.add("sourceUrl", document_link)
        h.apply_date(sanction, "date", action_date)
        context.emit(sanction)


def crawl(context: Context) -> None:
    path = context.fetch_resource("source.xlsx", context.data_url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    wb = load_workbook(path)
    ws = wb.worksheets[0]
    for row in h.parse_xlsx_sheet(context, ws):
        crawl_item(context, row)

    assert_all_accepted(context, raise_on_unaccepted=False)
