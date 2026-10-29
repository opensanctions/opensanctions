from python_calamine import CalamineWorkbook
from rigour.mime.types import XLSX

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise


def _build_canonical_ine_code(ine_code: str | None) -> str:
    """Pad an INE municipality code to its six digits.

    Some source rows store the code as a number, which drops its leading zero.
    """
    assert ine_code is not None
    canonical = ine_code.zfill(6)
    assert len(canonical) == 6 and canonical.isdigit(), ine_code
    return canonical


def crawl_item(
    context: Context,
    *,
    row: dict[str, str | None],
    position_label: str,
    area_names: dict[str, str],
    period_start: str,
    period_end: str,
) -> None:
    name = row.pop("name")
    province = row.pop("province")
    municipality = row.pop("municipality")
    first_last_name = row.pop("last_name", None)
    second_last_name = row.pop("second_last_name", None)
    ine_code = _build_canonical_ine_code(row.pop("ine_code"))
    start_date = row.pop("start_date")
    end_date = row.pop("end_date", None)
    assert start_date is not None, row

    if not name or not province or not municipality:
        context.log.warning("Missing required fields", row=row)
        return

    area_name = area_names.get(ine_code, f"{municipality}, {province}")

    pep = context.make("Person")
    pep.id = context.make_id(
        name, province, municipality, first_last_name, second_last_name
    )
    # municipal elections are open to other nationals
    # https://www.senado.es/web/conocersenado/normas/constitucion/detalleconstitucioncompleta/index.html?lang=en
    pep.add("country", "es")
    if first_last_name or second_last_name:
        last_name = " ".join(
            n for n in (first_last_name, second_last_name) if n is not None
        )
        h.apply_name(pep, first_name=name, last_name=last_name)
    else:
        pep.add("name", name)
    pep.add("political", row.pop("party"))
    pep.add("topics", "role.pep")

    position_title = context.lookup_value(
        "positions", position_label, warn_unmatched=True
    )
    if position_title is None:
        return
    position = h.make_position(
        context,
        f"{position_title} of {area_name}",
        country="es",
        subnational_area=area_name,
        lang="eng",
    )
    # Key the ID on the INE code, not the municipality name, because the source
    # spells some municipality names differently between files
    position.id = context.make_id(position_title, "es", ine_code)
    categorisation = categorise(context, position)
    pep.add("position", position.get("name"))

    occupancy = h.make_occupancy(
        context,
        pep,
        position,
        start_date=start_date,
        end_date=end_date,
        categorisation=categorisation,
    )

    if occupancy:
        occupancy.add("constituency", province)
        occupancy.add("periodStart", period_start)
        occupancy.add("periodEnd", period_end)
        context.emit(occupancy)
        context.emit(position)
        context.emit(pep)

    context.audit_data(
        row,
        # column_13 of the historical file is a glossary of party abbreviations
        ignore=["autonomous_community", "column_13"],
    )


def load_rows(
    context: Context, filename: str, url: str, title: str, skiprows: int
) -> list[dict[str, str | None]]:
    path = context.fetch_resource(filename, url)
    context.export_resource(path, XLSX, title=title)
    workbook = CalamineWorkbook.from_path(path)
    assert len(workbook.sheet_names) == 1, workbook.sheet_names
    return list(
        h.parse_xlsx_sheet(
            context,
            workbook.get_sheet_by_index(0),
            skiprows=skiprows,
            header_lookup=context.get_lookup("columns"),
        )
    )


def crawl(context: Context) -> None:
    assert context.dataset.model.url is not None
    hist_doc = context.fetch_html(
        context.dataset.model.url, cache_days=1, absolute_links=True
    )
    # The period years passed to crawl_item for these rows follow this file name
    hist_url = h.xpath_string(
        hist_doc,
        ".//li[@class='dnt-link-default']/a[contains(@href, 'Alcaldes_Mandato_2019_2023')]/@href",
    )
    hist_rows = load_rows(
        context,
        filename="historical.xlsx",
        url=hist_url,
        title="Mayors 2019-2023",
        skiprows=7,
    )
    current_doc = context.fetch_html(
        context.data_url, cache_days=1, absolute_links=True
    )
    current_url = h.xpath_string(
        current_doc, ".//a[@id='concejales_legislatura']/@href"
    )
    current_rows = load_rows(
        context,
        filename="current.xlsx",
        url=current_url,
        title=context.SOURCE_TITLE,
        skiprows=5,
    )

    # The two files spell some municipality names differently. Name each
    # municipality as the current file does, so a code always maps to one
    # subnationalArea.
    area_names: dict[str, str] = {}
    for row in current_rows:
        ine_code = _build_canonical_ine_code(row["ine_code"])
        area_names[ine_code] = f"{row['municipality']}, {row['province']}"

    for row in hist_rows:
        crawl_item(
            context,
            row=row,
            # The historical file lists only mayors and has no position column.
            # "Alcalde" (mayor) is the label the current file uses for them.
            position_label="Alcalde",
            area_names=area_names,
            period_start="2019",
            period_end="2023",
        )
    for row in current_rows:
        position_label = row.pop("position")
        assert position_label is not None, row
        crawl_item(
            context,
            row=row,
            position_label=position_label,
            area_names=area_names,
            period_start="2023",
            period_end="2027",
        )
