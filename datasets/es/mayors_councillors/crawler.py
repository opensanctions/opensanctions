import re

from rigour.mime.types import XLSX
from openpyxl import load_workbook

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise

IGNORE = [
    "autonomous_community",
    "column_0",
    "column_1",
    "column_2",
    "column_11",
    "column_12",
    "column_13",
    "column_14",
    "column_15",
    "column_16",
    "column_17",
]
DEFAULT_TOPICS = ["gov.muni"]
MAYOR_TOPICS = ["gov.muni", "gov.executive"]
COUNCILLOR_TOPICS = ["gov.muni", "gov.legislative"]
# e.g. .../Alcaldes_Mandato_2019_2023.xlsx, one file per completed mandate
MANDATE_FILE_RE = re.compile(r"Alcaldes_Mandato_(\d{4})_(\d{4})\.xlsx$")
# e.g. "MAYORS AND COUNCILLORS BY MUNICIPALITY 2023-2027"
CURRENT_TERM_RE = re.compile(r".* (\d{4})[-–](\d{4})$")


def crawl_item(
    context: Context,
    row: dict[str, str | None],
    period_start: str,
    period_end: str | None,
) -> None:
    start_date = row.pop("start_date")
    end_date = row.pop("end_date", None)
    assert start_date is not None

    name = row.pop("name")
    province = row.pop("province")
    municipality = row.pop("municipality")
    first_last_name = row.pop("last_name", None)
    second_last_name = row.pop("second_last_name", None)
    position_name = row.pop("position", None)

    if not name or not province or not municipality:
        context.log.warning("Missing required fields", row=row)
        return

    pep = context.make("Person")
    pep.id = context.make_id(
        name, province, municipality, first_last_name, second_last_name
    )
    # municipal elections are open to other nationals
    # https://www.senado.es/web/conocersenado/normas/constitucion/detalleconstitucioncompleta/index.html?lang=en
    pep.add("country", "es")
    if first_last_name or second_last_name:
        last_name = f"{first_last_name} {second_last_name}"
        h.apply_name(pep, first_name=name, last_name=last_name)
    else:
        pep.add("name", name)
    pep.add("political", row.pop("party"))
    pep.add("topics", "role.pep")
    # Positions are available for the current officials; historical data lists only mayors
    if not position_name:
        position_name, topics = f"Mayor of {municipality}, {province}", MAYOR_TOPICS
    else:
        translated = context.lookup_value("positions", position_name.strip())
        if translated:
            position_name = f"{translated} of {municipality}, {province}"
            topics = (
                MAYOR_TOPICS
                if "Mayor" in translated
                else COUNCILLOR_TOPICS
                if "Councillor" in translated
                else DEFAULT_TOPICS
            )
        else:
            context.log.warning("Unknown position", position=position_name.strip())
            position_name, topics = (
                f"{position_name.strip()} of {municipality}, {province}",
                DEFAULT_TOPICS,
            )
    pep.add("position", position_name)
    position = h.make_position(
        context,
        position_name,
        country="es",
        subnational_area=row.pop("ine_code"),
        lang="spa",
        topics=topics,
    )
    categorisation = categorise(context, position, default_is_pep=True)

    occupancy = h.make_occupancy(
        context,
        pep,
        position,
        start_date=start_date,
        end_date=end_date,
        period_start=period_start,
        period_end=period_end,
        categorisation=categorisation,
    )

    if occupancy:
        occupancy.add("constituency", province)
        context.emit(occupancy)
        context.emit(position)
        context.emit(pep)

    context.audit_data(row, IGNORE)


def process_excel(
    context: Context,
    filename: str,
    url: str,
    title: str,
    skiprows: int,
    period_start: str,
    period_end: str | None,
    expected_title: str | None = None,
) -> None:
    path = context.fetch_resource(filename, url)
    context.export_resource(path, XLSX, title=title)
    workbook = load_workbook(path, read_only=True)
    if len(workbook.sheetnames) != 1:
        context.log.warn(
            "Expected only one sheet in the workbook, found multiple",
            sheetnames=workbook.sheetnames,
        )
    sheet = workbook[workbook.sheetnames[0]]
    if expected_title is not None:
        # Cross-check the term against the title in the rows above the header
        titles = {
            cell.strip()
            for title_row in sheet.iter_rows(max_row=skiprows, values_only=True)
            for cell in title_row
            if isinstance(cell, str)
        }
        assert expected_title in titles, (expected_title, titles)
    for row in h.parse_xlsx_sheet_openpyxl(
        context,
        sheet,
        skiprows=skiprows,
        header_lookup=context.get_lookup("columns"),
    ):
        crawl_item(context, row, period_start=period_start, period_end=period_end)


def crawl(context: Context) -> None:
    """Crawl Spanish mayors and councillors data from official sources."""
    assert context.dataset.model.url is not None
    index_doc = context.fetch_html(
        context.dataset.model.url, cache_days=1, absolute_links=True
    )
    # Process historical mayors data from the latest completed mandate
    mandates: dict[tuple[str, str], str] = {}
    for href in h.xpath_strings(index_doc, ".//li[@class='dnt-link-default']/a/@href"):
        match = MANDATE_FILE_RE.search(href)
        if match is not None:
            mandates[(match.group(1), match.group(2))] = href
    assert len(mandates) > 0, "No mayoral mandate files found"
    hist_start, hist_end = max(mandates)
    # The current term is in the label of the link to the councillors application
    current_link = h.xpath_element(
        index_doc,
        ".//li[@class='dnt-link-default']/a[contains(@href, 'concejales.redsara.es')]",
    )
    current_label = h.element_text(current_link)
    match = CURRENT_TERM_RE.match(current_label)
    assert match is not None, current_label
    current_start = match.group(1)
    # Terms must be contiguous: the current term starts when the last completed one ends
    assert current_start == hist_end, (hist_start, hist_end, current_start)
    process_excel(
        context=context,
        filename=f"historical_{hist_start}_{hist_end}.xlsx",
        url=mandates[(hist_start, hist_end)],
        title=f"Mayors {hist_start}-{hist_end}",
        skiprows=7,
        period_start=hist_start,
        period_end=hist_end,
    )
    # Process current mayors and councillors data
    current_doc = context.fetch_html(
        context.data_url, cache_days=1, absolute_links=True
    )
    current_url = h.xpath_string(
        current_doc, ".//a[@id='concejales_legislatura']/@href"
    )
    process_excel(
        context=context,
        filename="current.xlsx",
        url=current_url,
        title=context.SOURCE_TITLE,
        skiprows=5,
        period_start=current_start,
        period_end=None,
        expected_title=f"Ficha de Concejales de la Legislatura {current_start}",
    )
