import openpyxl

from zavod import Context, helpers as h


def crawl(context: Context):
    doc = context.fetch_html(context.data_url, cache_days=1)
    url = doc.xpath(
        './/a[contains(text(), "Companies Operating in the Uyghur Region")]/@href'
    )
    path = context.fetch_resource("source.xlsx", url[0])
    workbook: openpyxl.Workbook = openpyxl.load_workbook(path, read_only=True)
    assert set(workbook.sheetnames) == {
        "Info ",
        "1. Companies Operating in XUAR",
        "2. Export Relevant Categories",
    }
    for row in h.parse_xlsx_sheet(
        context, sheet=workbook["1. Companies Operating in XUAR"], skiprows=1
    ):
        name = row.pop("company")
        addr = row.pop("address")
        sector = row.pop("sector")

        entity = context.make("Company")
        entity.id = context.make_id(name, addr, sector)
        entity.add("name", name, lang="zhu")
        entity.add("name", row.pop("company_machine_translated_english"), lang="eng")
        entity.add("sector", sector, lang="zhu")
        entity.add("sector", row.pop("sector_english"), lang="eng")
        entity.add("topics", "export.control")

        address_ent = h.make_address(
            context,
            full=addr,
            city=row.pop("city"),
            lang="zhu",
        )
        address_en_ent = h.make_address(
            context,
            full=row.pop("address_machine_translated_english"),
            city=row.pop("city_english"),
            lang="eng",
        )
        h.apply_address(context, entity, address_ent)
        h.apply_address(context, entity, address_en_ent)

        context.emit(entity)

    context.audit_data(row)
