import openpyxl

from zavod import Context, helpers as h


def crawl(context: Context):
    doc = context.fetch_html(context.data_url, cache_days=1)
    url = doc.xpath(
        './/a[contains(text(), "Companies Operating in the Uyghur Region")]/@href'
    )
    path = context.fetch_resource("source.xlsx", url[0])
    workbook: openpyxl.Workbook = openpyxl.load_workbook(path, read_only=True)
    for row in h.parse_xlsx_sheet(
        context, sheet=workbook["1. Companies Operating in XUAR"], skiprows=1
    ):
        name = row.pop("company")
        name_en = row.pop("company_machine_translated_english")
        city = row.pop("city")
        city_en = row.pop("city_english")
        addr = row.pop("address")
        addr_en = row.pop("address_machine_translated_english")
        sector = row.pop("sector")
        sector_en = row.pop("sector_english")

        entity = context.make("Company")
        entity.id = context.make_id(name, addr, sector)
        entity.add("name", name, lang="zhu")
        entity.add("name", name_en, lang="eng")
        entity.add("address", addr, lang="zhu")
        entity.add("address", addr_en, lang="eng")
        entity.add("sector", sector, lang="zhu")
        entity.add("sector", sector_en, lang="eng")
        entity.add("topics", "export.control")

        address_ent = h.make_address(context, full=addr, city=city, lang="zhu")
        address_en_ent = h.make_address(context, full=addr_en, city=city_en, lang="eng")
        h.copy_address(entity, address_ent)
        h.copy_address(entity, address_en_ent)

        context.emit(entity)

    context.audit_data(row)
