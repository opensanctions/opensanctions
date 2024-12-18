import openpyxl

from zavod import Context, helpers as h


LABOUR_SHEETS = [
    "1. Labor Transfers in XUAR",
    "2. Labor Transfers Out of XUAR",
    "3. XPCC",
    "4. Priority Sector Apparel",
    "5. Priority Sector Tomato",
    "6. Priority Sector Polysilicon",
]


def apply_addresses(context, entity, addr, addr_en, city, city_en):
    """Create and apply addresses to an entity."""
    if addr:
        address_ent = h.make_address(context, full=addr, city=city, lang="zhu")
        h.apply_address(context, entity, address_ent)
    if addr_en:
        address_en_ent = h.make_address(context, full=addr_en, city=city_en, lang="eng")
        h.apply_address(context, entity, address_en_ent)


def crawl_labour_transfers(context: Context, labour_transfers_url):
    path = context.fetch_resource("labour_transfers.xlsx", labour_transfers_url[0])
    workbook: openpyxl.Workbook = openpyxl.load_workbook(path, read_only=True)
    assert set(workbook.sheetnames) == {
        "Information",
        "1. Labor Transfers in XUAR",
        "2. Labor Transfers Out of XUAR",
        "3. XPCC",
        "4. Priority Sector Apparel",
        "5. Priority Sector Tomato",
        "6. Priority Sector Polysilicon",
    }
    for sheet in LABOUR_SHEETS:
        for row in h.parse_xlsx_sheet(
            context,
            sheet=workbook[sheet],
            skiprows=1,
        ):
            name_en = row.pop("supplier_machine_translated_english")
            if not name_en:
                context.log.info("Skipping empty row", row=row)
                continue
            addr_xuar = row.pop("address_in_xuar", None)
            addr_xuar_en = row.pop("address_in_xuar_machine_translated_english", None)
            addr = row.pop("address", None)
            addr_en = row.pop("address_machine_translated_english", None)
            parent = row.pop("parent_company")
            parent_en = row.pop("parent_company_english")
            if parent_en is not None and "parent company" in parent_en.lower():
                parent_en_looked_up = context.lookup_value("parent_company", parent_en)
                if not parent_en_looked_up:
                    context.log.warning("No parent company lookup", parent_en=parent_en)
                parent_en = parent_en_looked_up
            sector = row.pop("industry")

            entity = context.make("Company")
            # Setting the ID based on the translated name to avoid blanks
            entity.id = context.make_id(name_en, addr, sector)
            entity.add("name", row.pop("supplier"), lang="zhu")
            entity.add("name", name_en, lang="eng")
            entity.add("sector", sector, lang="eng")
            entity.add("keywords", row.pop("allegation", None))
            entity.add("notes", row.pop("notes"))
            entity.add("classification", sheet)
            entity.add("topics", "export.control")
            entity.add(
                "program",
                "Companies Named in Media and Academic Reports as engaging in Labour Transfers or other XUAR Government Programs",
            )
            # entity.add("topics", "export.risk") # not sure if this is needed

            if parent is not None:
                parent_ent = context.make("Company")
                parent_ent.id = context.make_id(parent, parent_en)
                parent_ent.add("name", parent, lang="zhu")
                parent_ent.add("name", parent_en, lang="eng")
                parent_ent.add("topics", "export.control")
                context.emit(parent_ent)
                entity.add("parent", parent_ent)

            apply_addresses(
                context,
                entity,
                addr=addr_xuar or addr,
                addr_en=addr_xuar_en or addr_en,
                city=None,
                city_en=None,
            )

            context.emit(entity)

        context.audit_data(
            row,
            ignore=[
                "add_date",
                "source_1",
                "updated",
                "source",
            ],
        )


def crawl_operating(context: Context, companies_url):
    path = context.fetch_resource("companies_registry.xlsx", companies_url[0])
    workbook: openpyxl.Workbook = openpyxl.load_workbook(path, read_only=True)
    assert set(workbook.sheetnames) == {
        "Info ",
        "1. Companies Operating in XUAR",
        "2. Export Relevant Categories",
    }
    for row in h.parse_xlsx_sheet(
        context, sheet=workbook["1. Companies Operating in XUAR"], skiprows=1
    ):
        name_en = row.pop("company_machine_translated_english")
        addr = row.pop("address")
        sector = row.pop("sector")

        entity = context.make("Company")
        entity.id = context.make_id(name_en, addr, sector)
        entity.add("name", row.pop("company"), lang="zhu")
        entity.add("name", name_en, lang="eng")
        entity.add("sector", sector, lang="zhu")
        entity.add("sector", row.pop("sector_english"), lang="eng")
        entity.add("program", "Companies operating in the Uyghur region")

        apply_addresses(
            context,
            entity,
            addr=addr,
            addr_en=row.pop("address_machine_translated_english"),
            city=row.pop("city"),
            city_en=row.pop("city_english"),
        )

        context.emit(entity)

    context.audit_data(row)


def crawl(context: Context):
    doc = context.fetch_html(context.data_url, cache_days=1)
    companies_url = doc.xpath(
        './/a[contains(text(), "Companies Operating in the Uyghur Region")]/@href'
    )
    h.assert_url_hash(
        context, companies_url[0], "e71aab15641be03abeb8b6fce1564aad8714a124"
    )
    crawl_operating(context, companies_url)

    labour_transfers_url = doc.xpath(
        './/a[contains(text(), "Companies Named in Media and Academic Reports as engaging in Labour Transfers or other XUAR Government Programs")]/@href'
    )
    h.assert_url_hash(
        context, labour_transfers_url[0], "8b2b90e50cede22c17689052f6c6674be60aa38f"
    )
    crawl_labour_transfers(context, labour_transfers_url)
