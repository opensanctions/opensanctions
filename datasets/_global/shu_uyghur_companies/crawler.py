import openpyxl

from zavod import Context, helpers as h


def crawl(context: Context):
    doc = context.fetch_html(context.data_url, cache_days=1)
    # companies_url = doc.xpath(
    #     './/a[contains(text(), "Companies Operating in the Uyghur Region")]/@href'
    # )
    # crawl_all_companies(context, companies_url)

    labour_transfers_url = doc.xpath(
        './/a[contains(text(), "Companies Named in Media and Academic Reports as engaging in Labour Transfers or other XUAR Government Programs")]/@href'
    )
    crawl_labour_transfers(context, labour_transfers_url)


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
    for row in h.parse_xlsx_sheet(
        context,
        sheet=workbook["1. Labor Transfers in XUAR"],
        skiprows=1,
    ):
        name = row.pop("supplier")
        name_en = row.pop("supplier_machine_translated_english")
        addr = row.pop("address_in_xuar")
        addr_en = row.pop("address_in_xuar_machine_translated_english")
        parent = row.pop("parent_company")
        parent_en = row.pop("parent_company_english")
        sector = row.pop("industry")
        allegation = row.pop("allegation")
        source1 = row.pop("source_1")
        source2 = row.pop("source_2")
        source3 = row.pop("source_3")
        source4 = row.pop("source_4")
        notes = row.pop("notes")

        entity = context.make("Company")
        entity.id = context.make_id(name, addr, sector)
        entity.add("name", name, lang="zhu")
        entity.add("name", row.pop("supplier_machine_translated_english"), lang="eng")
        entity.add("sector", sector, lang="zhu")
        entity.add("sector", row.pop("sector_english"), lang="eng")
        entity.add("topics", "labour.transfers")

        # address_ent = h.make_address(
        #     context,
        #     full=addr,
        #     city=row.pop("city"),
        #     lang="zhu",
        # )
        # address_en_ent = h.make_address(
        #     context,
        #     full=row.pop("address_machine_translated_english"),
        #     city=row.pop("city_english"),
        #     lang="eng",
        # )
        h.apply_address(context, entity, address_ent)
        h.apply_address(context, entity, address_en_ent)

        context.emit(entity)

    context.audit_data(row)


def crawl_all_companies(context: Context, companies_url):
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
