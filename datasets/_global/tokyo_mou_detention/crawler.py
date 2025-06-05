from zavod import Context, helpers as h


data = {
    "Mode": "DetList",
    "MOU": "TMOU",
    "Src": "online",
    "Type": "Auth",
    "Month": "06",
    "Year": "2025",
    "SaveFile": "",
}


def crawl_row(context: Context, clean_row: dict):
    ship_name = clean_row.pop("ship_name")
    imo = clean_row.pop("imo_no")

    vessel = context.make("Vessel")
    vessel.id = context.make_id(ship_name, imo)
    vessel.add("name", ship_name)
    vessel.add("imoNumber", imo)
    vessel.add("flag", clean_row.pop("ship_flag"))
    vessel.add("buildDate", clean_row.pop("year_of_build"))
    vessel.add("grossRegisteredTonnage", clean_row.pop("gross_tonnage"))
    vessel.add("type", clean_row.pop("ship_type"))
    vessel.add("topics", "reg.warn")

    sanction = h.make_sanction(
        context,
        vessel,
        start_date=clean_row.pop("date_of_detention"),
        end_date=clean_row.pop("date_of_release", None),
    )

    sanction.add("summary", clean_row.pop("nature_of_deficiencies"))

    context.emit(vessel)
    context.emit(sanction)

    context.audit_data(
        clean_row,
        ignore=[
            "classification_society",
            "related_ros",
            "company",
            "place_of_detention",
            "nature_of_deficiencies",
            "",
        ],
    )


def crawl(context: Context):
    url = "https://apcis.tmou.org/isss/public_apcis.php?Mode=DetList"
    doc = context.fetch_html(url, data=data, method="POST", cache_days=1)
    table = doc.xpath("//table[@cellspacing=1]")
    assert len(table) == 1, "Expected one table in the document"
    table = table[0]
    for row in h.parse_html_table(table, header_tag="td", skiprows=1):
        str_row = h.cells_to_str(row)
        clean_row = {k: v for k, v in str_row.items() if k is not None}
        crawl_row(context, clean_row)
