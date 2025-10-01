from zavod import Context, helpers as h
from zavod.shed import zyte_api

OHCHR_BHR = "OHCHR-BHR"


def assert_database_hash(doc):
    tables_div = doc.xpath(
        "//div[@data-block-plugin-id='entity_browser_block:oh_accordion_component']"
    )
    assert len(tables_div) == 1, len(tables_div)
    # If the hash changes, update the 'table_xpath' below and review the page
    # structure for any new tables that may have been added.
    h.assert_dom_hash(tables_div[0], "b3fda7012ede1df28021b765c68d71f6e46755ca")


def crawl_row(context: Context, row):
    response = h.links_to_dict(row.pop("response_from_business_enterprise"))
    response_url = response.get("response")
    str_row = h.cells_to_str(row)
    name = str_row.pop("business_enterprise")
    country = str_row.pop("home_state")
    activities = str_row.pop("listed_activity_subparagraph_of_paragraph_96")

    entity = context.make("Company")
    entity.id = context.make_id(name, country)
    entity.add("name", name)
    entity.add("country", country)
    entity.add("sourceUrl", context.data_url)
    entity.add("topics", "debarment")
    entity.add("notes", f"Listed activities: {activities}")
    if response:
        entity.add("notes", f"Response from business enterprise: {response_url}")
    context.emit(entity)
    context.audit_data(str_row, ["no"])

    sanction = h.make_sanction(context, entity, program_key=OHCHR_BHR)
    context.emit(sanction)


def crawl(context: Context):
    table_xpath = "//h5[contains(., 'Business enterprises involved in listed activities')]/following-sibling::div//table"
    doc = zyte_api.fetch_html(
        context,
        context.data_url,
        table_xpath,
        html_source="httpResponseBody",
        absolute_links=True,
        cache_days=1,
    )
    assert_database_hash(doc)
    table = doc.xpath(table_xpath)
    assert len(table) == 1, len(table)
    for row in h.parse_html_table(table[0]):
        crawl_row(context, row)
