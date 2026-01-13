import csv

from zavod.extract import zyte_api

from zavod import Context
from zavod import helpers as h

PROGRAM_KEY = "US-DOD-1260H"


def crawl(context: Context) -> None:
    results_xpath = ".//div[@class='alist stories release-list']"
    doc = zyte_api.fetch_html(
        context,
        "https://www.defense.gov/News/Releases/Search/1260H/",
        results_xpath,
        cache_days=1,
    )
    search_results = doc.xpath(results_xpath)
    assert len(search_results) == 1, "Expected exactly one section in the document"
    h.assert_dom_hash(search_results[0], "756b48c2a8a57399c96964c02c18ced39f2ac386")
    # Jan. 8, 2026
    # The War Department Strengthens Measures to Protect DOW‑Funded Research
    # Jan. 7, 2025
    # DOD Releases List of Chinese Military Companies in Accordance with Section 1260H of the National Defense Authorization Act for Fiscal Year 2021
    # Jan. 31, 2024
    # DOD Releases List of People's Republic of China (PRC) Military Companies in Accordance With Section 1260H of the National Defense Authorization Act for Fiscal Year 2021
    # Oct. 5, 2022
    # DOD Releases List of People's Republic of China (PRC) Military Companies in Accordance With Section 1260H of the National Defense Authorization Act for Fiscal Year 2021
    # June 3, 2021
    # DOD Releases List of Chinese Military Companies in Accordance With Section 1260H of the National Defense Authorization Act for Fiscal Year 2021

    source_file = context.fetch_resource("source.csv", context.data_url)
    with open(source_file, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            entity = context.make("Company")
            name = row.pop("Clean Name")
            entity.id = context.make_id(name)
            entity.add("name", name)
            entity.add("alias", row.pop("Name", None))
            entity.add("alias", row.pop("Alias", None))
            entity.add("notes", row.pop("Note", None))
            parent_name = row.pop("Parent Name", None)
            if parent_name != "" and parent_name != name:
                parent = context.make("Company")
                parent.id = context.make_id(parent_name)
                parent.add("name", parent_name)
                context.emit(parent)

                own = context.make("Ownership")
                own.id = context.make_id("ownership", name, parent_name)
                own.add("owner", parent)
                own.add("asset", entity)
                context.emit(own)
            entity.add("topics", "debarment")
            sanction = h.make_sanction(context, entity, program_key=PROGRAM_KEY)
            sanction.add("startDate", row.pop("Start date", None))
            sanction.add("endDate", row.pop("End date", None))
            context.emit(sanction)
            context.emit(entity)
            context.audit_data(row)
