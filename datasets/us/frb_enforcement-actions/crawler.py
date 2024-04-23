from io import StringIO
import csv
from urllib.parse import urlparse, urljoin

from zavod import Context, helpers as h


def crawl_item(input_dict: dict, context: Context):
    if input_dict["Individual"]:
        schema = "Person"
        names = [input_dict.pop("Individual")]
    else:
        schema = "Company"
        names = h.split_comma_names(context, input_dict.pop("Banking Organization"))

    effective_date = input_dict.pop("Effective Date")
    termination_date = input_dict.pop("Termination Date")
    url = input_dict.pop("URL")
    provisions = input_dict.pop("Action")
    sanction_description = input_dict.pop("Note")

    for name in names:
        entity = context.make(schema)
        entity.id = context.make_id(name)
        entity.add("name", name)

        sanction = h.make_sanction(context, entity)
        sanction.add("startDate", h.parse_date(effective_date, formats=["%Y-%m-%d"]))
        sanction.add("provisions", provisions)
        sanction.add("description", sanction_description)

        if url != "DNE":
            sanction.add("sourceUrl", url)

        if termination_date != "":
            sanction.add(
                "endDate", h.parse_date(termination_date, formats=["%Y-%m-%d"])
            )

        context.emit(entity, target=True)
        context.emit(sanction)

    # Individual Affiliation = The bank of the individual
    # Name = the string that appears in the url column
    context.audit_data(
        input_dict, ignore=["Individual Affiliation", "Name"]
    )


def crawl(context: Context):
    response = context.fetch_text(context.data_url)

    for item in csv.DictReader(StringIO(response)):
        item["URL"] = urljoin(context.data_url, item["URL"])
        crawl_item(item, context)
