import json
from zavod import Context, helpers as h
import re

def crawl_item(input_dict: dict, context: Context):

    name = input_dict.pop("name")

    # If it's a potential clone, we remove the "potential clone" from the name
    name = name.replace("Potential clone entity – ", "")

    entity = context.make("LegalEntity")
    entity.id = context.make_id(name)
    entity.add("topics", "sanction")
    entity.add("name", name)
    entity.add("address", input_dict.pop("address"))
    entity.add("notes", input_dict.pop("remark"))

    for website in input_dict.pop("website").split(" | "):
        entity.add("sourceUrl", website)

    sanction = h.make_sanction(context, entity)

    sanction.add("date", h.parse_date(input_dict.pop("date"), formats=["%Y"]))

    context.emit(entity, target=True)
    context.emit(sanction)

    # group is just the alphabetical order of the name
    context.audit_data(input_dict, ignore=["group"])


def crawl(context: Context):

    response = context.fetch_html(context.data_url)

    # We first try to find the script with the data
    target_script = None

    for script in response.findall(".//script"):
        if script.text is not None and script.text.strip().startswith('$X.PMD'):
            target_script = script
            break

    target_script = target_script.text_content().replace("$X.PMD = ", "")

    idx = target_script.find(';$X.PPG')

    full_data = json.loads(target_script[:idx])

    for key in full_data:
        if len(full_data[key]):
            target_data = full_data[key].values()

    for item in target_data:
        crawl_item(item, context)
