import json
import re

from zavod import Context


def crawl_item(input_dict: dict, context: Context):
    name = input_dict.pop("name")

    # If it's a potential clone, we remove the "potential clone" from the name
    potential_clone = "Potential clone entity – " in name
    name = name.replace("Potential clone entity – ", "")

    entity = context.make("LegalEntity")
    entity.id = context.make_id(name)
    entity.add("name", name.split(" / "))
    address = input_dict.pop("address").replace("N/A", "")
    addresses = re.split(r"\b\d\) ", address)
    entity.add("address", addresses)
    entity.add("notes", input_dict.pop("remark").split("\n"))
    entity.add("topics", "crime.fin")
    entity.add("sourceUrl", input_dict.pop("url"))
    if potential_clone:
        entity.add("description", "Potential clone entity")

    for website in input_dict.pop("website").split(" | "):
        entity.add("website", website)

    context.emit(entity)

    # group is just the alphabetical order of the name
    context.audit_data(input_dict, ignore=["group", "date"])


def crawl(context: Context):
    response = context.fetch_html(context.data_url)

    # We first try to find the script with the data
    target_script = None

    for script in response.findall(".//script"):
        if script.text is not None and script.text.strip().startswith("$X.PMD"):
            target_script = script
            break

    target_script = target_script.text_content().replace("$X.PMD = ", "")

    idx = target_script.find(";$X.PPG")

    full_data = json.loads(target_script[:idx])

    for key in full_data:
        if len(full_data[key]):
            target_data = full_data[key].values()

    for item in target_data:
        crawl_item(item, context)
