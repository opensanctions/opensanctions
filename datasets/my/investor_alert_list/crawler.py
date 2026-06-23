import json
import re

from zavod import Context
from zavod import helpers as h


def crawl_item(input_dict: dict[str, str], context: Context) -> None:
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


def crawl(context: Context) -> None:
    response = context.fetch_html(context.data_url)

    # We first try to find the script with the data
    target_script = None

    for script in response.findall(".//script"):
        if script.text is not None and script.text.strip().startswith("$X.PMD"):
            target_script = script
            break

    assert target_script is not None, "Could not find $X.PMD script block"
    script_text = h.element_text(target_script, squash=False).replace("$X.PMD = ", "")

    idx = script_text.find(";$X.PPG")

    full_data = json.loads(script_text[:idx])

    for key in full_data:
        if len(full_data[key]):
            target_data = full_data[key].values()

    for item in target_data:
        crawl_item(item, context)
