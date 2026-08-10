import json
import re

from zavod import Context
from zavod import helpers as h

# The website column lists several URLs per entry, separated by a pipe, whitespace,
# or both. Whitespace only separates where the next URL begins, because long URLs
# are wrapped across lines mid-query.
WEBSITE_SPLIT = re.compile(r"\s*\|\s*|\s+(?=https?://|www\.)", re.IGNORECASE)


def split_websites(value: str) -> list[str]:
    websites: list[str] = []
    for part in WEBSITE_SPLIT.split(value):
        part = part.strip()
        if len(part):
            websites.append(part)
    return websites


def crawl_item(input_dict: dict[str, str], context: Context) -> None:
    name = input_dict.pop("name")
    has_name = len(name.strip()) > 0

    # The source sometimes publishes a row with an empty name column. The remark
    # still describes the scheme being warned about, so a lookup keyed on the
    # remark can allow such a row to be emitted without a name. Rows that aren't
    # listed there are skipped, so new ones surface as a warning for review.
    if not has_name and context.lookup("no_name", input_dict["remark"]) is None:
        context.log.warning("Skipping entry without a name", data=input_dict)
        return

    # If it's a potential clone, we remove the "potential clone" from the name
    potential_clone = "Potential clone entity – " in name
    name = name.replace("Potential clone entity – ", "")

    entity = context.make("LegalEntity")
    remark = input_dict.pop("remark")
    # Rows without a name are identified by their remark instead.
    entity.id = context.make_id(name) if has_name else context.make_id(remark)
    if has_name:
        entity.add("name", name.split(" / "))
    address = input_dict.pop("address").replace("N/A", "")
    addresses = re.split(r"\b\d\) ", address)
    entity.add("address", addresses)
    entity.add("notes", remark.split("\n"))
    entity.add("topics", "crime.fin")
    entity.add("sourceUrl", input_dict.pop("url"))
    if potential_clone:
        entity.add("description", "Potential clone entity")

    entity.add("website", split_websites(input_dict.pop("website")))

    # The bank info column is new and so far only ever holds "N/A" - drop that so
    # audit_data still flags it once the source starts populating it.
    if input_dict.get("bankInfo") == "N/A":
        input_dict.pop("bankInfo")

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
