import re
import requests

from zavod import Context, helpers as h
from zavod.logic.pep import categorise


BELARUS_URL = "https://www.vm.ee/en/sanctions-government-republic-view-situation-belarus"
HUMAN_RIGHTS_URL = "https://www.vm.ee/subjektide-nimekiri-vabariigi-valitsuse-sanktsioon-inimoiguste-jargimise-tagamiseks"


def crawl_item_belarus(raw_name: str, context: Context):

    match = re.search(r"([^\(\n]+)\s*(?:\((.+)\))?", raw_name)
    if match:
        name = match.group(1)
        aliases = match.group(2).split("; ") if match.group(2) else []
    else:
        context.log.warning(f"Could not parse name: {raw_name}")
        return

    entity = context.make("Person")
    entity.id = context.make_id(name)
    entity.add("name", name)
    entity.add("topics", "sanction")
    
    for alias in aliases:
        entity.add("alias", alias)

    context.emit(entity, target=True)

def crawl_item_human_rights(raw_name: str, context: Context):

    match = re.search(r'\d+\.\s*([^(\n]+)(?:\s*\(also\s*([^)]+)\))?', raw_name)
    if match:
        name = match.group(1).strip()
        aliases = match.group(2).split("; ") if match.group(2) else []

    last_name, first_name = name.split(", ")

    entity = context.make("Person")
    entity.id = context.make_id(name)
    h.apply_name(entity, first_name=first_name, last_name=last_name)
    entity.add("topics", "sanction")
    
    for alias in aliases:
        entity.add("alias", alias)

    context.emit(entity, target=True)

def crawl(context: Context):
    
    
    response = context.fetch_html(BELARUS_URL)
    xpath = ".//*[@class='col-lg-6 col-xl-6 mb-2 mb-xl-0']/div/div/ol/li"

    # We find the list of names and iterate over them
    for item in response.findall(xpath):
        crawl_item_belarus(item.text_content(), context)

    response = context.fetch_html(HUMAN_RIGHTS_URL)
    xpath = ".//*[@class='clearfix paragraph paragraph--type--text-section paragraph--view-mode--default']/div/p"

    paragraph = response.find(xpath).text_content()

    # Here the list is in the same paragraph, so we split it by new lines
    # The last element is non-breaking space (\xa0)
    for item in paragraph.split("\n")[:-1]:
        crawl_item_human_rights(item, context)
