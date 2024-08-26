from lxml import html, etree
from zavod import Context

BASE_URL = "https://www.navy.mil"
API_URL = "https://www.navy.mil/API/ArticleCS/Public/GetList?moduleID=709&dpage=%d&TabId=119&language=en-US"


def crawl_entity(context: Context, item_html: str) -> None:
    doc = html.fromstring(item_html)
    link_el = doc.find(".//a")
    if link_el is None:
        return

    url = link_el.get("href")
    name = link_el.find(".//h2").text_content().strip()
    position = link_el.find(".//h3").text_content().strip()

    entity = context.make("Person")
    entity.id = context.make_id(name)
    entity.add("name", name)
    entity.add("sourceUrl", url)
    entity.add("position", position)

    context.emit(entity, target=True)


def process_page(context: Context, page_number: int):
    url = API_URL % page_number
    data = context.fetch_json(url)

    if not data:
        return False, False

    if isinstance(data, dict):
        html_data = data.get("data")
        done = data.get("done", True)
    else:
        root = etree.fromstring(data)
        html_data = root.find(".//data").text
        done = root.find(".//done").text.lower() == "true"

    if not html_data:
        return False, done

    items = html.fromstring(html_data).findall(".//li")
    for item in items:
        item_html = html.tostring(item, encoding="unicode")
        crawl_entity(context, item_html)

    return True, done


def crawl(context: Context):
    page_number = 1
    done = False
    error_pages = []

    while not done:
        context.log.info(f"Fetching page {page_number}")
        success, done_flag = process_page(context, page_number)
        if not success:
            error_pages.append(page_number)
        done = done_flag
        page_number += 1

    # Retry the error pages
    for page_number in error_pages:
        context.log.info(f"Retrying page {page_number}")
        success, _ = process_page(context, page_number)
        if not success:
            context.log.error(f"Failed again for page {page_number}")
