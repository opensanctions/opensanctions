import re

from zavod import Context
from zavod import helpers as h

BELARUS_DESC = "The sanctions of the Government of the Republic in view of the situation in Belarus"
HUMAN_RIGHTS_DESC = (
    "Sanction of the Government of the Republic to ensure following of human rights"
)


def crawl_item_belarus(context: Context, source_url, raw_name: str):
    match = re.search(r"([^\(\n]+)\s*(?:\((.+)\))?", raw_name)
    if match:
        name = match.group(1)
        or_names = h.multi_split(match.group(2), [";", ","])
    else:
        context.log.warning(f"Could not parse name: {raw_name}")
        return

    entity = context.make("Person")
    entity.id = context.make_id(name)
    entity.add("name", name, lang="eng")
    entity.add("topics", "sanction")

    for name in or_names:
        entity.add("name", name)

    sanction = h.make_sanction(context, entity)
    sanction.add("sourceUrl", source_url)
    sanction.add("description", BELARUS_DESC)

    context.emit(entity)
    context.emit(sanction)


def crawl_item_human_rights(context: Context, source_url, raw_name: str):
    # "1. Mr. John Doe (also known as John Smith)"
    # "1.23 Mr. John Doe (also known as John Smith)"
    match = re.search(r"^\d+\.\d*\.?\s*([^(\n]+)(?:\s*\(also\s*([^)]+)\))?", raw_name)
    if match:
        name = match.group(1).strip()
        aliases = match.group(2).split("; ") if match.group(2) else []
    else:
        context.log.warning("Could not parse name", raw_name=raw_name)
        return

    last_name, first_name = name.split(", ")

    entity = context.make("Person")
    entity.id = context.make_id(name)
    h.apply_name(entity, first_name=first_name, last_name=last_name, lang="eng")
    entity.add("topics", "sanction")

    for alias in aliases:
        entity.add("alias", alias)

    sanction = h.make_sanction(context, entity)
    sanction.add("sourceUrl", source_url)
    sanction.add("description", HUMAN_RIGHTS_DESC)

    context.emit(entity)
    context.emit(sanction)


def crawl_belarus(context, url):
    doc = context.fetch_html(url)
    main_container = doc.xpath(".//article")
    assert len(main_container) == 1, (
        main_container,
        "Could not find the main container",
    )

    last_updated = main_container[0].xpath(".//p[contains(text(), 'Last updated')]")
    assert len(last_updated) == 1, (
        last_updated,
        "Could not find the last updated date",
    )
    last_updated[0].getparent().remove(last_updated[0])

    list_container = main_container[0].xpath(".//ol")
    assert len(list_container) == 1, (
        list_container,
        "Could not find the list container",
    )
    list_container[0].getparent().remove(list_container[0])

    # Find out of more lists are added without being <ol>
    h.assert_dom_hash(main_container[0], "c7f1460711ffebddcfd97263c5e3e4cc1df4cde1")

    # We find the list of names and iterate over them
    for item in list_container[0].findall(".//li"):
        crawl_item_belarus(context, url, item.text_content())


def crawl_human_rights(context, url):
    doc = context.fetch_html(url)
    main_container = doc.xpath(".//article")
    assert len(main_container) == 1, (
        main_container,
        "Could not find the main container",
    )

    # "According to&nbsp;directive", but let's not assume there's always a non-breaking space
    xpath = ".//p[contains(text(), 'According to')][contains(text(), 'directive')]/following-sibling::p[1]"
    directives = main_container[0].xpath(xpath)
    assert len(directives) > 1, xpath
    for directive in directives:
        items = h.multi_split(directive.text_content(), "\n")
        assert len(items) > 1, items
        # The last element is non-breaking space (\xa0)
        for item in items:
            if item == "\xa0":
                continue
            crawl_item_human_rights(context, url, item)

        directive.getparent().remove(directive)


def crawl(context: Context):
    index_doc = context.fetch_html(context.data_url)
    index_doc.make_links_absolute(context.data_url)

    anchors = index_doc.xpath(".//*[contains(text(), 'LIST OF SUBJECTS')]/ancestor::a")
    assert len(anchors) == 2, "Could not find the links to the lists"
    for a in anchors:
        url = a.get("href")
        label = a.text_content().lower()

        if "belarus" in label:
            crawl_belarus(context, url)
        elif "human rights" in label:
            crawl_human_rights(context, url)
        else:
            context.log.warning("Unhandled list", url=url, label=label)
