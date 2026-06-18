import re

from zavod import Context
from zavod import helpers as h

BELARUS_DESC = "The sanctions of the Government of the Republic in view of the situation in Belarus"
RUS_DESC = "Sanction of the Government of the Republic to protect Estonia’s security and interests and against the actions of the Russian Federation in Ukraine"
HUMAN_RIGHTS_DESC = (
    "Sanction of the Government of the Republic to ensure following of human rights"
)


def crawl_item_belarus(context: Context, source_url: str, raw_name: str) -> None:
    match = re.search(r"([^\(\n]+)\s*(?:\((.+)\))?", raw_name)
    if match:
        name = match.group(1)
        or_names = h.multi_split(match.group(2), [";", ","])
    else:
        context.log.warning(f"Could not parse name: {raw_name}")
        return

    entity = context.make("Person")
    entity.id = context.make_id(raw_name)
    original = h.Names(name=raw_name)
    suggested = h.Names()
    entity.add("name", name, lang="eng")
    suggested.add("name", name)
    entity.add("topics", "sanction")

    for name in or_names:
        entity.add("name", name)
        suggested.add("name", name)
    is_irregular, suggested = h.check_names_regularity(entity, suggested)
    h.review_names(
        context,
        entity,
        original=original,
        suggested=suggested,
        is_irregular=is_irregular,
        default_accepted=True,
    )

    sanction = h.make_sanction(context, entity)
    sanction.add("sourceUrl", source_url)
    sanction.add("description", BELARUS_DESC)

    context.emit(entity)
    context.emit(sanction)


def crawl_item_human_rights(context: Context, source_url: str, raw_name: str) -> None:
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
    entity.id = context.make_id(raw_name)
    original = h.Names(name=raw_name)
    suggested = h.Names()
    h.apply_name(entity, first_name=first_name, last_name=last_name, lang="eng")
    suggested.add("name", h.make_name(first_name=first_name, last_name=last_name))
    entity.add("topics", "sanction")

    for alias in aliases:
        entity.add("alias", alias)
        suggested.add("alias", alias)
    is_irregular, suggested = h.check_names_regularity(entity, suggested)
    h.review_names(
        context,
        entity,
        original=original,
        suggested=suggested,
        is_irregular=is_irregular,
        default_accepted=True,
    )

    sanction = h.make_sanction(context, entity)
    sanction.add("sourceUrl", source_url)
    sanction.add("description", HUMAN_RIGHTS_DESC)

    context.emit(entity)
    context.emit(sanction)


def crawl_item_rus(context: Context, source_url: str, raw_name: str) -> None:
    match = re.search(r"^\d+\.\d*\.?", raw_name)
    if match:
        prefix = match.group()
        name_part = raw_name[len(prefix) :]
        parts = h.multi_split(name_part, [" (also ", " ("])
        name = parts[0].strip()
        alias = parts[1].rstrip(")") if len(parts) > 1 else ""
    else:
        context.log.warning("Could not parse name", raw_name)

    entity = context.make("Person")
    entity.id = context.make_id(raw_name)
    original = h.Names(name=raw_name)
    suggested = h.Names()
    entity.add("name", name, lang="eng")
    suggested.add("name", name)
    entity.add("alias", alias)
    suggested.add("alias", alias)
    entity.add("topics", "sanction")
    is_irregular, suggested = h.check_names_regularity(entity, suggested)
    h.review_names(
        context,
        entity,
        original=original,
        suggested=suggested,
        is_irregular=is_irregular,
        default_accepted=True,
    )

    sanction = h.make_sanction(context, entity)
    sanction.add("sourceUrl", source_url)
    sanction.add("description", RUS_DESC)

    context.emit(entity)
    context.emit(sanction)


def crawl_belarus(context: Context, url: str) -> None:
    doc = context.fetch_html(url)
    main_container = h.xpath_element(doc, ".//article")

    last_updated = h.xpath_element(
        main_container, ".//p[contains(text(), 'Last updated')]"
    )
    last_updated_parent = last_updated.getparent()
    assert last_updated_parent is not None
    last_updated_parent.remove(last_updated)

    list_container = h.xpath_element(main_container, ".//ol")
    list_container_parent = list_container.getparent()
    assert list_container_parent is not None
    list_container_parent.remove(list_container)

    # Find out of more lists are added without being <ol>
    h.assert_dom_hash(main_container, "c7f1460711ffebddcfd97263c5e3e4cc1df4cde1")

    # We find the list of names and iterate over them
    for item in h.xpath_elements(list_container, ".//li"):
        crawl_item_belarus(context, url, h.element_text(item))


def crawl_human_rights(context: Context, url: str) -> None:
    doc = context.fetch_html(url)
    main_container = h.xpath_element(doc, ".//article")

    # "According to&nbsp;directive", but let's not assume there's always a non-breaking space
    xpath = ".//p[contains(text(), 'According to')][contains(text(), 'directive')]/following-sibling::p[1]"
    directives = h.xpath_elements(main_container, xpath)
    assert len(directives) > 1, xpath
    for directive in directives:
        items = h.multi_split(h.element_text(directive, squash=False), "\n")
        assert len(items) > 1, items
        # The last element is non-breaking space (\xa0)
        for item in items:
            if item == "\xa0":
                continue
            crawl_item_human_rights(context, url, item)

        directive_parent = directive.getparent()
        assert directive_parent is not None
        directive_parent.remove(directive)


def crawl_rus(context: Context, url: str) -> None:
    doc = context.fetch_html(url)
    main_container = h.xpath_element(doc, ".//article")
    h.assert_dom_hash(main_container, "ee2ce6c8eaec412ae93ecb4e38a305ba627d7a47")
    raw_names = h.xpath_elements(main_container, ".//p")
    names = [h.element_text(p) for p in raw_names]
    for raw_name in names:
        crawl_item_rus(context, url, raw_name)


def crawl(context: Context) -> None:
    index_doc = context.fetch_html(context.data_url, absolute_links=True)
    anchors = h.xpath_elements(
        index_doc, ".//*[contains(text(), 'LIST OF SUBJECTS')]/ancestor::a"
    )
    assert len(anchors) == 3, "Could not find the links to the lists"
    for a in anchors:
        url = a.get("href")
        assert url is not None
        label = h.element_text(a).lower()

        if "belarus" in label:
            crawl_belarus(context, url)
        elif "human rights" in label:
            crawl_human_rights(context, url)
        elif "actions of the russian federation" in label:
            crawl_rus(context, url)
        else:
            context.log.warning("Unhandled list", url=url, label=label)
