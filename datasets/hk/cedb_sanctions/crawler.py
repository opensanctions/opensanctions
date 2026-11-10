from rigour.mime.types import XML
from zavod.shed.un_sc import get_legal_entities, get_persons, load_un_sc

from zavod import Context
from zavod import helpers as h


def crawl_file(context: Context, name: str, url: str, title: str, prefix: str) -> None:
    path = context.fetch_resource(f"{name}.xml", url)
    context.export_resource(path, XML, title=f"Source data - {title}")
    doc = context.parse_resource_xml(path)
    for _node, person in get_persons(context, prefix, doc):
        context.emit(person)
    for _node, entity in get_legal_entities(context, prefix, doc):
        context.emit(entity)


def crawl(context: Context) -> None:
    # using the UNSC's prefix like for TR and PL
    un_meta, _ = load_un_sc(context)
    prefix = un_meta.prefix
    assert prefix is not None, "UN SC dataset prefix is required"

    # The dataset page lists one XML resource per regime, each linked twice
    # (details and download).
    page = context.fetch_html(context.data_url, cache_days=1, absolute_links=True)
    files: dict[str, str] = {}
    for link in h.xpath_elements(page, "//a[contains(@href, '/datagovhk/TFSlists/')]"):
        url = link.get("href")
        if url is None or not url.endswith(".xml"):
            continue
        name = url.rsplit("/", 1)[-1].removesuffix(".xml")
        files[name] = url
    if len(files) == 0:
        raise ValueError(f"No XML resources found on {context.data_url}")

    # Compare against the files we expect, so that a regime being added to or
    # dropped from the page is noticed. Unexpected files are still crawled.
    expected: list[str] = context.dataset.config["expected_files"]
    missing = set(expected) - set(files)
    if missing:
        context.log.warning("Expected files missing from page", files=sorted(missing))
    unexpected = set(files) - set(expected)
    if unexpected:
        context.log.warning("Unexpected files on page", files=sorted(unexpected))

    for name, url in sorted(files.items()):
        title = name.replace("_", " ").title()
        crawl_file(context, name, url, title, prefix)
