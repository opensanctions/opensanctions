from typing import Dict
from rigour.mime.types import PDF
from pdfplumber.page import Page

from zavod import Context, helpers as h
from zavod.shed.zyte_api import fetch_html, fetch_resource


def crawl_row(context, name, category, start_date):
    full_name = None
    alias = None
    if "(aka" in name:
        full_name, alias = name.split("(aka")
        alias = alias.replace(")", "")
    if "(dba" in name:
        full_name, alias = name.split("(dba")
        alias = alias.replace(")", "")

    owners = []
    if "Owner)" in name and len(name.split(" (")) == 2:
        full_name, owner_names = name.split(" (")
        owner_names = owner_names.replace("Owner)", "")
        owners = h.multi_split(owner_names, ["Owner &", "&", "Owner ;", ";"])

    sector = None
    name_parts = h.multi_split(name, [","])
    if full_name is None and len(name_parts) > 2:
        full_name = ", ".join(name_parts[:2])
        sector = name_parts[2:]

    if full_name is None:
        full_name = name

    # If it's still not clean
    related_entities = []
    if context.lookup("unclean_names", full_name) is not None:
        res = context.lookup("override", name)
        if res:
            full_name = res.name
            sector = res.sector
            alias = res.alias
            related_entities = res.related_entities
        else:
            context.log.warning(
                "Probably not a clean name", name=full_name, full_string=name
            )

    entity = context.make("LegalEntity")
    entity.id = context.make_id(full_name, sector)
    entity.add("name", full_name)
    entity.add("alias", alias)
    entity.add("country", "us")
    entity.add("topics", "debarment")
    entity.add("sector", sector)
    entity.add("sector", category)

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "startDate", start_date)

    context.emit(entity)
    context.emit(sanction)

    for owner in owners:
        owner_entity = context.make("LegalEntity")
        owner_entity.id = context.make_id(owner)
        owner_entity.add("name", owner)
        owner_entity.add("country", "us")
        owner_entity.add("topics", "debarment")

        relation = context.make("Ownership")
        relation.id = context.make_id(entity.id, owner_entity.id)
        relation.add("asset", entity)
        relation.add("owner", owner_entity)

        sanction = h.make_sanction(context, entity)
        h.apply_date(sanction, "startDate", start_date)
        context.emit(owner_entity)
        context.emit(relation)

    for item in related_entities if related_entities else []:
        related = context.make("LegalEntity")
        related.id = context.make_id(item["name"], entity.id)
        related.add("name", item["name"])
        related.add("country", "us")
        related.add("topics", "debarment")

        relation = context.make(item["schema"])
        relation.id = context.make_id(entity.id, related.id)
        relation.add(item["from_prop"], entity)
        relation.add(item["to_prop"], related)

        sanction = h.make_sanction(context, entity)
        h.apply_date(sanction, "startDate", start_date)
        context.emit(related)
        context.emit(relation)
        context.emit(sanction)


def crawl_data_url(context: Context):
    file_xpath = "//a[contains(., 'PDF Version')]"
    doc = fetch_html(context, context.data_url, file_xpath)
    doc.make_links_absolute(context.data_url)
    return doc.xpath(file_xpath)[0].get("href")


def page_settings(page: Page) -> Dict:
    settings = {"join_y_tolerance": 15}
    if page.page_number == 1:
        table_start = 510
        # im = page.to_image()
        # im.draw_hline(table_start)
        # im.save("page.png")
        page = page.crop((0, table_start, page.width - 15, page.height - 15))
    return page, settings


def crawl(context: Context) -> None:
    # The .xls file first seemed to work, then a newer file couldn't be parsed
    # as a valid Compond Document file.
    # xlrd gave "xlrd.compdoc.CompDocError: MSAT extension: accessing sector ..."
    # https://stackoverflow.com/questions/74262026/reading-the-excel-file-from-python-pandas-given-msat-extension-error
    # didn't work.

    # First we find the link to the PDF file
    url = crawl_data_url(context)
    _, _, _, path = fetch_resource(context, "source.pdf", url, expected_media_type=PDF)
    context.export_resource(path, PDF, title=context.SOURCE_TITLE)

    try:
        category = None
        for row in h.parse_pdf_table(context, path, page_settings=page_settings):
            name = row.pop("name_of_provider").replace("\n", " ")
            if name == "":
                continue
            start_date = row.pop("suspension_effective_date")
            if start_date == "":
                if context.lookup("categories", name):
                    category = name
                else:
                    category = None
                    context.log.warning(
                        "Unexpected category. Confirm we're parsing the PDF correctly.",
                        category=name,
                    )
                continue
            crawl_row(context, name, category, start_date)
    except Exception as e:
        if "No table found on page 49" in str(e):
            # this is where the right-hand side of the table starts wrapping
            pass
        else:
            if "No table found on page" in str(e):
                raise RuntimeError(
                    "PDF pages changed. See if they've upgraded to xlsx or update max page."
                )
            else:
                raise
