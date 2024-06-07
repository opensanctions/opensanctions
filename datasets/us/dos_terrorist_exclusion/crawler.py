from zavod import Context, helpers as h

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
}


def crawl_item(raw_name: str, context: Context):

    entity = context.make("LegalEntity")

    names = h.multi_split(
        raw_name, ["; a.k.a.", "(a.k.a", "(f.k.a.", "; f.k.a", ", a.k.a"]
    )

    entity.id = context.make_id(names)

    for name in names:
        entity.add("name", name)

    entity.add("topics", "crime.terror")
    entity.add(
        "program",
        "Section 411 of the USA PATRIOT ACT of 2001 (8 U.S.C. § 1182) Terrorist Exclusion List (TEL) ",
    )

    context.emit(entity, target=True)


def crawl(context: Context):

    doc = context.fetch_html(context.data_url, headers=HEADERS)

    # Find the title of the list by the text, then find the next sibling (which is the list), then get all the list items texts
    xpath = ".//*[contains(text(), 'Terrorist Exclusion List Designees (alphabetical listing)')]/../following-sibling::*[1]/li/text()"

    for item in doc.xpath(xpath):
        crawl_item(item, context)
