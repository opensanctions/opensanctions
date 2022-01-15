from lxml import html
from normality import slugify, collapse_spaces
from pantomime.types import HTML

from opensanctions.core import Context
from opensanctions import helpers as h

FORMATS = ["%d/%b/%Y"]
REG_NRS = ["(Reg. No:", "(Reg. No.:", "(Reg. No.", "(Trade Register No.:"]


async def crawl(context: Context):
    path = await context.fetch_resource("source.html", context.dataset.data.url)
    await context.export_resource(path, HTML, title=context.SOURCE_TITLE)
    with open(path, "r", encoding="ISO-8859-1") as fh:
        doc = html.parse(fh)

    table = doc.find("//div[@id='viewcontainer']/table")
    headers = None
    for row in table.findall(".//tr"):
        if headers is None:
            headers = [slugify(c.text_content(), "_") for c in row.findall("./th")]
            continue
        cells = [collapse_spaces(c.text_content()) for c in row.findall("./td")]
        cells = dict(zip(headers, cells))
        cells.pop(None, None)

        full_name = name = cells.pop("name")
        registration_number = None
        for splitter in REG_NRS:
            if splitter in name:
                name, registration_number = name.split(splitter, 1)
                registration_number = registration_number.replace(")", "")

        country = cells.pop("nationality")
        country = country.replace("Non ADB Member Country", "")
        country = country.replace("Rep. of", "")
        entity = context.make("LegalEntity")
        entity.id = context.make_id(full_name, country)
        entity.add("name", name)
        entity.add("alias", cells.pop("othername_logo"))
        entity.add("topics", "debarment")
        entity.add("country", country)
        entity.add("registrationNumber", registration_number)

        sanction = h.make_sanction(context, entity)
        sanction.add("reason", cells.pop("grounds"))
        sanction.add("program", cells.pop("sanction_type"))
        date_range = cells.pop("effect_date_lapse_date", "")
        if "|" in date_range:
            start_date, end_date = date_range.split("|")
            sanction.add("startDate", h.parse_date(start_date.strip(), FORMATS))
            sanction.add("endDate", h.parse_date(end_date.strip(), FORMATS))

        address = h.make_address(context, full=cells.pop("address"), country=country)
        await h.apply_address(context, entity, address)

        await context.emit(entity, target=True)
        await context.emit(sanction)
