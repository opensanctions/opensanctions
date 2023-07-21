import string
from lxml import html
from pantomime.types import HTML
from normality import collapse_spaces
from followthemoney import model
from followthemoney.types import registry

from opensanctions.core import Context
from opensanctions import helpers as h
from opensanctions.util import multi_split

SPLITS = ["%s)" % c for c in string.ascii_lowercase]
SPLITS = SPLITS + ["; ", " и "]

DATE_FORMATS = ["%d %b. %Y", "%d %B %Y", "%d.%m.%Y"]


def clean_date(text):
    return h.parse_date(text, DATE_FORMATS)


def letter_split(text):
    return multi_split(text, SPLITS)


def maybe_rsplit(text, splitter):
    if text is None:
        return None, None
    if splitter not in text:
        return text, None
    text, remain = text.rsplit(splitter, 1)
    remain = remain.strip().replace("д/о", "")
    if not len(remain):
        remain = None
    return text, remain


def crawl(context: Context):
    path = context.fetch_resource("source.html", context.data_url)
    context.export_resource(path, HTML, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        doc = html.parse(fh)
    tables = doc.findall(".//table")
    assert len(tables) == 1
    rows = tables[0].findall(".//tr")
    for row in rows[2:]:
        cells = [collapse_spaces(c.text_content()) for c in row.findall("./td")]
        index = cells[0]
        body = cells[1]
        decision = cells[2]
        un_id = cells[3]
        listing_date = cells[4]
        entity = context.make("Thing")
        entity.id = context.make_slug(index, un_id)
        entity.add("notes", h.clean_note(cells[5]))

        sanction = h.make_sanction(context, entity)
        sanction.add("listingDate", clean_date(listing_date))
        sanction.add("program", decision)
        sanction.add("recordId", un_id)

        body, gender = maybe_rsplit(body, "пол:")
        entity.add_cast("Person", "gender", gender)
        body, gender = maybe_rsplit(body, "Пол:")
        entity.add_cast("Person", "gender", gender)
        body, location = maybe_rsplit(body, "местонахождение:")
        entity.add_cast("LegalEntity", "country", location)
        body, imo_num = maybe_rsplit(body, "Присвоенный ИМО номер компании:")
        body, imo_num = maybe_rsplit(body, "Номер ИМО:")
        body, emails = maybe_rsplit(body, "Адрес эл. почты:")
        for email in letter_split(emails):
            entity.add_cast("LegalEntity", "email", email)
        body, fax = maybe_rsplit(body, "Номер факса:")
        body, fax = maybe_rsplit(body, "Факс:")
        body, phones = maybe_rsplit(body, "Номера телефонов:")
        for phone in letter_split(phones):
            entity.add_cast("LegalEntity", "phone", phone)
        body, phones = maybe_rsplit(body, "Тел.:")
        for phone in letter_split(phones):
            entity.add_cast("LegalEntity", "phone", phone)
        body, swift = maybe_rsplit(body, "СВИФТ-код:")
        entity.add_cast("LegalEntity", "swiftBic", swift)
        body, swift = maybe_rsplit(body, "СВИФТ/БИК-код:")
        entity.add_cast("LegalEntity", "swiftBic", swift)

        body, other_info = maybe_rsplit(body, "Прочая информация:")
        entity.add_cast("Thing", "notes", other_info)
        body, listing_date = maybe_rsplit(body, "Дата внесения в перечень:")
        body, addresses = maybe_rsplit(body, "Адрес:")
        for address in letter_split(addresses):
            country = address
            if ", " in country:
                country = address.rsplit(", ", 1)
            code = registry.country.clean(country, fuzzy=True)
            obj = h.make_address(context, full=address, country_code=code)
            h.apply_address(context, entity, obj)
            entity.add("country", code)
        body, national_ids = maybe_rsplit(body, "Национальный идентификационный номер:")
        for national_id in letter_split(national_ids):
            entity.add_cast("LegalEntity", "idNumber", national_id)
        body, passport_nos = maybe_rsplit(body, "Паспорт №:")
        for passport_no in letter_split(passport_nos):
            entity.add_cast("Person", "passportNumber", passport_no)
        body, citizenship = maybe_rsplit(body, "Гражданство:")
        entity.add_cast("Person", "nationality", citizenship)
        aka = "На основании менее достоверных источников также известен как:"
        body, aka = maybe_rsplit(body, aka)
        entity.add("alias", letter_split(aka))
        strong_aka = "На основании достоверных источников также известен как:"
        body, strong_aka = maybe_rsplit(body, strong_aka)
        entity.add("alias", letter_split(strong_aka))
        body, rik_no = maybe_rsplit(body, "Р.И.К.:")

        body, birth_place = maybe_rsplit(body, "Место рождения:")
        entity.add_cast("Person", "birthPlace", birth_place)
        body, birth_dates = maybe_rsplit(body, "Дата рождения:")
        for birth_date in letter_split(birth_dates):
            entity.add_cast("Person", "birthDate", clean_date(birth_date))
        body, position = maybe_rsplit(body, "Должность:")
        entity.add_cast("Person", "position", position)
        body, job = maybe_rsplit(body, "Обращение:")
        entity.add_cast("Person", "position", job)

        body, aliases = maybe_rsplit(body, "Другие названия:")
        entity.add("alias", letter_split(aliases))

        body, aliases = maybe_rsplit(body, "Вымышленные названия:")
        entity.add("alias", letter_split(aliases))

        names = body.split(", ")
        entity.add("name", names)
        # context.inspect(names)

        if entity.schema.name == "Thing":
            entity.schema = model.get("LegalEntity")

        context.emit(entity, target=True)
        context.emit(sanction)
