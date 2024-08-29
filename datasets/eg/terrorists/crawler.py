from openpyxl import load_workbook
from pantomime.types import XLSX
from datetime import datetime

from zavod import Context, helpers as h


def arabic_to_latin(arabic_date):
    arabic_numerals = "٠١٢٣٤٥٦٧٨٩"
    latin_numerals = "0123456789"
    transtable = str.maketrans(arabic_numerals, latin_numerals)
    return arabic_date.translate(transtable)


def parse_date(date_str: str) -> datetime:

    if not date_str:
        return None

    latin_date = arabic_to_latin(date_str)

    # We first check if it's just one date
    try:
        return datetime.strptime(latin_date, "%Y/%m/%d")
    except ValueError:
        # Otherwise we check if the first line is a date
        try:
            return datetime.strptime(latin_date.split("\n")[0], "%d/%m/%Y")
        except ValueError:
            return None


def crawl_terrorist(input_dict: dict, context: Context):

    name = input_dict.pop("name")
    case_number = input_dict.pop("case_number")

    person = context.make("Person")
    person.id = context.make_id(name, case_number)
    person.add("name", name)
    person.add("alias", input_dict.pop("alias"))
    person.add("nationality", input_dict.pop("nationality"), lang="ara")
    person.add("country", "eg")
    person.add("passportNumber", input_dict.pop("passport"))
    person.add("idNumber", input_dict.pop("national_id"))
    person.add("topics", "sanction.counter")

    sanction = h.make_sanction(context, person, case_number)
    sanction.add("listingDate", parse_date(input_dict.pop("date_of_publication")))
    sanction.add("description", f"Case number: {case_number}")
    sanction.add("authorityId", input_dict.pop("terrorist_designation_decision_number"))
    sanction.add(
        "description",
        f"Publication Page: {input_dict.pop('number_of_publication')}",
    )

    context.emit(person, target=True)
    context.emit(sanction)
    context.audit_data(input_dict)


def crawl_terrorist_entities(input_dict: dict, context: Context):

    name = input_dict.pop("name")
    case_number = input_dict.pop("case_number")

    if not name:
        return

    entity = context.make("LegalEntity")
    entity.id = context.make_id(name, case_number)
    entity.add("name", name)
    entity.add("country", "eg")
    entity.add("topics", "sanction.counter")

    gazette_issue = input_dict.pop("issue_in_official_gazette")
    sanction = h.make_sanction(context, entity, case_number + gazette_issue)
    sanction.add("description", f"Case number: {case_number}")
    sanction.add("description", f"Issue in official gazette: {gazette_issue}")
    sanction.add("summary", input_dict.pop("updates"), lang="ara")

    context.emit(entity, target=True)
    context.emit(sanction)
    context.audit_data(input_dict, ignore=["series"])


def crawl_legal_persons(input_dict: dict, context: Context):

    name = input_dict.pop("name")
    case_number = input_dict.pop("case_number")

    if not name:
        return

    entity = context.make("LegalEntity")
    entity.id = context.make_id(name, case_number)
    entity.add("name", name)
    entity.add("country", "eg")

    entity.add("address", input_dict.pop("headquarters"))
    entity.add("registrationNumber", input_dict.pop("commercial_registration_number"))
    entity.add("topics", "sanction.counter")

    gazette_issue = input_dict.pop("issue_in_official_gazette")
    sanction = h.make_sanction(context, entity, case_number + gazette_issue)
    sanction.add("description", f"Case number: {case_number}")
    sanction.add("description", f"Issue in official gazette: {gazette_issue}")
    sanction.add("summary", input_dict.pop("updates"), lang="ara")

    context.emit(entity, target=True)
    context.emit(sanction)
    context.audit_data(input_dict, ignore=["series"])


def crawl(context: Context):
    response = context.fetch_html(context.data_url)
    response.make_links_absolute(context.data_url)

    excel_link = response.find(".//*[@class='LinkStyle AutoDownload']").get("href")

    path = context.fetch_resource("list.xlsx", excel_link)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    wb = load_workbook(path, read_only=True)

    for item in h.parse_sheet(context, wb["الإرهابيين"], header_lookup="columns"):
        crawl_terrorist(item, context)

    for item in h.parse_sheet(
        context, wb["الكيانات الإرهابية"], skiprows=1, header_lookup="columns"
    ):
        crawl_terrorist_entities(item, context)

    for item in h.parse_sheet(
        context, wb["الشخصيات الاعتبارية"], skiprows=1, header_lookup="columns"
    ):
        crawl_legal_persons(item, context)
