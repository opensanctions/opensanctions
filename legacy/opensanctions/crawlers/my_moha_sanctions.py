import csv
import string
from normality import slugify
from pantomime.types import CSV

from zavod import Context
from zavod import helpers as h

PERSONS_CSV = "https://docs.google.com/spreadsheets/d/e/2PACX-1vT8r6lvQerO1tMNTUFaECYax-rU3Wra53U7T_yyRAZHMbGqvx6y93W8LO8OMatg08EQupZmAe7Wm-Wx/pub?gid=780934597&single=true&output=csv"
GROUPS_CSV = "https://docs.google.com/spreadsheets/d/e/2PACX-1vT8r6lvQerO1tMNTUFaECYax-rU3Wra53U7T_yyRAZHMbGqvx6y93W8LO8OMatg08EQupZmAe7Wm-Wx/pub?gid=676382776&single=true&output=csv"

SPLITS = [f"{l})" for l in string.ascii_letters]
FORMATS = ["%d %B %Y", "%d.%m.%Y"]
MONTHS = {"Mei": "May", "Februari": "February", "Ogos": "August"}


def parse_date(date: str):
    for malay, eng in MONTHS.items():
        date = date.replace(malay, eng)
    return h.parse_date(date, FORMATS)


def crawl_persons(context: Context):
    path = context.fetch_resource("persons.csv", PERSONS_CSV)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            data = {slugify(k, sep="_"): v for k, v in row.items()}
            entity = context.make("Person")
            code = data.pop("2_rujukan")
            entity.id = context.make_slug("person", code)
            entity.add("name", data.pop("3_nama"))
            entity.add("topics", "sanction")
            entity.add("birthDate", parse_date(data.pop("6_tarikh_lahir")))
            entity.add("birthPlace", data.pop("7_tempat_lahir"))

            aliases = h.multi_split(data.pop("8_nama_lain"), SPLITS)
            entity.add("alias", aliases)
            countries = h.multi_split(data.pop("9_warganegara"), SPLITS)
            entity.add("nationality", countries)

            entity.add("passportNumber", data.pop("10_nombor_pasport"))
            entity.add("idNumber", data.pop("11_nombor_kad_pengenalan"))
            entity.add("address", data.pop("12_alamat"))

            # short_alias = str(data.pop("5_nama_lain"))
            # if len(short_alias.strip()) > 1:
            #     entity.add("weakAlias", short_alias)

            sanction = h.make_sanction(context, entity)
            sanction.add("listingDate", parse_date(data.pop("13_tarikh_disenaraikan")))
            sanction.add("authorityId", code)

            context.emit(entity, target=True)
            context.emit(sanction)
            context.audit_data(data, ignore=["1_no"])


def crawl_groups(context: Context):
    path = context.fetch_resource("groups.csv", GROUPS_CSV)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            data = {slugify(k, sep="_"): v for k, v in row.items()}
            entity = context.make("Organization")
            code = data.pop("2_no_ruj")
            entity.id = context.make_slug("group", code)
            entity.add("name", data.pop("3_nama"))
            entity.add("topics", "sanction")
            aliases = h.multi_split(data.pop("4_alias"), SPLITS)
            entity.add("alias", aliases)
            countries = h.multi_split(data.pop("6_alamat"), SPLITS)
            entity.add("country", countries)

            short_alias = str(data.pop("5_nama_lain"))
            if len(short_alias.strip()) > 1:
                entity.add("weakAlias", short_alias)

            sanction = h.make_sanction(context, entity)
            sanction.add("listingDate", parse_date(data.pop("7_tarikh_disenaraikan")))
            sanction.add("authorityId", code)

            context.emit(entity, target=True)
            context.emit(sanction)
            context.audit_data(data, ignore=["1_no"])


def crawl(context: Context):
    crawl_persons(context)
    crawl_groups(context)
