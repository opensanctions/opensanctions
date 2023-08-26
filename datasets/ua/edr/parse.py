from zipfile import ZipFile
from lxml import etree
from typing import IO
from lxml.etree import _Element as Element, tostring
from followthemoney.util import make_entity_id

from zavod import Context

REMOVE = [
    "КІНЦЕВИЙ БЕНЕФІЦІАР ВЛАСНИК(КОНТРОЛЕР)",
    "КІНЦЕВИЙ БЕНЕФЕЦІАРНИЙ ВЛАСНИК(КОНТРОЛЕР)",
    "КІНЦЕВИЙ БЕНЕФІЦІАРНИЙ ВЛАСНИК(КОНТРОЛЕР)",
    "КІНЦЕВИЙ БЕНЕФІЦІАРНИЙ ВЛАСНИК",
    "(КОНТРОЛЕР)",
    "акціонери akcíoneri",
    "КІНЦЕВИЙ БЕНЕФІЦІАРНИЙ ВЛВСНИК  У ЮРИДИЧНОЇ ОСОБИ ВІДСУТНІЙ",
]


def tag_text(el: Element) -> str:
    return tostring(el, encoding="utf-8").decode("utf-8")


def parse_owner(context: Context, company_id: str, unique_id: str, el: Element):
    if el.text is None:
        return
    if "причина відсутності:" in el.text:
        return
    if el.text.startswith("релігійна громада в кількості"):
        return
    owner = context.make("LegalEntity")
    owner.id = context.make_id(unique_id, el.text)
    ownership = context.make("Ownership")
    ownership.id = context.make_id(unique_id, el.tag, el.text)
    ownership.add("owner", owner)
    ownership.add("asset", company_id)
    ownership.add("role", el.tag)
    text = el.text
    for rem in REMOVE:
        text = text.replace(rem, "")
    text = text.strip().strip("-")
    parts = text.rsplit(", розмір частки -", 1)
    text = parts[0]
    if len(parts) > 1:
        ownership.add("sharesValue", parts[1])

    parts = text.split(",", 1)
    name = text
    if len(parts) > 1:
        name = parts[1]

    name = name.strip()
    if not len(name):
        return

    owner.add("name", name)
    if name != el.text:
        owner.add("notes", el.text)
    # print(name, latinize_text(name))
    if len(owner.properties):
        context.emit(owner)
        context.emit(ownership)


def parse_uo(context: Context, fh: IO[bytes]):
    for idx, (_, el) in enumerate(etree.iterparse(fh, tag="RECORD")):
        if idx > 0 and idx % 10000 == 0:
            context.log.info("Parse UO records: %d..." % idx)
        # print(tag_text(el))

        company = context.make("Company")
        long_name = el.findtext("./NAME")
        short_name = el.findtext("./SHORT_NAME")
        edrpou = el.findtext("./EDRPOU")
        if short_name and len(short_name.strip()):
            company.add("name", short_name)
            company.add("alias", long_name)
        else:
            company.add("name", long_name)

        unique_id = make_entity_id(edrpou, short_name, long_name)
        company.id = context.make_slug(edrpou, unique_id, strict=False)
        if company.id is None:
            context.log.warn("Could not generate company ID", xml=tag_text(el))
            continue
        company.add("registrationNumber", edrpou)
        company.add("jurisdiction", "ua")
        company.add("address", el.findtext("./ADDRESS"))
        company.add("classification", el.findtext("./KVED"))
        company.add("status", el.findtext("./STAN"))
        context.emit(company)

        for boss in el.findall(".//BOSS"):
            name = boss.text
            director = context.make("Person")
            director.id = context.make_id(unique_id, name)
            director.add("name", name)
            if not len(director.properties):
                continue
            context.emit(director)

            directorship = context.make("Directorship")
            directorship.id = context.make_id(unique_id, "BOSS", name)
            directorship.add("organization", company)
            directorship.add("director", director)
            context.emit(director)

        for founder in el.findall("./FOUNDERS/FOUNDER"):
            parse_owner(context, company.id, unique_id, founder)
            # founder_name = founder.text
            # capital = None
            # if founder_name is None:
            #     continue
            # if ", розмір частки -" not in founder.text:
            #     print("FOUNDER", founder.text)

        for bene in el.findall("./BENEFICIARIES/BENEFICIARY"):
            parse_owner(context, company.id, unique_id, bene)
            # if bene.text is None:
            #     continue
            # if "причина відсутності:" in bene.text:
            #     continue
            # parts = bene.text.split(";", 2)
            # if len(parts) != 3:
            #     print("BENE", parts)

        # TODO: beneficiary
        # TODO: founder
        el.clear()


# def parse_fop(context: Zavod, fh: BinaryIO):
#     for idx, (_, el) in enumerate(etree.iterparse(fh, tag="RECORD")):
#         if idx > 0 and idx % 10000 == 0:
#             context.log.info("Parse FOP records: %d..." % idx)
#         print(tag_text(el))
#         el.clear()


def crawl(context: Context):
    path = context.fetch_resource("source.zip", context.data_url)
    context.log.info("Parsing: %s" % path)
    with ZipFile(path, "r") as zip:
        for name in zip.namelist():
            if not name.lower().endswith(".xml"):
                continue
            with zip.open(name, "r") as fh:
                if "EDR_UO" in name:
                    parse_uo(context, fh)
                # if "EDR_FOP" in name:
                #     parse_fop(context, fh)
