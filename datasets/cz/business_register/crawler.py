import tarfile
from normality import slugify
from typing import Optional, IO

from followthemoney.util import make_entity_id
from lxml import etree
from zavod import Context, Entity
from zavod import helpers as h
from zavod.helpers.xml import ElementOrTree


def company_id(
    context: Context, reg_nr: Optional[str], name: Optional[str] = None
) -> Optional[str]:
    if reg_nr:
        return f"oc-companies-cz-{reg_nr}"
    return context.make_slug("company", name)


def person_id(
    context: Context, name: str, address: Optional[str], company_id: str
) -> Optional[str]:
    if slugify(address) is not None:
        return context.make_slug("person", name, make_entity_id(address))
    return context.make_slug("person", name, make_entity_id(company_id))


def make_address(tree: Optional[ElementOrTree] = None) -> Optional[str]:
    if tree is None:
        return None

    components = {
        "stat": "state",
        "psc": "postal_code",
        "okres": "county",
        "obec": "city",
        "ulice": "street",
        "cisloTxt": "house_number",
    }
    data = {}
    for path, key in components.items():
        data[key] = tree.findtext(path)

    return h.format_address(country_code="cz", **data)


def make_company(context: Context, tree: ElementOrTree) -> Optional[Entity]:
    tree = h.remove_namespace(tree)
    name = tree.findtext(".//ObchodniFirma")
    proxy = context.make("Company")
    reg_nr = tree.findtext(".//ICO")
    proxy.id = company_id(context, reg_nr, name)
    if proxy.id is not None:
        proxy.add("name", name)
        proxy.add("registrationNumber", reg_nr)
        proxy.add("address", make_address(tree.find(".//Sidlo")))
        proxy.add("incorporationDate", tree.findtext(".//DatumZapisu"))
        proxy.add("dissolutionDate", tree.findtext(".//DatumVymazu"))
        return proxy


def parse_xml(context: Context, reader: IO[bytes]):
    tree = etree.parse(reader)
    company = make_company(context, tree)
    if company is None or company.id is None:
        return
    context.emit(company)
    for member in tree.findall(".//Clen"):
        proxy = context.make("Person")
        # context.inspect(member)
        first_name = member.findtext("fosoba/jmeno")
        last_name = member.findtext("fosoba/prijmeni")
        h.apply_name(proxy, first_name=first_name, last_name=last_name)
        proxy.add("title", member.findall("fosoba/titulPred"))
        address = make_address(member.find("fosoba/adresa"))
        proxy.add("address", address)
        name = proxy.first("name")
        if name is None:
            continue
        proxy.id = person_id(context, name, address, company.id)
        if proxy.id is None:
            continue
        context.emit(proxy)

        role = member.findtext("funkce/nazev")
        if role is not None:
            rel = context.make("Directorship")
            rel.id = context.make_slug("directorship", company.id, proxy.id)
            rel.add("role", role)
            rel.add("director", proxy)
            rel.add("organization", company)
            rel.add("startDate", member.findtext("funkce/vznikFunkce"))
            rel.add("endDate", member.findtext("funkce/zanikFunkce"))
            context.emit(rel)


def crawl(context: Context):
    data_path = context.fetch_resource("data.tar.gz", context.data_url)
    idx = 0
    with tarfile.open(data_path, "r:gz") as f:
        archive_member = f.next()
        while archive_member is not None:
            idx += 1
            res = f.extractfile(archive_member)
            if res is None:
                context.log.warn("Cannot read: %s" % archive_member.name)
                continue
            parse_xml(context, res)
            archive_member = f.next()
            if idx and idx % 10_000 == 0:
                context.log.info("Parse item %d ..." % idx)
    context.log.info("Parsed %d items." % (idx + 1), fp=data_path.name)
