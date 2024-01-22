from lxml import etree
from zipfile import ZipFile
from urllib.parse import urljoin, urlparse
from typing import Dict, Optional, Set, IO, List
from lxml.etree import _Element as Element, tostring
from addressformatting import AddressFormatter

from zavod import Context, Entity
from zavod import helpers as h

INN_URL = "https://egrul.itsoft.ru/%s.xml"
# original source: "https://egrul.itsoft.ru/EGRUL_406/01.01.2022_FULL/"
aformatter = AddressFormatter()


def tag_text(el: Element) -> str:
    return tostring(el, encoding="utf-8").decode("utf-8")


def dput(data: Dict[str, Optional[str]], name: str, value: Optional[str]):
    if value is None or not len(value.strip()):
        return
    dd = value.replace("-", "")
    if not len(dd.strip()):
        return
    data[name] = value


def parse_name(name: Optional[str]) -> List[str]:
    if name is None:
        return []
    names: List[str] = []
    if name.endswith(')'):
        parts = name.rsplit('(', 1)
        if len(parts) == 2:
            name = parts[0].strip()
            alias = parts[1].strip(')').strip()
            names.append(alias)
    names.append(name)
    return names
    

def elattr(el: Optional[Element], attr: str):
    if el is not None:
        return el.get(attr)


def entity_id(
    context: Context,
    name: Optional[str] = None,
    inn: Optional[str] = None,
    ogrn: Optional[str] = None,
    local_id: Optional[str] = None,
) -> Optional[str]:
    if inn is not None:
        return context.make_slug("inn", inn)
    if ogrn is not None:
        return context.make_slug("ogrn", ogrn)
    if name is not None:
        return context.make_id(local_id, name)
    return None


def make_person(
    context: Context, el: Element, local_id: Optional[str]
) -> Optional[Entity]:
    name_el = el.find(".//СвФЛ")
    if name_el is None:
        return None
    last_name = name_el.get("Фамилия")
    first_name = name_el.get("Имя")
    patronymic = name_el.get("Отчество")
    inn_code = name_el.get("ИННФЛ")
    name = h.make_name(
        first_name=first_name, patronymic=patronymic, last_name=last_name
    )
    entity = context.make("Person")
    entity.id = entity_id(context, name, inn_code, local_id=local_id)
    entity.add("name", name)
    entity.add("firstName", first_name)
    entity.add("fatherName", patronymic)
    entity.add("lastName", last_name)
    entity.add("innCode", inn_code)

    country = el.find("./СвГраждФЛ")
    if country is not None:
        if country.get("КодГражд") == "1":
            entity.add("country", "ru")
        entity.add("country", country.get("НаимСтран"))

    return entity


def make_org(context: Context, el: Element, local_id: Optional[str]) -> Entity:
    entity = context.make("Organization")
    name_el = el.find("./НаимИННЮЛ")
    if name_el is not None:
        name = name_el.get("НаимЮЛПолн")
        inn = name_el.get("ИНН")
        ogrn = name_el.get("ОГРН")
        entity.id = entity_id(context, name, inn, ogrn, local_id)
        entity.add("name", parse_name(name))
        entity.add("innCode", inn)
        entity.add("ogrnCode", ogrn)

    name_latin_el = el.find("./СвНаимЮЛПолнИн")
    if name_latin_el is not None:
        name_latin = name_latin_el.get("НаимПолн")
        entity.id = entity_id(context, name=name_latin, local_id=local_id)
        entity.add("name", name_latin)

    foreign_reg_el = el.find("./СвРегИн")
    if foreign_reg_el is not None:
        entity.add("jurisdiction", foreign_reg_el.get("НаимСтран"))
        entity.add("registrationNumber", foreign_reg_el.get("РегНомер"))
        entity.add("publisher", foreign_reg_el.get("НаимРегОрг"))
        entity.add("address", foreign_reg_el.get("АдрСтр"))
    return entity


def parse_founder(context: Context, company: Entity, el: Element):
    meta = el.find("./ГРНДатаПерв")
    owner = context.make("LegalEntity")
    local_id = company.id
    if meta is not None:
        local_id = meta.get("ГРН") or local_id
    link_summary: Optional[str] = None
    link_date: Optional[str] = None
    link_record_id: Optional[str] = None

    if el.tag == "УчрФЛ":  # Individual founder
        owner_proxy = make_person(context, el, local_id)
        if owner_proxy is not None:
            owner = owner_proxy
    elif el.tag == "УчрЮЛИн":  # Foreign company
        owner = make_org(context, el, local_id)
    elif el.tag == "УчрЮЛРос":  # Russian legal entity
        # print(tag_text(el))
        owner = make_org(context, el, local_id)
    elif el.tag == "УчрПИФ":  # Mutual investment fund
        # TODO: nested ownership structure, make Security
        # owner = context.make("Security")
        # FIXME: Security cannot own.
        fund_name_el = el.find("./СвНаимПИФ")
        if fund_name_el is not None:
            # owner.add("name", fund_name_el.get("НаимПИФ"))
            link_summary = fund_name_el.get("НаимПИФ")

        manager_el = el.find("./СвУпрКомпПИФ/УпрКомпПиф")
        if manager_el is not None:
            name = manager_el.get("НаимЮЛПолн")
            inn = manager_el.get("ИНН")
            ogrn = manager_el.get("ОГРН")
            owner.id = entity_id(context, name, inn, ogrn, local_id)
            owner.add("name", parse_name(name))
            owner.add("innCode", inn)
            owner.add("ogrnCode", ogrn)
    elif el.tag == "УчрРФСубМО":  # Russian public body
        pb_name_el = el.find("./ВидНаимУчр")
        if pb_name_el is not None:
            # Name of the owning authority
            pb_name = pb_name_el.get("НаимМО")
            if pb_name is not None:
                owner = context.make("Organization")
                owner.id = entity_id(context, name=pb_name, local_id=local_id)
                owner.add("name", pb_name)

        # managing body:
        pb_el = el.find("./СвОргОсущПр")
        if pb_el is not None:
            owner = make_org(context, pb_el, local_id)
    elif el.tag == "УчрДогИнвТов":  # investment partnership agreement.
        # FIXME: should the partnership be its own entity?
        terms_el = el.find("./ИнПрДогИнвТов")
        if terms_el is not None:
            link_summary = terms_el.get("НаимДог")
            link_record_id = terms_el.get("НомерДог")
            link_date = terms_el.get("Дата")

        # managing vehicle
        manager_el = el.find("./СвУпТовЮЛ")
        if manager_el is not None:
            name = manager_el.get("НаимЮЛПолн")
            inn = manager_el.get("ИНН")
            ogrn = manager_el.get("ОГРН")
            owner.id = entity_id(context, name, inn, ogrn, local_id)
            owner.add("name", parse_name(name))
            owner.add("innCode", inn)
            owner.add("ogrnCode", ogrn)
    elif el.tag == "УчрРФСубМО":
        # Skip municipal ownership
        return
    else:
        context.log.warn("Unknown owner type", tag=el.tag)
        return

    if owner.id is None:
        context.log.warning(
            "No ID for owner: %s" % company.id, el=tag_text(el), owner=owner.to_dict()
        )
        return
    ownership = context.make("Ownership")
    ownership.id = context.make_id(company.id, owner.id)
    ownership.add("summary", link_summary)
    ownership.add("recordId", link_record_id)
    ownership.add("date", link_date)

    meta = el.find("./ГРНДатаПерв")
    if meta is not None:
        ownership.add("startDate", meta.get("ДатаЗаписи"))

    ownership.add("role", el.tag)
    ownership.add("owner", owner)
    ownership.add("asset", company)

    share_el = el.find("./ДоляУстКап")
    if share_el is not None:
        ownership.add("sharesCount", share_el.get("НоминСтоим"))
        percent_el = share_el.find("./РазмерДоли/Процент")
        if percent_el is not None:
            ownership.add("percentage", percent_el.text)

    reliable_el = el.find("./СвНедДанУчр")
    if reliable_el is not None:
        ownership.add("summary", reliable_el.get("ТекстНедДанУчр"))

    # pprint(owner.to_dict())
    context.emit(owner)

    # pprint(ownership.to_dict())
    context.emit(ownership)


def parse_directorship(context: Context, company: Entity, el: Element):
    # TODO: can we use the ГРН as a fallback ID?
    director = make_person(context, el, company.id)
    if director is None:
        # context.log.warn("Directorship has no person", company=company.id)
        return

    context.emit(director)

    role = el.find("./СвДолжн")
    if role is None:
        context.log.warn("Directorship has no role", tag=tag_text(el))
        return

    directorship = context.make("Directorship")
    directorship.id = context.make_id(company.id, director.id, role.get("ВидДолжн"))
    directorship.add("role", role.get("НаимДолжн"))
    directorship.add("summary", role.get("НаимВидДолжн"))
    directorship.add("director", director)
    directorship.add("organization", company)

    date = el.find("./ГРНДатаПерв")
    if date is not None:
        directorship.add("startDate", date.get("ДатаЗаписи"))

    context.emit(directorship)


def parse_address(context: Context, entity: Entity, el: Element):
    data: Dict[str, Optional[str]] = {}
    country = "ru"
    if el.tag == "АдресРФ":  # normal address
        # print(tag_text(el))
        pass
    elif el.tag == "СвМНЮЛ":  # location of legal entity
        # print(tag_text(el))
        pass
    elif el.tag == "СвАдрЮЛФИАС":  # special structure?
        # print(tag_text(el))
        pass
    elif el.tag == "СвНедАдресЮЛ":  # missing address
        # print(el.get("ТекстНедАдресЮЛ"))
        return None  # ignore this one entirely
    elif el.tag == "СвРешИзмМН":  # address change
        # print(tag_text(el))
        # print(el.get("ТекстРешИзмМН"))
        pass
    else:
        context.log.warn("Unknown address type", tag=el.tag)
        return

    # FIXME: this is a complete mess
    dput(data, "postcode", el.get("Индекс"))
    dput(data, "postcode", el.get("ИдНом"))
    dput(data, "house", el.get("Дом"))
    dput(data, "house_number", el.get("Корпус"))
    dput(data, "neighbourhood", el.get("Кварт"))
    dput(data, "neighbourhood", el.get("Кварт"))
    dput(data, "city", el.findtext("./НаимРегион"))
    dput(data, "city", elattr(el.find("./Регион"), "НаимРегион"))
    dput(data, "state", elattr(el.find("./Район"), "НаимРайон"))
    dput(data, "town", elattr(el.find("./НаселПункт"), "НаимНаселПункт"))
    dput(data, "municipality", elattr(el.find("./МуниципРайон"), "Наим"))
    dput(data, "suburb", elattr(el.find("./НаселенПункт"), "Наим"))
    dput(data, "road", elattr(el.find("./ЭлУлДорСети"), "Наим"))
    dput(data, "road", elattr(el.find("./Улица"), "НаимУлица"))
    dput(data, "house", elattr(el.find("./ПомещЗдания"), "Номер"))
    address = aformatter.one_line(data, country=country)
    entity.add("address", address)


def parse_company(context: Context, el: Element):
    entity = context.make("Company")
    inn = el.get("ИНН")
    ogrn = el.get("ОГРН")
    name_full: Optional[str] = None
    name_short: Optional[str] = None

    for name_el in el.findall("./СвНаимЮЛ"):
        name_full = name_el.get("НаимЮЛПолн")
        name_short = name_el.get("НаимЮЛСокр")

    name = name_full or name_short
    entity.id = entity_id(context, name=name, inn=inn, ogrn=ogrn)
    entity.add("jurisdiction", "ru")
    entity.add("name", name_full)
    entity.add("name", name_short)
    entity.add("ogrnCode", ogrn)
    entity.add("innCode", inn)
    entity.add("kppCode", el.get("КПП"))
    entity.add("legalForm", el.get("ПолнНаимОПФ"))
    entity.add("incorporationDate", el.get("ДатаОГРН"))

    email_el = el.find("./СвАдрЭлПочты")
    if email_el is not None:
        entity.add("email", email_el.get("E-mail"))

    citizen_el = el.find("./СвГражд")
    if citizen_el is not None:
        entity.add("country", citizen_el.get("НаимСтран"))

    for addr_el in el.findall("./СвАдресЮЛ/*"):
        parse_address(context, entity, addr_el)

    # prokura or directors etc.
    for director in el.findall("./СведДолжнФЛ"):
        parse_directorship(context, entity, director)

    for founder in el.findall("./СвУчредит/*"):
        parse_founder(context, entity, founder)

    # pprint(entity.to_dict())
    context.emit(entity)


def parse_sole_trader(context: Context, el: Element):
    inn = el.get("ИННФЛ")
    ogrn = el.get("ОГРНИП")
    entity = context.make("LegalEntity")
    entity.id = entity_id(context, inn=inn, ogrn=ogrn)
    if entity.id is None:
        context.log.warn("No ID for sole trader")
        return
    entity.add("country", "ru")
    entity.add("ogrnCode", ogrn)
    entity.add("innCode", inn)
    entity.add("legalForm", el.get("НаимВидИП"))
    context.emit(entity)


def parse_xml(context: Context, handle: IO[bytes]):
    doc = etree.parse(handle)
    for el in doc.findall(".//СвЮЛ"):
        parse_company(context, el)
    for el in doc.findall(".//СвИП"):
        parse_sole_trader(context, el)


def parse_examples(context: Context):
    for inn in ["7709383684", "7704667322", "9710075695"]:
        path = context.fetch_resource("%s.xml" % inn, INN_URL % inn)
        with open(path, "rb") as fh:
            parse_xml(context, fh)


def crawl_index(context: Context, url: str) -> Set[str]:
    archives: Set[str] = set()
    doc = context.fetch_html(url)
    for a in doc.findall(".//a"):
        link_url = urljoin(url, a.get("href"))
        if not link_url.startswith(url):
            continue
        if link_url.endswith(".zip"):
            archives.add(link_url)
            continue
        archives.update(crawl_index(context, link_url))
    return archives


def crawl_archive(context: Context, url: str) -> None:
    url_path = urlparse(url).path.lstrip("/")
    path = context.fetch_resource(url_path, url)
    try:
        context.log.info("Parsing: %s" % url_path)
        with ZipFile(path, "r") as zip:
            for name in zip.namelist():
                if not name.lower().endswith(".xml"):
                    continue
                with zip.open(name, "r") as fh:
                    parse_xml(context, fh)

    finally:
        path.unlink(missing_ok=True)


def crawl(context: Context) -> None:
    # TODO: thread pool execution
    for archive_url in sorted(crawl_index(context, context.data_url)):
        crawl_archive(context, archive_url)
