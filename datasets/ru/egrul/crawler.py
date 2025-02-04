import re
import shutil
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Optional, IO, List, Any, Tuple, Iterable
from zipfile import ZipFile

import orjson
import plyvel
from lxml import etree
from lxml.etree import _Element as Element, tostring

from followthemoney import model
from zavod import Context, Entity
from zavod import helpers as h
from zavod.shed.internal_data import fetch_internal_data, list_internal_data

INN_URL = "https://egrul.itsoft.ru/%s.xml"
# original source: "https://egrul.itsoft.ru/EGRUL_406/01.01.2022_FULL/"

AbbreviationList = List[Tuple[str, re.Pattern, List[str]]]
# global variable to store the compiled abbreviations
abbreviations: Optional[AbbreviationList] = None

INTERNAL_DATA_ARCHIVE_PREFIX = "ru_egrul/egrul.itsoft.ru/EGRUL_406/01.01.2022_FULL/"
INTERNAL_DATA_CACHE_PREFIX = "ru_egrul/cache/"

LOCAL_BUCKET_PATH_FOR_DEBUG = "/home/leon/internal-data/"

# TODO(Leon Handreke): This is really awful, figure out a better way to pass Context to another process!
DATASET_PATH = Path("datasets/ru/egrul/ru_egrul.yml")


def tag_text(el: Element) -> str:
    """
    Convert an XML element to a string, preserving the encoding.
    Args:
        el: The XML element to convert.
    Returns:
        The XML element as a string.
    """
    return tostring(el, encoding="utf-8").decode("utf-8")


def dput(data: Dict[str, List[str | None]], name: str, value: Optional[str]) -> None:
    """
    A setter for the address data dictionary.
    Address data dictionary allows to accumulate values under the same key.

    Args:
        data: The address data dictionary.
        name: The key to set.
        value: The value to add.
    Returns:
        None
    """

    if value is None or not value.strip():
        return
    dd = value.replace("-", "")
    if not dd.strip():
        return
    data[name].append(value)


def parse_name(name: Optional[str]) -> List[str]:
    """
    A simple rule-based parser for names, which can contain aliases in parentheses.
    Args:
        name: The name to parse.
    Returns:
        A list of names.
    """
    if name is None:
        return []
    names: List[str] = []
    if name.endswith(")"):
        parts = name.rsplit("(", 1)
        if len(parts) == 2:
            name = parts[0].strip()
            alias = parts[1].strip(")").strip()
            names.append(alias)
    names.append(name)
    return names


def elattr(el: Optional[Element], attr: str) -> Any:
    """
    Get an attribute from an XML element.
    Args:
        el: The XML element.
        attr: The attribute to get.
    Returns:
        The attribute value or None.
    """

    if el is not None:
        return el.get(attr)


def entity_id(
    context: Context,
    name: Optional[str] = None,
    inn: Optional[str] = None,
    ogrn: Optional[str] = None,
    local_id: Optional[str] = None,
) -> Optional[str]:
    """
    Generate an entity ID from the given parameters.
    The priorities are: INN, OGRN, local_id/name.

    Args:
        context: The processing context.
        name: The name of the entity.
        inn: The INN of the entity.
        ogrn: The OGRN of the entity.
        local_id: A local ID for the entity.
    Returns:
        The entity ID or None.
    """
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
    """
    Parse a person from the XML element.
    Args:
        context: The processing context.
        el: The XML element.
        local_id: A local ID for the entity.
    Returns:
        The person entity or None.
    """
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
    """
    Parse an organization from the XML element.
    Args:
        context: The processing context.
        el: The XML element.
        local_id: A local ID for the entity.
    Returns:
        The organization entity.
    """
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


def make_owner(context: Context, company: Entity, el: Element) -> List[Entity]:
    """
    Parse a founder from the XML element.
    Args:
        context: The processing context.
        company: The company entity.
        el: The XML element.
    Returns:
        A list of entities containing the owner and the Ownership relation.
    """
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
            pb_code = pb_name_el.get("КодУчрРФСубМО")
            pb_region = pb_name_el.get("НаимРегион")
            if pb_code == "1":
                pb_region = "Российская Федерация"

            # That's our default owner in case managing body is not found
            owner = context.make("PublicBody")

            if pb_name is not None:
                owner.id = entity_id(context, name=pb_name, local_id=local_id)
                owner.add("name", pb_name)
            else:
                # to @pudo: I'm using local_id==state here to glue together the regions
                # let me know if you want me to switch it to local_id
                owner.id = entity_id(context, name=pb_region, local_id="state")
                owner.add("name", pb_region)

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
        return []
    else:
        context.log.warn("Unknown owner type", tag=el.tag)
        return []

    if owner.id is None:
        context.log.warning(
            "No ID for owner: %s" % company.id, el=tag_text(el), owner=owner.to_dict()
        )
        return []
    ownership = context.make("Ownership")
    ownership.id = context.make_id(company.id, owner.id)
    ownership.add("summary", link_summary)
    ownership.add("recordId", link_record_id)
    ownership.add("date", link_date)
    ownership.add("endDate", company.get("dissolutionDate"))

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

    return [owner, ownership]


def make_directorship(context: Context, company: Entity, el: Element) -> List[Entity]:
    """
    Parse a directorship from the XML element.
    Args:
        context: The processing context.
        company: The company entity.
        el: The XML element.
    Returns:
        A list of Entity objects, containing the director and the Directorship relation.
    """
    # TODO: can we use the ГРН as a fallback ID?
    director = make_person(context, el, company.id)
    if director is None:
        # context.log.warn("Directorship has no person", company=company.id)
        return []

    role = el.find("./СвДолжн")
    if role is None:
        context.log.warn("Directorship has no role", tag=tag_text(el))
        return []

    directorship = context.make("Directorship")
    directorship.id = context.make_id(company.id, director.id, role.get("ВидДолжн"))
    directorship.add("role", role.get("НаимДолжн"))
    directorship.add("summary", role.get("НаимВидДолжн"))
    directorship.add("director", director)
    directorship.add("organization", company)

    date = el.find("./ГРНДатаПерв")
    if date is not None:
        directorship.add("startDate", date.get("ДатаЗаписи"))

    directorship.add("endDate", company.get("dissolutionDate"))
    return [director, directorship]


def parse_address(context: Context, entity: Entity, el: Element) -> None:
    """
    Parse an address from the XML element and set to entity.
    Args:
        context: The processing context.
        entity: The entity to attach the address to.
        el: The XML element.
    Returns:
        None
    """

    data: Dict[str, Optional[List[str]]] = defaultdict(list)
    country = "ru"
    # ФИАС - Федеральная информационная адресная система (since 2011,
    # https://ru.wikipedia.org/wiki/Федеральная_информационная_адресная_система)
    # КЛАДР - Классификатор адресов Российской Федерации (old one, since 17.11.2005)

    # According to this source: https://www.garant.ru/products/ipo/prime/doc/74812994/
    if el.tag in [
        "АдресРФ",  # КЛАДР address structure, Сведения об адресе юридического лица (в структуре КЛАДР)
        "СвАдрЮЛФИАС",  # ФИАС address structure, Сведения об адресе юридического лица (в структуре ФИАС)
        "СвРешИзмМН",  # address change, Сведения о принятии юридическим лицом решения об изменении места нахождения
    ]:
        pass
    elif el.tag in [
        "СвНедАдресЮЛ",  # Information about address inaccuracy, Сведения о недостоверности адреса
        "СвМНЮЛ",  # this seems to be  a general location (up to town), not an address,
        # Сведения о месте нахождения юридического лица
    ]:
        return None  # ignore this one entirely
    else:
        context.log.warn("Unknown address type", tag=el.tag)
        return

    # Still a mess, but at least I gave it some love and order

    # zip code
    dput(data, "postcode", el.get("Индекс"))

    # Наименование субъекта Российской Федерации
    # name of the subject of the Russian Federation
    # either a region or big city such as Moscow or St. Petersburg
    # see https://выставить-счет.рф/classifier/regions/
    dput(data, "state", el.findtext("./НаимРегион"))
    dput(data, "state", elattr(el.find("./Регион"), "НаимРегион"))

    # City or town
    dput(data, "city", elattr(el.find("./Город"), "ТипГород"))
    dput(data, "city", elattr(el.find("./Город"), "НаимГород"))

    # Наименование населенного пункта (name of the settlement)
    dput(data, "city", elattr(el.find("./НаселПункт"), "НаимНаселПункт"))
    # Населенный пункт (город, деревня, село и прочее) (Settlement (city, town, village, etc.))
    dput(data, "city", elattr(el.find("./НаселенПункт"), "Наим"))
    # Городское / сельское поселение (Urban / rural settlement)
    dput(data, "city", elattr(el.find("./ГородСелПоселен"), "Наим"))

    # Муниципальный район (municipal district within a region)
    dput(data, "municipality", elattr(el.find("./МуниципРайон"), "Наим"))

    # Элемент улично-дорожной сети (street, road, etc.)
    dput(data, "road", elattr(el.find("./ЭлУлДорСети"), "Тип"))
    dput(data, "road", elattr(el.find("./ЭлУлДорСети"), "Наим"))
    # Street
    dput(data, "road", elattr(el.find("./Улица"), "ТипУлица"))
    dput(data, "road", elattr(el.find("./Улица"), "НаимУлица"))

    # # Район (District or area within a city)
    # dput(data, "road", elattr(el.find("./Район"), "НаимРайон"))

    # To be honest I don't understand the difference between these house and house_number fields
    dput(data, "house_number", el.get("Дом"))
    dput(data, "house_number", el.get("Корпус"))

    for bld in el.findall("./Здание"):
        dput(data, "house_number", bld.get("Тип"))
        dput(data, "house_number", bld.get("Номер"))

    # To @pudo: this is an apartment/flat number/floor number/office number which is not
    # directly supported by the addressformatting library. I'm commenting it out for now.
    # dput(data, "house", elattr(el.find("./ПомещЗдания"), "Тип"))
    # dput(data, "house", elattr(el.find("./ПомещЗдания"), "Номер"))
    # dput(data, "neighbourhood", el.get("Кварт")) this is actually a flat number or office number

    address = h.format_address(
        street=" ".join(data.get("road", "")),
        house_number=" ".join(data.get("house_number", "")),
        postal_code=" ".join(data.get("postcode", "")),
        city=" ".join(data.get("city", "")),
        state=" ".join(data.get("state", "")),
        state_district=" ".join(data.get("municipality", "")),
        country_code=country,
    )
    entity.add("address", address)


def compile_abbreviations(context) -> AbbreviationList:
    """
    Load abbreviations and compile regex patterns from the YAML config.
    Returns:
    AbbreviationList: A list of tuples containing canonical abbreviations
    and their compiled regex patterns for substitution.
    """
    types = context.dataset.config.get("organizational_types")
    abbreviations = []
    for canonical, phrases in types.items():
        # we want to match the whole word and allow for ["] or ['] at the beginning
        for phrase in phrases:
            phrase_pattern = rf"^[ \"']?{re.escape(phrase)}"

            compiled_pattern = re.compile(phrase_pattern, re.IGNORECASE)
            # Append the canonical form, compiled regex pattern, and phrase to the list
            abbreviations.append((canonical, compiled_pattern, phrase))
    # Reverse-sort by length so that the most specific phrase would match first.
    abbreviations.sort(key=lambda x: len(x[2]), reverse=True)
    return abbreviations


def substitute_abbreviations(
    name: Optional[str], abbreviations: Optional[AbbreviationList]
) -> Tuple[Optional[str], Optional[str]]:
    """
    Substitute organisation type in the name with its abbreviation
    using the compiled regex patterns.

    :param name: The input name where abbreviations should be substituted.
    :param abbreviations: A list of tuples with canonical abbreviations, regex patterns,
                          and original phrases.
    :return: The name shorted if possible, otherwise the original
    """
    if abbreviations is None:
        raise ValueError("Abbreviations not compiled")
    if name is None:
        return None
    # Iterate over all abbreviation groups
    for canonical, regex, phrases in abbreviations:
        modified_name = regex.sub(canonical, name)
        if modified_name != name:
            return modified_name
    # If no match, return the original name
    return name


def make_company(context: Context, el: Element) -> List[Entity]:
    """
    Parse a company from the XML element.
    Args:
        context: The processing context.
        el: The XML element.
    Returns:
        A list of Entities containing the company, its Directorships, Ownership and Successions
    """
    company = context.make("Company")
    inn = el.get("ИНН")
    ogrn = el.get("ОГРН")
    name_full: Optional[str] = None
    name_short: Optional[str] = None

    for name_el in el.findall("./СвНаимЮЛ"):
        name_full = name_el.get("НаимЮЛПолн")
        name_short = name_el.get("НаимЮЛСокр")
        name_full_short = substitute_abbreviations(name_full, abbreviations)
        name_short_shortened = substitute_abbreviations(name_short, abbreviations)
    name = name_full or name_short_shortened
    company.id = entity_id(context, name=name, inn=inn, ogrn=ogrn)
    company.add("jurisdiction", "ru")
    company.add("name", name_full_short, original_value=name_full)
    company.add("name", name_short_shortened, original_value=name_short)
    company.add("ogrnCode", ogrn)
    company.add("innCode", inn)
    company.add("kppCode", el.get("КПП"))
    company.add("legalForm", el.get("ПолнНаимОПФ"))
    company.add("incorporationDate", el.get("ДатаОГРН"))

    for term_el in el.findall("./СвПрекрЮЛ"):
        company.add("dissolutionDate", term_el.get("ДатаПрекрЮЛ"))

    email_el = el.find("./СвАдрЭлПочты")
    if email_el is not None:
        company.add("email", email_el.get("E-mail"))

    citizen_el = el.find("./СвГражд")
    if citizen_el is not None:
        company.add("country", citizen_el.get("НаимСтран"))

    for addr_el in el.findall("./СвАдресЮЛ/*"):
        parse_address(context, company, addr_el)

    directorships = []
    # prokura or directors etc.
    for director in el.findall("./СведДолжнФЛ"):
        directorships.extend(make_directorship(context, company, director))

    ownerships = []
    for founder in el.findall("./СвУчредит/*"):
        ownerships.extend(make_owner(context, company, founder))

    successions = []
    for successor in el.findall("./СвПреем"):
        succ_name = successor.get("НаимЮЛПолн")
        succ_name_short = substitute_abbreviations(succ_name, abbreviations)
        succ_inn = successor.get("ИНН")
        succ_ogrn = successor.get("ОГРН")
        successor_id = entity_id(
            context,
            name=succ_name,
            inn=succ_inn,
            ogrn=succ_ogrn,
        )
        if successor_id is not None:
            succ = context.make("Succession")
            succ.id = context.make_id(company.id, "successor", successor_id)
            succ.add("successor", successor_id)
            succ.add("predecessor", company.id)
            succ_entity = context.make("Company")
            succ_entity.id = successor_id
            succ_entity.add("name", succ_name_short, original_value=succ_name)
            succ_entity.add("innCode", succ_inn)
            succ_entity.add("ogrnCode", succ_ogrn)
            # To @pudo: not sure if I got your idea right
            succ_entity.add("innCode", inn)
            succ_entity.add("ogrnCode", ogrn)

            successions.extend([succ_entity, succ])

    # To @pudo: Also adding this for the predecessor
    for predecessor in el.findall("./СвПредш"):
        pred_name = predecessor.get("НаимЮЛПолн")
        pred_name_short = substitute_abbreviations(pred_name, abbreviations)
        pred_inn = predecessor.get("ИНН")
        pred_ogrn = predecessor.get("ОГРН")
        predecessor_id = entity_id(
            context,
            name=pred_name,
            inn=pred_inn,
            ogrn=pred_ogrn,
        )
        if predecessor_id is not None:
            pred = context.make("Succession")
            pred.id = context.make_id(company.id, "predecessor", predecessor_id)
            pred.add("predecessor", predecessor_id)
            pred.add("successor", company.id)
            pred_entity = context.make("Company")
            pred_entity.id = predecessor_id
            pred_entity.add("name", pred_name_short, original_value=pred_name)
            pred_entity.add("innCode", pred_inn)
            pred_entity.add("ogrnCode", pred_ogrn)

            # To @pudo: not sure if I got your idea right
            pred_entity.add("innCode", inn)
            pred_entity.add("ogrnCode", ogrn)

            successions.extend([pred_entity, pred])

    # It's important that the company is first here, since later code depends on that. Sure, it would be nicer to
    # make a custom data structure here that contains a Company & its associated Entities, but alas, this is more
    # pragmatic for now.
    return [company] + directorships + ownerships + successions


def parse_sole_trader(context: Context, el: Element) -> List[Entity]:
    """
    Parse a sole trader from the XML element.
    Args:
        context: The processing context.
        el: The XML element.
    Returns:
        A list containing the entity (or empty).
    """
    inn = el.get("ИННФЛ")
    ogrn = el.get("ОГРНИП")
    entity = context.make("LegalEntity")
    entity.id = entity_id(context, inn=inn, ogrn=ogrn)
    if entity.id is None:
        context.log.warn("No ID for sole trader")
        return None
    entity.add("country", "ru")
    entity.add("ogrnCode", ogrn)
    entity.add("innCode", inn)
    entity.add("legalForm", el.get("НаимВидИП"))
    return [entity]


def parse_xml(context: Context, handle: IO[bytes]) -> List[Entity]:
    """
    Parse an XML file and emit entities from СвЮЛ/СвИп elements found to a result queue.

    For every company, a list of Entity.as_dict() objects will be published to the result queue, with the
    Company/LegalEntity being first in the list.

    Args:
        context: The processing context.
        handle: The XML file handle.
    Returns:
        None
    """
    doc = etree.parse(handle)
    res = []
    for el in doc.findall(".//СвЮЛ"):
        res.append(make_company(context, el))
    for el in doc.findall(".//СвИП"):
        res.append(parse_sole_trader(context, el))
    return res


def parse_examples(context: Context):
    """
    Parse some example INN numbers from cached xml files (debug purposes only).
    Args:
        context: The processing context.
    """
    # This subset contains a mix of companies with different address structures
    # and an example of successor/predecessor relationship
    for inn in [
        "7714034350",  # organizational form abbreviations testing
        "7729348110",
        # "7709383684",
        # "7704667322",
        # "9710075695",
        # "7813654884",
        # "1122031001454",
        # "1025002029580",
        # "1131001011283",
        # "1088601000047"
    ]:
        path = context.fetch_resource("%s.xml" % inn, INN_URL % inn)
        with open(path, "rb") as fh:
            parse_xml(context, fh)


def crawl_local_archive(context: Context, zip_path: str) -> List[List[Entity]]:
    """
    Parse the XML files inside a zip archive.
    Args:
        context: The processing context.
        zip_path: The path to the zip file.
        result_queue: The result queue.
    Returns:
        None
    """
    res = []
    # context.log.info("Opening archive: %s" % zip_path)
    with ZipFile(zip_path, "r") as zip:
        for name in zip.namelist():
            if not name.lower().endswith(".xml"):
                continue
            with zip.open(name, "r") as fh:
                # context.log.info(f"Parsing {name} from {zip_path}")
                res.extend(parse_xml(context, fh))
    return res


def crawl_remote_archive(context: Context, blob_name: str) -> List[List[Entity]]:
    local_path = context.get_resource_path(blob_name)
    fetch_internal_data(blob_name, local_path)
    try:
        return crawl_local_archive(context, str(local_path))
    finally:
        local_path.unlink(missing_ok=True)


def dicts_to_entities(context: Context, entity_dicts: Iterable[dict]) -> List[Entity]:
    # TODO(Leon Handreke): This is a bit hacky, we have to hand Entity objects across process boundaries,
    # is there a better way maybe?
    return [
        Entity.from_dict(model, d, cleaned=True, default_dataset=context.dataset)
        for d in entity_dicts
    ]


def entities_to_dicts(entities: Iterable[Entity]) -> List[dict]:
    # TODO(Leon Handreke): This is a bit hacky, we have to hand Entity objects across process boundaries,
    # is there a better way maybe?
    return [e.to_dict() for e in entities]


def day_before(iso_date: str) -> str:
    return (date.fromisoformat(iso_date) - timedelta(days=1)).isoformat()


def add_expired_entities_from_previous(
    context: Context,
    new_company_entities: List[Entity],
    old_company_entities: List[Entity],
) -> List[Entity]:
    """Enriches a company and related entities (Ownership, Directorship) with information from a previous version.

    Parameters:
        new_company_entities: List[Entity]
            A list of new entities, with the Company/LegalEntity being first.
        old_company_entities: List[Entity]
            A list of old entities, with the Company/LegalEntity being first.

    Returns:
        List[Entity]: The new list of entities, including the expired Directorship and Ownership entities. The
        Company/LegalEntity is the first element.
    """
    expired_entities = []

    new_company = new_company_entities[0]
    old_company = old_company_entities[0]
    for prop in ["name", "address", "ogrnCode"]:
        if old_company.get(prop) != new_company.get(prop):
            new_company.add(prop, old_company.get(prop))

    for prop in ["ogrnCode", "kppCode", "innCode"]:
        if old_company.get(prop) != new_company.get(prop):
            pass

    if old_company.to_dict() != new_company.to_dict():
        # TODO(Leon Handreke): Where is the __eq__? Does this actually work? Does it detect changed addresses?
        pass
        # print("DIFF")
        # print(old_company.to_dict())
        # print(new_company.to_dict())
        # print("------------------------------")

    # import pdb; pdb.set_trace()
    old_directorships = set(
        [e for e in old_company_entities if e.schema.name == "Directorship"]
    )
    new_directorships = set(
        [e for e in new_company_entities if e.schema.name == "Directorship"]
    )

    # Directorship __eq__ compares on entity.id, which is a tuple of (company, director, role)
    ended_directorships = old_directorships.difference(new_directorships)
    for ended_directorship in ended_directorships:
        new_directorships_same_role = [
            x
            for x in new_directorships
            if x.get("role") == ended_directorship.get("role")
        ]
        new_directorship_same_role = (
            new_directorships_same_role[0] if new_directorships_same_role else None
        )
        # Set end_date for ended directorship, using the transition date of the new role if available
        if new_directorship_same_role and new_directorship_same_role.get("startDate"):
            ended_directorship.set(
                "endDate", day_before(new_directorship_same_role.get("startDate")[0])
            )

        # If there is no new Directorship with the same role or it doesn't have a start date, use context.data_time
        # as the transition date.
        if not ended_directorship.get("endDate"):
            ended_directorship.set(
                "endDate", day_before(context.data_time.date().isoformat())
            )
    expired_entities.extend(ended_directorships)

    old_ownerships = set(
        [e for e in old_company_entities if e.schema.name == "Ownership"]
    )
    new_ownerships = set(
        [e for e in new_company_entities if e.schema.name == "Ownership"]
    )
    # TODO(Leon Handreke): Ownership.__eq__ compares on entity.id, which does not contain the percentage of ownership.
    # The right thing to do here would be to include that so we can properly model increases in shares, but this would
    # mean we'd have to rekey all existing Ownerships - do we want that?
    ended_ownerships = old_ownerships.difference(new_ownerships)
    for ended_ownership in ended_ownerships:
        # Sometimes, the old ownership will already have an end date. For some reason, companies get de-dissoluted
        # in the database, and in this case the old ownership still carries the old dissolution date.
        # TODO(Leon Handreke): Handle this differently? Use data_date now that the dissultion date is no longer in
        # the database?
        if not ended_ownership.get("endDate"):
            # We don't have any information on where the shares came from, so use the data date
            ended_ownership.set(
                "endDate", day_before(context.data_time.date().isoformat())
            )
    expired_entities.extend(ended_ownerships)

    # Return the new state of the entity, plus the expired Ownership and Directorship entities
    return new_company_entities + expired_entities


def aggregate_archives_by_date(
    archive_paths: Iterable[Path],
) -> Dict[date, Iterable[Path]]:
    archives_by_date = defaultdict(set)
    for archive_path in archive_paths:
        dirname = archive_path.parts[-2]  # [..., "dirname", "archive.zip"]
        dirname = dirname.rstrip("_FULL")
        archive_date = datetime.strptime(dirname, "%d.%m.%Y").date()
        archives_by_date[archive_date].add(archive_path)
    return archives_by_date


def get_entity_cache_db_path(cache_date: date) -> Path:
    if LOCAL_BUCKET_PATH_FOR_DEBUG:
        cache_path = Path(LOCAL_BUCKET_PATH_FOR_DEBUG).joinpath(
            INTERNAL_DATA_CACHE_PREFIX
        )
        cache_path.mkdir(parents=True, exist_ok=True)
        dbname = cache_date.isoformat() + ".leveldb"
        return cache_path.joinpath(dbname)
    # TODO(Leon Handreke): Implement keeping these in Cloud Storage, have a local copy here
    raise NotImplementedError


def crawl_archives_for_date(
    context: Context,
    archive_date: date,
    archives: Iterable[Any],
    previous_cache_db_path: Optional[Path],
) -> Path:
    """
    Crawl the archives for a date, using a previous cache DB as a base.

    Args:
        context: The context
        archive_date: The date of the archives.
        archives: The iterable of archives.
        previous_cache_db_path: A list to the previous cache DB that will be used as a base for the new one.

    Returns:
        The path of the new cache DB.

    """
    context.data_time = archive_date
    context.log.info(
        "Processing %d archives for %s" % (len(archives), archive_date.isoformat())
    )
    cache_db_path = get_entity_cache_db_path(archive_date)
    if cache_db_path.exists():
        # We've already computed that day from a previous run
        # TODO(Leon Handreke): Implement this backed by internal-data Cloud Storage
        return cache_db_path

    # For the first item, we don't have a previous DB to roll over yet
    if previous_cache_db_path is not None:
        shutil.copytree(previous_cache_db_path, cache_db_path)
    # Write to a tempfile first so that if we crash halfway through, it won't think we've cached that date already
    unfinished_cache_db_path = cache_db_path.with_suffix(".unfinished")
    shutil.rmtree(unfinished_cache_db_path, ignore_errors=True)
    cache_db = plyvel.DB(str(unfinished_cache_db_path), create_if_missing=True)

    n = 0
    for archive in sorted(archives):
        if LOCAL_BUCKET_PATH_FOR_DEBUG:
            res = crawl_local_archive(context, str(archive))
        else:
            res = crawl_remote_archive(context, str(archive))

        with cache_db.write_batch() as wb:
            for new_entities in res:
                # The first element in the list is always the Company/LegalEntity entity
                company_key = bytes(new_entities[0].id, "utf-8")
                # Get the existing company record from the db, if it exists, patch it with the new data.
                previous_value = cache_db.get(company_key)

                if previous_value:
                    previous_entities = dicts_to_entities(
                        context, orjson.loads(previous_value)
                    )
                    new_entities = add_expired_entities_from_previous(
                        context, new_entities, previous_entities
                    )

                wb.put((company_key, orjson.dumps(entities_to_dicts(new_entities))))

                n += 1
                if n % 10000 == 0 and n != 0:
                    context.log.info(
                        "Processed %d companies for date %s"
                        % (n, archive_date.isoformat())
                    )

    cache_db.close()
    unfinished_cache_db_path.rename(cache_db_path)

    return cache_db_path


def crawl(context: Context) -> None:
    # Load abbreviations once using the context
    global abbreviations
    abbreviations = compile_abbreviations(context)

    # Left in there for debugging, if you have a local instance of the data
    if LOCAL_BUCKET_PATH_FOR_DEBUG:
        archives = [
            name
            for name in Path(LOCAL_BUCKET_PATH_FOR_DEBUG)
            .joinpath(INTERNAL_DATA_ARCHIVE_PREFIX)
            .glob("*.zip")
        ]
    else:
        archives = [
            name
            for name in list_internal_data(INTERNAL_DATA_ARCHIVE_PREFIX)
            if name.endswith(".zip")
        ]

    archives_by_date = sorted(aggregate_archives_by_date(archives).items())

    previous_cache_db_path = None
    # Go through list of (date, [archives]) tuples that are sorted by date
    # For each of the dates, we apply the data in the archives for that day to the cache database
    # After we've rolled the database forward to the latest archive, we emit the entities
    for archive_date, archives in archives_by_date:
        previous_cache_db_path = crawl_archives_for_date(
            context, archive_date, archives, previous_cache_db_path
        )

    last_date = archives_by_date[-1][0]
    last_cache_db = plyvel.DB(
        str(get_entity_cache_db_path(last_date)), create_if_missing=False
    )
    # We've rolled the cache database forward to the latest available archive, emit the entities
    for company_key, value in last_cache_db:
        entities = dicts_to_entities(context, orjson.loads(value))
        for e in entities:
            context.emit(e)
