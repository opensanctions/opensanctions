import logging
import re
from typing import Dict, Optional, Set, IO, List, Any, Tuple
from collections import defaultdict
from zipfile import ZipFile

from lxml import etree
from lxml.etree import _Element as Element, tostring

import zavod.logs
from zavod import Context, Entity
from zavod import helpers as h
from zavod.shed.internal_data import fetch_internal_data, list_internal_data

INN_URL = "https://egrul.itsoft.ru/%s.xml"
# original source: "https://egrul.itsoft.ru/EGRUL_406/01.01.2022_FULL/"

AbbreviationList = List[Tuple[str, re.Pattern, List[str]]]
# global variable to store the compiled abbreviations
abbreviations: Optional[AbbreviationList] = None


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


def parse_founder(context: Context, company: Entity, el: Element) -> None:
    """
    Parse a founder from the XML element and emit entity.
    Args:
        context: The processing context.
        company: The company entity.
        el: The XML element.
    Returns:
        None
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
        return
    else:
        context.log.warn("Unknown owner type", tag=el.tag)
        return

    if owner.id is None:
        context.log.info(
            "No ID for owner: %s" % company.id, el=tag_text(el), owner=owner.to_dict()
        )
        return
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

    context.emit(owner)
    context.emit(ownership)


def parse_directorship(context: Context, company: Entity, el: Element) -> None:
    """
    Parse a directorship from the XML element and emit entity.
    Args:
        context: The processing context.
        company: The company entity.
        el: The XML element.
    Returns:
        None
    """
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

    directorship.add("endDate", company.get("dissolutionDate"))
    context.emit(directorship)


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


def build_successor_predecessor(
    context: Context, other_entity: Entity, el: Element
) -> Optional[Entity]:
    name = el.get("НаимЮЛПолн")
    name_short = substitute_abbreviations(name, abbreviations)
    inn = el.get("ИНН")
    ogrn = el.get("ОГРН")
    successor_id = entity_id(
        context,
        name=name,
        inn=inn,
        ogrn=ogrn,
    )
    if successor_id is not None:
        entity = context.make("Company")
        entity.id = successor_id
        entity.add("name", name_short, original_value=name)
        entity.add("innCode", inn)
        entity.add("ogrnCode", ogrn)

        entity.add("ogrnCode", other_entity.get("ogrnCode"))
        entity.add("innCode", other_entity.get("innCode"))

        return entity


def parse_company(context: Context, el: Element) -> None:
    """
    Parse a company from the XML element and emit entities.
    Args:
        context: The processing context.
        el: The XML element.
    Returns:
        None
    """
    entity = context.make("Company")
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
    entity.id = entity_id(context, name=name, inn=inn, ogrn=ogrn)
    entity.add("jurisdiction", "ru")
    entity.add("name", name_full_short, original_value=name_full)
    entity.add("name", name_short_shortened, original_value=name_short)
    entity.add("ogrnCode", ogrn)
    entity.add("innCode", inn)
    entity.add("kppCode", el.get("КПП"))
    entity.add("legalForm", el.get("ПолнНаимОПФ"))
    entity.add("incorporationDate", el.get("ДатаОГРН"))

    for term_el in el.findall("./СвПрекрЮЛ"):
        entity.add("dissolutionDate", term_el.get("ДатаПрекрЮЛ"))

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

    for successor_el in el.findall("./СвПреем"):
        succ_entity = build_successor_predecessor(context, entity, successor_el)
        if succ_entity is not None:
            succ = context.make("Succession")
            succ.id = context.make_id(entity.id, "successor", succ_entity.id)
            succ.add("successor", succ_entity.id)
            succ.add("predecessor", entity.id)

            context.emit(succ_entity)
            context.emit(succ)

    for predecessor_el in el.findall("./СвПредш"):
        pred_entity = build_successor_predecessor(context, entity, predecessor_el)
        if pred_entity is not None:
            pred = context.make("Succession")
            pred.id = context.make_id(entity.id, "predecessor", pred_entity.id)
            pred.add("predecessor", pred_entity.id)
            pred.add("successor", entity.id)

            context.emit(pred_entity)
            context.emit(pred)

    context.emit(entity)


def parse_sole_trader(context: Context, el: Element) -> None:
    """
    Parse a sole trader from the XML element and emit entities.
    Args:
        context: The processing context.
        el: The XML element.
    Returns:
        None
    """
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


def parse_xml(context: Context, handle: IO[bytes]) -> None:
    """
    Parse an XML file and emit entities from СвЮЛ/СвИп elements found
    Args:
        context: The processing context.
        handle: The XML file handle.
    Returns:
        None
    """
    doc = etree.parse(handle)
    for el in doc.findall(".//СвЮЛ"):
        parse_company(context, el)
    for el in doc.findall(".//СвИП"):
        parse_sole_trader(context, el)


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


def list_prefix_internal(context) -> Set[str]:
    """
    List ZIP archives in a specific folder of the internal bucket and return their paths.

    Args:
        context: The processing context.

    Returns:
        A set of relative paths (blob names) to the ZIP archives in the specified folder.
    """
    # Define the prefix for the directory in the bucket
    prefix = "ru_egrul/egrul.itsoft.ru/EGRUL_406/01.01.2022_FULL/"
    archives: Set[str] = set()

    for blob_name in list_internal_data(prefix):
        if blob_name.endswith(".zip"):
            archives.add(blob_name)

    return archives


def crawl_archive(context: Context, blob_name: str) -> None:
    """
    Fetch and process a ZIP archive, extracting and parsing XML files inside.
    Args:
        context: The processing context.
        blob_name: The name of the ZIP file in the internal bucket.
    Returns:
        None
    """
    local_path = context.get_resource_path(blob_name)
    fetch_internal_data(blob_name, local_path)
    try:
        context.log.info("Parsing: %s" % blob_name)
        with ZipFile(local_path, "r") as zip:
            for name in zip.namelist():
                if not name.lower().endswith(".xml"):
                    continue
                with zip.open(name, "r") as fh:
                    parse_xml(context, fh)

    finally:
        local_path.unlink(missing_ok=True)


def crawl(context: Context) -> None:
    # Shut up warnings being logged, we only care about them once they get pulled
    # into our default dataset by ext_ru_egrul
    zavod.logs.get_logger("zavod.runtime.cleaning").setLevel(logging.ERROR)
    # TODO: thread pool execution
    # Load abbreviations once using the context
    global abbreviations
    abbreviations = compile_abbreviations(context)
    for blob_name in sorted(list_prefix_internal(context)):
        crawl_archive(context, blob_name)
