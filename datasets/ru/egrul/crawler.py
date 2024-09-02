from functools import cache
from urllib.parse import urljoin, urlparse
from typing import Dict, Optional, Set, IO, List, Any, Tuple
from collections import defaultdict
from zipfile import ZipFile
from followthemoney.types import registry
from lxml import etree
from lxml.etree import _Element as Element, tostring
import re
from addressformatting import AddressFormatter

from zavod import Context, Entity
from zavod import helpers as h

MIN_NAME_LENGTH = 40
INN_URL = "https://egrul.itsoft.ru/%s.xml"
# original source: "https://egrul.itsoft.ru/EGRUL_406/01.01.2022_FULL/"
aformatter = AddressFormatter()


TYPES = {
    "АВТОНОМНАЯ НЕКОММЕРЧЕСКАЯ ОРГАНИЗАЦИЯ": "АНО",
    "ГОСУДАРСТВЕННОЕ УНИТАРНОЕ ПРЕДПРИЯТИЕ": "ГУП",
    "ЗАКРЫТОЕ АКЦИОНЕРНОЕ ОБЩЕСТВО": "ЗАО",
    "МЕЖРЕГИОНАЛЬНАЯ ОБЩЕСТВЕННАЯ ОРГАНИЗАЦИЯ": "МОО",
    "ОБЩЕСТВО С ОГРАНИЧЕННОЙ ОТВЕТСТВЕННОСТЬЮ": "ООО",
    "ОТКРЫТОЕ АКЦИОНЕРНОЕ ОБЩЕСТВО": "ОАО",
    "ПУБЛИЧНОЕ АКЦИОНЕРНОЕ ОБЩЕСТВО": "ПАО",
    "ПРИВАТНЕ АКЦІОНЕРНЕ ТОВАРИСТВО": "ПАО",
    "АКЦИОНЕРНОЕ ОБЩЕСТВО": "AO",
}


# Prefixes we believe we can trim without losing meaning to shorten names that are too long.
TRIM_PREFIXES = [
    "ГОСУДАРСТВЕННАЯ ОБЩЕОБРАЗОВАТЕЛЬНАЯ ШКОЛА-ИНТЕРНАТ",
    "ГОСУДАРСТВЕННОЕ БЮДЖЕТНОЕ ОБРАЗОВАТЕЛЬНОЕ УЧРЕЖДЕНИЕ ВЫСШЕГО ПРОФЕССИОНАЛЬНОГО ОБРАЗОВАНИЯ",
    "ГОСУДАРСТВЕННОЕ БЮДЖЕТНОЕ СПЕЦИАЛЬНОЕ (КОРРЕКЦИОННОЕ) ОБРАЗОВАТЕЛЬНОЕ УЧРЕЖДЕНИЕ ДЛЯ ОБУЧАЮЩИХСЯ, ВОСПИТАННИКОВ С ОГРАНИЧЕННЫМИ ВОЗМОЖНОСТЯМИ ЗДОРОВЬЯ СПЕЦИАЛЬНАЯ (КОРРЕКЦИОННАЯ)",
    "ГОСУДАРСТВЕННОЕ ДОШКОЛЬНОЕ ОБРАЗОВАТЕЛЬНОЕ УЧРЕЖДЕНИЕ",
    "ГОСУДАРСТВЕННОЕ КАЗЕННОЕ СПЕЦИАЛЬНОЕ (КОРРЕКЦИОННОЕ) ОБРАЗОВАТЕЛЬНОЕ УЧРЕЖДЕНИЕ",
    "ГОСУДАРСТВЕННОЕ КАЗЁННОЕ УЧРЕЖДЕНИЕ ЯМАЛО-НЕНЕЦКОГО АВТОНОМНОГО ОКРУГА",
    "ГОСУДАРСТВЕННОЕ КАЗЕННОЕ",
    "ГОСУДАРСТВЕННОЕ НАУЧНОЕ УЧРЕЖДЕНИЕ",
    "ГОСУДАРСТВЕННОЕ ОБРАЗОВАТЕЛЬНОЕ УЧРЕЖДЕНИЕ ДЛЯ ДЕТЕЙ ДОШКОЛЬНОГО И МЛАДШЕГО ШКОЛЬНОГО ВОЗРАСТА НАЧАЛЬНАЯ ШКОЛА",
    "ГОСУДАРСТВЕННОЕ ОБРАЗОВАТЕЛЬНОЕ УЧРЕЖДЕНИЕ ДОПОЛНИТЕЛЬНОГО ПРОФЕССИОНАЛЬНОГО ОБРАЗОВАНИЯ (ПОВЫШЕНИЯ КВАЛИФИКАЦИИ) СПЕЦИАЛИСТОВ",
    "ГОСУДАРСТВЕННОЕ ОБРАЗОВАТЕЛЬНОЕ УЧРЕЖДЕНИЕ",
    "ГОСУДАРСТВЕННОЕ ОБЩЕОБРАЗОВАТЕЛЬНОЕ УЧРЕЖДЕНИЕ",
    "ГОСУДАРСТВЕННОЕ СПЕЦИАЛЬНОЕ (КОРРЕКЦИОННОЕ) ОБРАЗОВАТЕЛЬНОЕ УЧРЕЖДЕНИЕ ДЛЯ ОБУЧАЮЩИХСЯ, ВОСПИТАННИКОВ С ОТКЛОНЕНИЯМИ В РАЗВИТИИ",
    "ГОСУДАРСТВЕННОЕ УНИТАРНОЕ СЕЛЬСКОХОЗЯЙСТВЕННОЕ ПРЕДПРИЯТИЕ",
    "ГОСУДАРСТВЕННОЕ УЧРЕЖДЕНИЕ",
    "ГОСУДАРСТВЕННОЕ",  # State
    "ДОЧЕРНЕЕ ГОСУДАРСТВЕННОЕ УНИТАРНОЕ ПРЕДПРИЯТИЕ",
    "КАЗЕННОЕ ОБРАЗОВАТЕЛЬНОЕ УЧРЕЖДЕНИЕ ХАНТЫ-МАНСИЙСКОГО АВТОНОМНОГО ОКРУГА - ЮГРЫ ДЛЯ ДЕТЕЙ-СИРОТ И ДЕТЕЙ, ОСТАВШИХСЯ БЕЗ ПОПЕЧЕНИЯ РОДИТЕЛЕЙ",
    "МЕЖРЕГИОНАЛЬНЫЙ ПРОФСОЮЗ РАБОТНИКОВ ФИЛИАЛА",
    "МЕСТНАЯ ОБЩЕСТВЕННАЯ ОРГАНИЗАЦИЯ - ПЕРВИЧНАЯ ПРОФСОЮЗНАЯ",
    "МЕСТНАЯ ОБЩЕСТВЕННАЯ ОРГАНИЗАЦИЯ -",
    "МЕСТНАЯ ОБЩЕСТВЕННАЯ ОРГАНИЗАЦИЯ ПЕРВИЧНАЯ ПРОФСОЮЗНАЯ ОРГАНИЗАЦИЯ РАБОТНИКОВ",
    "МЕСТНАЯ ОБЩЕСТВЕННАЯ ОРГАНИЗАЦИЯ ПЕРВИЧНАЯ ПРОФСОЮЗНАЯ",
    "МЕСТНАЯ ОБЩЕСТВЕННАЯ ОРГАНИЗАЦИЯ-ПЕРВИЧНАЯ ПРОФСОЮЗНАЯ ОРГАНИЗАЦИЯ ГОСУДАРСТВЕННОГО УНИТАРНОГО ПРЕДПРИЯТИЯ",
    "МЕСТНАЯ ОБЩЕСТВЕННАЯ ОРГАНИЗАЦИЯ-ПЕРВИЧНАЯ ПРОФСОЮЗНАЯ",
    "МЕСТНАЯ РЕЛИГИОЗНАЯ ОРГАНИЗАЦИЯ ПРАВОСЛАВНЫЙ ПРИХОД ХРАМА ВО ИМЯ СВЯТОГО ВЕЛИКОМУЧЕНИКА ГЕОРГИЯ ПОБЕДОНОСЦА С. АФОНЬЕВКА ВОЛОКОНОВСКОГО РАЙОНА БЕЛГОРОДСКОЙ ОБЛАСТИ РЕЛИГИОЗНОЙ ОРГАНИЗАЦИИ",
    "МЕСТНАЯ РЕЛИГИОЗНАЯ ОРГАНИЗАЦИЯ",
    "МУНИЦИПАЛЬНОЕ АВТОНОМНОЕ ДОШКОЛЬНОЕ ОБРАЗОВАТЕЛЬНОЕ УЧРЕЖДЕНИЕ",
    "МУНИЦИПАЛЬНОЕ АВТОНОМНОЕ ОБЩЕОБРАЗОВАТЕЛЬНОЕ УЧРЕЖДЕНИЕ МУНИЦИПАЛЬНОГО ОБРАЗОВАНИЯ ГОРОД КРАСНОДАР",
    "МУНИЦИПАЛЬНОЕ БЮДЖЕТНОЕ ДОШКОЛЬНОЕ ОБРАЗОВАТЕЛЬНОЕ УЧРЕЖДЕНИЕ",
    "МУНИЦИПАЛЬНОЕ БЮДЖЕТНОЕ ОБЩЕОБРАЗОВАТЕЛЬНОЕ УЧРЕЖДЕНИЕ",
    "МУНИЦИПАЛЬНОЕ КАЗЕННОЕ ДОШКОЛЬНОЕ ОБРАЗОВАТЕЛЬНОЕ УЧРЕЖДЕНИЕ",
    "МУНИЦИПАЛЬНОЕ КАЗЕННОЕ УЧРЕЖДЕНИЕ",
    "МУНИЦИПАЛЬНОЕ ОБРАЗОВАТЕЛЬНОЕ УЧРЕЖДЕНИЕ ДЛЯ ДЕТЕЙ ДОШКОЛЬНОГО И МЛАДШЕГО ШКОЛЬНОГО ВОЗРАСТА",
    "МУНИЦИПАЛЬНОЕ ОБЩЕОБРАЗОВАТЕЛЬНОЕ БЮДЖЕТНОЕ УЧРЕЖДЕНИЕ",
    "НЕГОСУДАРСТВЕННОЕ ОБРАЗОВАТЕЛЬНОЕ УЧРЕЖДЕНИЕ ДОПОЛНИТЕЛЬНОГО ПРОФЕССИОНАЛЬНОГО ОБРАЗОВАНИЯ",
    "НЕГОСУДАРСТВЕННОЕ ОБРАЗОВАТЕЛЬНОЕ УЧРЕЖДЕНИЕ НАЧАЛЬНОГО И ДОПОЛНИТЕЛЬНОГО ПРОФЕССИОНАЛЬНОГО ОБРАЗОВАНИЯ",
    "НЕГОСУДАРСТВЕНОЕ ОБРАЗОВАТЕЛЬНОЕ УЧРЕЖДЕНИЕ ДОПОЛНИТЕЛЬНОГО ПРОФЕССИОНАЛЬНОГО ОБРАЗОВАНИЯ (ПОВЫШЕНИЯ КВАЛИФИКАЦИИ) СПЕЦИАЛИСТОВ",
    "ОБЛАСТНОЕ ГОСУДАРСТВЕННОЕ КАЗЁННОЕ ОБРАЗОВАТЕЛЬНОЕ УЧРЕЖДЕНИЕ ДЛЯ ДЕТЕЙ-СИРОТ И ДЕТЕЙ, ОСТАВШИХСЯ БЕЗ ПОПЕЧЕНИЯ РОДИТЕЛЕЙ",
    "ОБЩЕСТВЕННАЯ ОРГАНИЗАЦИЯ ПЕРВИЧНАЯ ПРОФСОЮЗНАЯ",
    "ОБЩЕСТВЕННАЯ ОРГАНИЗАЦИЯ",
    "ОКРУЖНОЕ ГОСУДАРСТВЕННОЕ УЧРЕЖДЕНИЕ",
    "ОРГАНИЗАЦИЯ НАУЧНОГО ОБСЛУЖИВАНИЯ И СОЦИАЛЬНОЙ СФЕРЫ",
    "ПЕРВИЧНАЯ ОРГАНИЗАЦИЯ ПРОФСОЮЗА ОБЛАСТНОГО БЮДЖЕТНОГО УЧРЕЖДЕНИЯ ЗДРАВООХРАНЕНИЯ",
    "ПЕРВИЧНАЯ ОРГАНИЗАЦИЯ ПРОФСОЮЗА СОТРУДНИКОВ ГОСУДАРСТВЕННОГО БЮДЖЕТНОГО ОБРАЗОВАТЕЛЬНОГО УЧРЕЖДЕНИЯ ВЫСШЕГО ПРОФЕССИОНАЛЬНОГО ОБРАЗОВАНИЯ",
    "ПЕРВИЧНАЯ ПРОФСОЮЗНАЯ ОБЩЕСТВЕННАЯ ОРГАНИЗАЦИЯ АКЦИОНЕРНОГО ОБЩЕСТВА",
    "ПЕРВИЧНАЯ ПРОФСОЮЗНАЯ ОБЩЕСТВЕННАЯ ОРГАНИЗАЦИЯ АТНИНСКОГО РАЙОННОГО УЗЛА ЭЛЕКТРИЧЕСКОЙ СВЯЗИ ГОСУДАРСТВЕННОГО УНИТАРНОГО ПРЕДПРИЯТИЯ УПРАВЛЕНИЕ ЭЛЕКТРИЧЕСКОЙ СВЯЗИ",
    "ПЕРВИЧНАЯ ПРОФСОЮЗНАЯ ОРГАНИЗАЦИЯ ГОСУДАРСТВЕННОГО ОБРАЗОВАТЕЛЬНОГО УЧРЕЖДЕНИЯ СРЕДНЕГО ПРОФЕССИОНАЛЬНОГО ОБРАЗОВАНИЯ",
    "ПЕРВИЧНАЯ ПРОФСОЮЗНАЯ ОРГАНИЗАЦИЯ ГОСУДАРСТВЕННОГО УЧРЕЖДЕНИЯ СРЕДНЕГО ПРОФЕССИОНАЛЬНОГО ОБРАЗОВАНИЯ",
    "ПЕРВИЧНАЯ ПРОФСОЮЗНАЯ ОРГАНИЗАЦИЯ КАЛАЧЕЕВСКОГО ЛИНЕЙНОГО ПРОИЗВОДСТВЕННОГО УПРАВЛЕНИЯ МАГИСТРАЛЬНЫХ ГАЗОПРОВОДОВ",
    "ПЕРВИЧНАЯ ПРОФСОЮЗНАЯ ОРГАНИЗАЦИЯ НИЖЕГОРОДСКОЙ ОБЛАСТНОЙ ОРГАНИЗАЦИИ ОБЩЕСТВЕННОЙ ОРГАНИЗАЦИИ",
    "ПЕРВИЧНАЯ ПРОФСОЮЗНАЯ ОРГАНИЗАЦИЯ ОБЩЕСТВА С ОГРАНИЧЕННОЙ ОТВЕТСТВЕННОСТЬЮ",
    "ПЕРВИЧНАЯ ПРОФСОЮЗНАЯ ОРГАНИЗАЦИЯ ОТКРЫТОГО АКЦИОНЕРНОГО ОБЩЕСТВА",
    "ПЕРВИЧНАЯ ПРОФСОЮЗНАЯ ОРГАНИЗАЦИЯ РАБОТНИКОВ ДОЧЕРНЕГО ОБЩЕСТВА С ОГРАНИЧЕНОЙ ОТВЕТСТВЕННОСТЬЮ",
    "ПЕРВИЧНАЯ ПРОФСОЮЗНАЯ ОРГАНИЗАЦИЯ СОТРУДНИКОВ ФЕДЕРАЛЬНОГО ГОСУДАРСТВЕННОГО УНИТАРНОГО ПРЕДПРИЯТИЯ",
    "ПЕРВИЧНАЯ ПРОФСОЮЗНАЯ ОРГАНИЗАЦИЯ ФЕДЕРАЛЬНОГО ГОСУДАРСТВЕННОГО БЮДЖЕТНОГО ОБРАЗОВАТЕЛЬНОГО УЧРЕЖДЕНИЯ ВЫСШЕГО ОБРАЗОВАНИЯ",
    "ПЕРВИЧНАЯ ПРОФСОЮЗНАЯ ОРГАНИЗАЦИЯ ФЕДЕРАЛЬНОГО ГОСУДАРСТВЕННОГО БЮДЖЕТНОГО УЧРЕЖДЕНИЯ",
    "ПЕРВИЧНАЯ ПРОФСОЮЗНАЯ ОРГАНИЗАЦИЯ",  # Primary trade union organization
    "ПЕРВИЧНАЯ ПРОФСОЮЗНАЯ",  # Primary trade union
    "ПРАВОСЛАВНЫЙ ПРИХОД ХРАМА СВЯТИТЕЛЯ ПАВЛА МИТРОПОЛИТА ТОБОЛЬСКОГО",
    "ПРОФЕССИОНАЛЬНАЯ ОБРАЗОВАТЕЛЬНАЯ АВТОНОМНАЯ НЕКОММЕРЧЕСКАЯ ОРГАНИЗАЦИЯ",
    "ПРОФЕССИОНАЛЬНОЕ ОБРАЗОВАТЕЛЬНОЕ УЧРЕЖДЕНИЕ",
    "РЕЛИГИОЗНАЯ ОРГАНИЗАЦИЯ",
    "УПРАВЛЯЮЩИЙ ТОВАРИЩ ИНВЕСТИЦИОННОГО ТОВАРИЩЕСТВА",
    "ФЕДЕРАЛЬНОЕ ГОСУДАРСТВЕННОЕ БЮДЖЕТНОЕ ВОЕННОЕ ОБРАЗОВАТЕЛЬНОЕ УЧРЕЖДЕНИЕ ВЫСШЕГО ОБРАЗОВАНИЯ",
    "ФЕДЕРАЛЬНОЕ ГОСУДАРСТВЕННОЕ БЮДЖЕТНОЕ ДОШКОЛЬНОЕ ОБРАЗОВАТЕЛЬНОЕ УЧРЕЖДЕНИЕ",
    "ФЕДЕРАЛЬНОЕ ГОСУДАРСТВЕННОЕ БЮДЖЕТНОЕ ОБРАЗОВАТЕЛЬНОЕ УЧРЕЖДЕНИЕ ВЫСШЕГО ОБРАЗОВАНИЯ",
    "ФЕДЕРАЛЬНОЕ ГОСУДАРСТВЕННОЕ БЮДЖЕТНОЕ ОБРАЗОВАТЕЛЬНОЕ УЧРЕЖДЕНИЕ ВЫСШЕГО ПРОФЕССИОНАЛЬНОГО ОБРАЗОВАНИЯ",
    "ФЕДЕРАЛЬНОЕ ГОСУДАРСТВЕННОЕ БЮДЖЕТНОЕ ОБРАЗОВАТЕЛЬНОЕ УЧРЕЖДЕНИЕ ДОПОЛНИТЕЛЬНОГО ПРОФЕССИОНАЛЬНОГО ОБРАЗОВАНИЯ",
    "ФЕДЕРАЛЬНОЕ ГОСУДАРСТВЕННОЕ БЮДЖЕТНОЕ УЧРЕЖДЕНИЕ",
    "ФЕДЕРАЛЬНОЕ ГОСУДАРСТВЕННОЕ КАЗЕННОЕ ВОЕННОЕ ОБРАЗОВАТЕЛЬНОЕ УЧРЕЖДЕНИЕ ВЫСШЕГО ОБРАЗОВАНИЯ",
    "ФЕДЕРАЛЬНОЕ ГОСУДАРСТВЕННОЕ КАЗЕННОЕ ОБРАЗОВАТЕЛЬНОЕ УЧРЕЖДЕНИЕ ДОПОЛНИТЕЛЬНОГО ПРОФЕССИОНАЛЬНОГО ОБРАЗОВАНИЯ (ПЕРЕПОДГОТОВКИ И ПОВЫШЕНИЯ КВАЛИФИКАЦИИ)",
    "ФЕДЕРАЛЬНОЕ ГОСУДАРСТВЕННОЕ ОБРАЗОВАТЕЛЬНОЕ УЧРЕЖДЕНИЕ ВЫСШЕГО ПРОФЕССИОНАЛЬНОГО ОБРАЗОВАНИЯ",
    "ФЕДЕРАЛЬНОЕ ГОСУДАРСТВЕННОЕ ОБРАЗОВАТЕЛЬНОЕ УЧРЕЖДЕНИЕ ДОПОЛНИТЕЛЬНОГО ПРОФЕССИОНАЛЬНОГО ОБРАЗОВАНИЯ СПЕЦИАЛИСТОВ",
    "ФЕДЕРАЛЬНОЕ ГОСУДАРСТВЕННОЕ ОБРАЗОВАТЕЛЬНОЕ УЧРЕЖДЕНИЕ ДОПОЛНИТЕЛЬНОГО ПРОФЕССИОНАЛЬНОГО ОБРАЗОВАНИЯ СПЕЦИАЛИСТОВ",
    "ФЕДЕРАЛЬНОЕ ГОСУДАРСТВЕННОЕ УНИТАРНОЕ ПРЕДПРИЯТИЕ",
    "ФЕДЕРАЛЬНОЕ ГОСУДАРСТВЕННОЕ",  # Federal state
    "ФЕДЕРАЛЬНОЕ ДОЧЕРНЕЕ ГОСУДАРСТВЕННОЕ УНИТАРНОЕ ПРЕДПРИЯТИЕ",
    'ФЕДЕРАЛЬНОЕ КАЗЕННОЕ УЧРЕЖДЕНИЕ "ОТДЕЛ ОХРАНЫ ФЕДЕРАЛЬНОГО КАЗЕННОГО УЧРЕЖДЕНИЯ',
    "ФЕДЕРАЛЬНОЕ КАЗЕННОЕ УЧРЕЖДЕНИЕ",
]
REGEX_TRIM_PREFIX = re.compile("^(" + "|".join(TRIM_PREFIXES) + ")")


@cache
def type_sub():
    return {re.combile(rf"\b{k}\b", re.I): v for k, v in TYPES.items()}


def replace_acronyms(text: str) -> str:
    """
    Replace company types with their acronyms.
    """
    for k, v in type_sub().items():
        text = k.sub(v, text)
    return text


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
        context.log.warning(
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
    dput(data, "postcode", el.get("Индекс"))  # Zip code
    dput(
        data, "city", el.findtext("./НаимРегион")
    )  # Наименование субъекта Российской Федерации, (either a region or big city
    # such as Moscow or St. Petersburg), see https://выставить-счет.рф/classifier/regions/

    dput(data, "city", elattr(el.find("./Город"), "ТипГород"))
    dput(data, "city", elattr(el.find("./Город"), "НаимГород"))
    dput(data, "city", elattr(el.find("./Регион"), "НаимРегион"))
    dput(data, "state", elattr(el.find("./Район"), "НаимРайон"))

    dput(
        data, "town", elattr(el.find("./НаселПункт"), "НаимНаселПункт")
    )  # Сведения об адресообразующем элементе населенный пункт
    dput(
        data, "town", elattr(el.find("./НаселенПункт"), "Наим")
    )  # Населенный пункт (город, деревня, село и прочее)

    dput(
        data, "suburb", elattr(el.find("./ГородСелПоселен"), "Наим")
    )  # Городское поселение / сельское поселение / межселенная территория в
    # составе муниципального района / внутригородской район городского округа

    dput(
        data, "municipality", elattr(el.find("./МуниципРайон"), "Наим")
    )  # Municipality

    dput(
        data, "road", elattr(el.find("./ЭлУлДорСети"), "Тип")
    )  # Элемент улично-дорожной сети (street, road, etc.)
    dput(data, "road", elattr(el.find("./ЭлУлДорСети"), "Наим"))

    dput(data, "road", elattr(el.find("./Улица"), "ТипУлица"))  # Street
    dput(data, "road", elattr(el.find("./Улица"), "НаимУлица"))

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

    address = aformatter.one_line(
        {k: " ".join(v) for k, v in data.items()}, country=country
    )
    entity.add("address", address)


def shorten_long_names(original_names: List[str]) -> Tuple[List[str], List[str]]:
    names = []
    descriptions = []
    for name in names:
        if len(name) > registry.name.max_length:
            trimmed = replace_acronyms(name)
            if len(trimmed) <= registry.name.max_length:
                names.append(trimmed)
                descriptions.append(name)
                continue

            trimmed = REGEX_TRIM_PREFIX.sub("", name)
            if trimmed != name and len(trimmed) < registry.name.max_length:
                names.append(trimmed)
                descriptions.append(name)
            else:
                names.append(name)
    return names, descriptions


def categorise_names(
    full_names: List[str], short_names: List[str]
) -> Tuple[List[str], List[str]]:
    """
    If any names are too long, and any names are short enough but not too short
    to be meaningful, use all the short enough names and put the too-long names
    in description.

    Otherwise use all the names and let length validation alert as usual.

    This would let
    ППОО МБУ ДО ДШИ №6 ИМЕНИ Г.В.СВИРИДОВА РРО РПСРК
    PPOO MBU DO DSHI №6 NAMED AFTER G.V.SVIRIDOV RRO RPSRK
    in names and put
    ПЕРВИЧНАЯ ПРОФСОЮЗНАЯ ОБЩЕСТВЕННАЯ ОРГАНИЗАЦИЯ МУНИЦИПАЛЬНОГО БЮДЖЕТНОГО УЧРЕЖДЕНИЯ ДОПОЛНИТЕЛЬНОГО ОБРАЗОВАНИЯ ДЕТСКАЯ ШКОЛА ИСКУССТВ №6 ИМЕНИ Г.В.СВИРИДОВА Г.РОСТОВА-НА-ДОНУ РОСТОВСКОГО РЕГИОНАЛЬНОГО ОТДЕЛЕНИЯ РОССИЙСКОГО ПРОФЕССИОНАЛЬНОГО СОЮЗА РАБОТНИКОВ КУЛЬТУРЫ
    PRIMARY TRADE UNION PUBLIC ORGANIZATION OF THE MUNICIPAL BUDGETARY INSTITUTION OF ADDITIONAL EDUCATION CHILDREN'S ARTS SCHOOL №6 NAMED AFTER G.V.SVIRIDOVA ROSTOV-ON-DON ROSTOV REGIONAL BRANCH OF THE RUSSIAN TRADE UNION OF CULTURAL WORKERS
    in description.
    """
    descriptions = set()
    sufficient_short = set()
    too_short = set()
    too_long = set()

    for name in short_names + full_names:
        if len(name) < registry.name.max_length:
            if len(name) > MIN_NAME_LENGTH:
                sufficient_short.add(name)
            else:
                too_short.add(name)
        else:
            too_long.add(name)
    if too_long and sufficient_short:
        for name in short_names + full_names:
            if len(name) > registry.name.max_length:
                descriptions.add(name)
        ok = sufficient_short.union(too_short)
        assert len(ok.union(descriptions)) == len(full_names + short_names)
        return list(ok), list(descriptions)
    else:
        return full_names + short_names, []


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
    names_full: List[str] = []
    names_short: List[str] = []

    for name_el in el.findall("./СвНаимЮЛ"):
        name_full = name_el.get("НаимЮЛПолн")
        if name_full:
            names_full.append(name_full)
        name_short = name_el.get("НаимЮЛСокр")
        if name_short and name_short != "-":
            names_short.append(name_short)
        for sub_name_el in name_el.findall("./СвНаимЮЛСокр"):
            name_short = sub_name_el.get("НаимСокр")
            if name_short and name_short != "-":
                names_short.append(name_short)

    name = names_full[0] if names_full else names_short[0] if names_short else None
    entity.id = entity_id(context, name=name, inn=inn, ogrn=ogrn)
    entity.add("jurisdiction", "ru")
    names, descriptions = categorise_names(names_full, names_short)
    entity.add("description", descriptions)
    names, descriptions = shorten_long_names(names)
    entity.add("name", names)
    entity.add("description", descriptions)
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

    for successor in el.findall("./СвПреем"):
        succ_name = successor.get("НаимЮЛПолн")
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
            succ.id = context.make_id(entity.id, "successor", successor_id)
            succ.add("successor", successor_id)
            succ.add("predecessor", entity.id)
            succ_entity = context.make("Company")
            succ_entity.id = successor_id
            succ_entity.add("name", succ_name)
            succ_entity.add("innCode", succ_inn)
            succ_entity.add("ogrnCode", succ_ogrn)

            # To @pudo: not sure if I got your idea right
            succ_entity.add("innCode", inn)
            succ_entity.add("ogrnCode", ogrn)

            context.emit(succ_entity)
            context.emit(succ)

    # To @pudo: Also adding this for the predecessor
    for predecessor in el.findall("./СвПредш"):
        pred_name = predecessor.get("НаимЮЛПолн")
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
            pred.id = context.make_id(entity.id, "predecessor", predecessor_id)
            pred.add("predecessor", predecessor_id)
            pred.add("successor", entity.id)
            pred_entity = context.make("Company")
            pred_entity.id = predecessor_id
            pred_entity.add("name", pred_name)
            pred_entity.add("innCode", pred_inn)
            pred_entity.add("ogrnCode", pred_ogrn)

            # To @pudo: not sure if I got your idea right
            pred_entity.add("innCode", inn)
            pred_entity.add("ogrnCode", ogrn)

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
        "7709383684",
        "7704667322",
        "9710075695",
        "7813654884",
        "1122031001454",
        "1025002029580",
        "1131001011283",
        "1088601000047",
    ]:
        path = context.fetch_resource("%s.xml" % inn, INN_URL % inn)
        with open(path, "rb") as fh:
            parse_xml(context, fh)


def crawl_index(context: Context, url: str) -> Set[str]:
    """
    Crawl an index page with ZIP archives and return the URLs.
    Args:
        context: The processing context.
        url: The URL to crawl.
    Returns:
        A set of ZIP archive URLs.
    """
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
    """
    Crawl an archive and parse the XML files inside.
    Args:
        context: The processing context.
        url: The URL to crawl.
    Returns:
        None
    """
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
