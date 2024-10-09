import re
from urllib.parse import urljoin, urlparse
from typing import Dict, Optional, Set, IO, List, Any
from collections import defaultdict
from zipfile import ZipFile

from lxml import etree
from lxml.etree import _Element as Element, tostring

from addressformatting import AddressFormatter

from zavod import Context, Entity
from zavod import helpers as h

INN_URL = "https://egrul.itsoft.ru/%s.xml"
# original source: "https://egrul.itsoft.ru/EGRUL_406/01.01.2022_FULL/"
aformatter = AddressFormatter()


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
    if (
        el.tag
        in [
            "АдресРФ",  # КЛАДР address structure, Сведения об адресе юридического лица (в структуре КЛАДР)
            "СвАдрЮЛФИАС",  # ФИАС address structure, Сведения об адресе юридического лица (в структуре ФИАС)
            "СвРешИзмМН",  # address change, Сведения о принятии юридическим лицом решения об изменении места нахождения
        ]
    ):
        pass
    elif (
        el.tag
        in [
            "СвНедАдресЮЛ",  # Information about address inaccuracy, Сведения о недостоверности адреса
            "СвМНЮЛ",  # this seems to be  a general location (up to town), not an address,
            # Сведения о месте нахождения юридического лица
        ]
    ):
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


def load_abbreviations(context):
    """
    Load abbreviations and compile regex patterns from the context's YAML config.

    Args:
    context: Context object containing the dataset configuration.

    Returns:
    List[Tuple[str, re.Pattern]]: A list of tuples containing canonical abbreviations
    and their compiled regex patterns for substitution.
    """
    yaml = context.dataset.config.get("types")
    abbreviations = []
    for canonical, phrases in yaml.items():
        # Join the phrases into a single regex pattern, escaping only necessary characters
        phrase_pattern = "|".join(escape_special_chars(phrase) for phrase in phrases)
        compiled_pattern = re.compile(phrase_pattern, re.IGNORECASE)
        abbreviations.append((canonical, compiled_pattern))
    return abbreviations


def substitute_abbreviations(name, abbreviations):
    """
    Substitute matched phrases in a company name with their canonical abbreviations.

    Args:
    name (str): The company name to process.
    abbreviations (List[Tuple[str, re.Pattern]]): List containing the abbreviations
    and their associated regex patterns.

    Returns:
    str: The updated company name with substitutions applied.
    """
    for canonical, pattern in abbreviations:
        name = pattern.sub(canonical, name)
    return name


def escape_special_chars(phrase):
    """
    Escape only special regex characters in a given phrase.
    """
    # Define regex meta-characters that need escaping
    special_chars = ".^$*+?{}[]\\|()"
    escaped_phrase = "".join(
        f"\\{char}" if char in special_chars else char for char in phrase
    )
    return escaped_phrase


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

    # Load abbreviations once using the context
    abbreviations = load_abbreviations(context)

    for name_el in el.findall("./СвНаимЮЛ"):
        name_full = name_el.get("НаимЮЛПолн")
        name_short = name_el.get("НаимЮЛСокр")
        # Replace phrases with abbreviations in both full and short names
        if name_full:
            print(name_full)
            name_full = substitute_abbreviations(name_full, abbreviations)
            print(name_full)
        if name_short:
            print(name_short)
            name_short = substitute_abbreviations(name_short, abbreviations)
            print(name_short)

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
    parse_examples(context)
    # for archive_url in sorted(crawl_index(context, context.data_url)):
    #     crawl_archive(context, archive_url)
