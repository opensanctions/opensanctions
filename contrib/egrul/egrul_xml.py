from datetime import date
from typing import Any, Generator, Optional, Dict, IO

from lxml import etree
from lxml.etree import _Element as Element

from address import parse_address
from zavod import Context
from zavod import helpers as h

NULL_NAMES = {"-", "0"}


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
) -> Optional[Dict[str, Any]]:
    """
    Parse a person from the XML element.
    Args:
        context: The processing context.
        el: The XML element.
        local_id: A local ID for the entity.
    Returns:
        A dictionary representing the person entity or None.
    """

    name_el = el.find(".//СвФЛ")
    if name_el is None:
        return None
    last_name = name_el.get("Фамилия")
    first_name = name_el.get("Имя")
    patronymic = name_el.get("Отчество")
    inn_code = name_el.get("ИННФЛ")

    countries = []
    country = el.find("./СвГраждФЛ")
    if country is not None:
        if country.get("КодГражд") == "1":
            countries.append("ru")
        # TODO: Is the else here true?
        else:
            country_name = country.get("НаимСтран")
            if country_name is not None:
                countries.append(country_name)

    name = h.make_name(
        first_name=first_name, patronymic=patronymic, last_name=last_name
    )
    return {
        "id": entity_id(context, name, inn_code, local_id=local_id),
        "schema": "Person",
        "seen_date": context.data_time.date(),
        "name": name if name not in NULL_NAMES else None,
        "first_name": first_name if first_name not in NULL_NAMES else None,
        "last_name": last_name if last_name not in NULL_NAMES else None,
        "father_name": patronymic if patronymic not in NULL_NAMES else None,
        "inn_code": inn_code,
        "countries": countries,
    }


def make_org(
    context: Context, el: Element, local_id: Optional[str]
) -> Optional[Dict[str, Any]]:
    """
    Parse an organization from the XML element.
    Args:
        context: The processing context.
        el: The XML element.
        local_id: A local ID for the entity.
    Returns:
        A dictionary representing the organization entity or None.
    """
    org = {
        "seen_date": context.data_time.date(),
        "schema": "Organization",
    }

    name_el = el.find("./НаимИННЮЛ")
    if name_el is not None:
        name = name_el.get("НаимЮЛПолн")
        inn = name_el.get("ИНН")
        ogrn = name_el.get("ОГРН")
        org["id"] = entity_id(context, name, inn, ogrn, local_id)
        org["name"] = name
        org["inn_code"] = inn
        org["ogrn_code"] = ogrn

    name_latin_el = el.find("./СвНаимЮЛПолнИн")
    if name_latin_el is not None:
        name_latin = name_latin_el.get("НаимПолн")
        org["id"] = entity_id(context, name=name_latin, local_id=local_id)
        org["name_latin"] = name_latin

    foreign_reg_el = el.find("./СвРегИн")
    if foreign_reg_el is not None:
        org["jurisdiction"] = foreign_reg_el.get("На��мСтран")
        org["registration_number"] = foreign_reg_el.get("РегНомер")
        org["publisher"] = foreign_reg_el.get("НаимРегОрг")
        org["addresses"] = [foreign_reg_el.get("АдрСтр")]

    return org


def make_owner(
    context: Context, company: Dict[str, Any], el: Element
) -> Optional[Dict[str, Any]]:
    meta = el.find("./ГРНДатаПерв")
    owner = None
    owner_union: Dict[str, Any] = {
        "person": None,
        "legal_entity": None,
    }

    local_id = company["id"]
    if meta is not None:
        local_id = meta.get("ГРН") or local_id
    link_summary: Optional[str] = None
    link_date: Optional[str] = None
    link_record_id: Optional[str] = None

    if el.tag == "УчрФЛ":  # Individual founder
        owner = make_person(context, el, local_id)
        owner_union["person"] = owner
    elif el.tag == "УчрЮЛИн":  # Foreign company
        owner = make_org(context, el, local_id)
        owner_union["legal_entity"] = owner
    elif el.tag == "УчрЮЛРос":  # Russian legal entity
        owner = make_org(context, el, local_id)
        owner_union["legal_entity"] = owner
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
            owner = {
                "seen_date": context.data_time.date(),
                "schema": "LegalEntity",
            }
            owner_union["legal_entity"] = owner
            owner["id"] = entity_id(context, name, inn, ogrn, local_id)
            owner["name"] = name
            owner["inn_code"] = inn
            owner["ogrn_code"] = ogrn
    elif el.tag == "УчрРФСубМО":  # Russian public body
        pb_name_el = el.find("./ВидНаимУчр")
        if pb_name_el is not None:
            # Name of the owning authority
            pb_name = pb_name_el.get("НаимМО")
            pb_code = pb_name_el.get("КодУчрРФСубМО")
            pb_region = pb_name_el.get("НаимРегион")
            if pb_code == "1":
                pb_region = "Российская Федерация"

            owner = {
                "seen_date": context.data_time.date(),
                "schema": "PublicBody",
            }
            owner_union["legal_entity"] = owner

            if pb_name is not None:
                owner["id"] = entity_id(context, name=pb_name, local_id=local_id)
                owner["name"] = pb_name
            else:
                # to @pudo: I'm using local_id==state here to glue together the regions
                # let me know if you want me to switch it to local_id
                owner["id"] = entity_id(context, name=pb_region, local_id="state")
                owner["name"] = pb_region

        # managing body:
        pb_el = el.find("./СвОргОсущПр")
        if pb_el is not None:
            owner = make_org(context, pb_el, local_id)
            owner_union["legal_entity"] = owner
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
            owner = {
                "seen_date": context.data_time.date(),
                "schema": "LegalEntity",
            }
            owner_union["legal_entity"] = owner
            owner["id"] = entity_id(context, name, inn, ogrn, local_id)
            owner["name"] = name
            owner["inn_code"] = inn
            owner["ogrn_code"] = ogrn
    elif el.tag == "УчрРФСубМО":
        # Skip municipal ownership
        return None
    else:
        context.log.warn("Unknown owner type", tag=el.tag)
        return None

    if owner is None or owner.get("id") is None:
        context.log.warning(
            "No ID for ownership of company %s, skipping Ownership" % company["id"],
            el=el,
            owner=owner,
        )
        return None

    ownership: Dict[str, Any] = {
        "seen_date": context.data_time.date(),
        "date": date.fromisoformat(str(link_date)) if link_date else None,
        "record_id": link_record_id,
        "summary_1": link_summary,
    }

    if company.get("dissolution_date"):
        ownership["end_date"] = company["dissolution_date"]

    meta = el.find("./ГРНДатаПерв")
    if meta is not None:
        start_date = meta.get("ДатаЗаписи")
        ownership["start_date"] = date.fromisoformat(start_date) if start_date else None

    ownership["role"] = el.tag
    ownership["owner"] = owner_union
    ownership["asset_id"] = company["id"]

    share_el = el.find("./ДоляУстКап")
    if share_el is not None:
        ownership["shares_count"] = share_el.get("НоминСтоим")
        percent_el = share_el.find("./РазмерДоли/Процент")
        if percent_el is not None:
            ownership["percentage"] = percent_el.text

    # NOTE(Leon Handreke): Here we re-key (vs. the old crawler) to detect changes in ownership structure
    # The previous key did not contain shares_count and role
    # This ID will also be used to detect changes in ownership when building historic data
    ownership["id"] = context.make_id(
        str(company["id"]),
        str(owner["id"]),
        str(ownership.get("shares_count")),
        str(ownership["role"]),
    )

    reliable_el = el.find("./СвНедДанУчр")
    if reliable_el is not None:
        ownership["summary_2"] = reliable_el.get("ТекстНедДанУчр")

    return ownership


def make_directorship(
    context: Context, company: Dict[str, Any], el: Element
) -> Optional[Dict[str, Any]]:
    """
    Parse a directorship from the XML element.
    Args:
        context: The processing context.
        company: The company entity.
        el: The XML element.
    Returns:
        A dictionary representing the directorship entity or None.
    """
    director = make_person(context, el, company["id"])
    if director is None:
        context.log.warn("Directorship has no person", company=company["id"])
        return None

    role = el.find("./СвДолжн")
    if role is None:
        context.log.warn("Directorship has no role", tag=el)
        return None

    directorship = {
        # This ID will also be used to detect changes in directorship when building historic data
        "id": context.make_id(company["id"], director["id"], role.get("ВидДолжн")),
        "seen_date": context.data_time.date(),
        "role": role.get("НаимДолжн"),
        "summary": role.get("НаимВидДолжн"),
        "director": director,
        "organization_id": company["id"],
    }
    start_date_el = el.find("./ГРНДатаПерв")
    if start_date_el is not None and start_date_el.get("ДатаЗаписи") is not None:
        directorship["start_date"] = date.fromisoformat(
            str(start_date_el.get("ДатаЗаписи"))
        )

    return directorship


def build_successor_predecessor(
    context: Context, other_entity: Dict[str, Any], el: Element
) -> Optional[Dict[str, Any]]:
    name = el.get("НаимЮЛПолн")
    inn = el.get("ИНН")
    ogrn = el.get("ОГРН")
    successor_id = entity_id(
        context,
        name=name,
        inn=inn,
        ogrn=ogrn,
    )
    if successor_id is None:
        return None

    entity = {
        "schema": "Company",
        "seen_date": context.data_time.date(),
        "id": successor_id,
        "name_full": name,
        "inn_code": inn,
        "ogrn_code": ogrn,
    }

    if not entity["ogrn_code"]:
        entity["ogrn_code"] = other_entity.get("ogrn_code")
    if not entity["inn_code"]:
        entity["inn_code"] = other_entity.get("inn_code")
    return entity


def parse_company(context: Context, el: Element) -> Dict[str, Any]:
    company: Dict[str, Any] = {
        "schema": "Company",
        "seen_date": context.data_time.date(),
    }
    inn = el.get("ИНН")
    ogrn = el.get("ОГРН")
    name_full: Optional[str] = None
    name_short: Optional[str] = None

    for name_el in el.findall("./СвНаимЮЛ"):
        name_full = name_el.get("НаимЮЛПолн")
        name_short = name_el.get("НаимЮЛСокр")

    name = name_full or name_short
    company["id"] = entity_id(context, name=name, inn=inn, ogrn=ogrn)
    company["jurisdiction"] = "ru"
    company["name_full"] = name_full
    company["name_short"] = name_short
    company["ogrn_code"] = ogrn
    company["inn_code"] = inn
    company["kpp_code"] = el.get("КПП")
    company["legal_form"] = el.get("ПолнНаимОПФ")
    incorporation_date = el.get("ДатаОГРН")
    company["incorporation_date"] = (
        date.fromisoformat(incorporation_date) if incorporation_date else None
    )

    for term_el in el.findall("./СвПрекрЮЛ"):
        dissolution_date = term_el.get("ДатаПрекрЮЛ")
        company["dissolution_date"] = (
            date.fromisoformat(dissolution_date) if dissolution_date else None
        )

    email_el = el.find("./СвАдрЭлПочты")
    if email_el is not None:
        company["email"] = email_el.get("E-mail")

    citizen_el = el.find("./СвГражд")
    if citizen_el is not None:
        company["country"] = citizen_el.get("НаимСтран")

    # СвОКВЭД = container for OKVED economic-activity codes.
    # СвОКВЭДОсн = primary activity (0..1), СвОКВЭДДоп = additional (0..*).
    # NOTE: EGRUL format 4.08 (effective 2026-02-01, mandatory 2026-08-01)
    # adds a second sibling block СвОКВЭДОтч ("reporting type", derived from
    # tax filings) with СвОКВЭДОтчОсн/СвОКВЭДОтчДоп. We currently ignore it.
    okved_codes: list[str] = []
    okved_root = el.find("./СвОКВЭД")
    if okved_root is not None:
        primary = okved_root.find("./СвОКВЭДОсн")
        if primary is not None:
            code = primary.get("КодОКВЭД")
            if code:
                okved_codes.append(code)
        for extra in okved_root.findall("./СвОКВЭДДоп"):
            code = extra.get("КодОКВЭД")
            if code:
                okved_codes.append(code)
    company["okved_codes"] = ";".join(okved_codes) if okved_codes else None

    for addr_el in el.findall("./СвАдресЮЛ/*"):
        if "addresses" not in company:
            company["addresses"] = []
        addr = parse_address(context, addr_el)
        if addr is not None:
            company["addresses"].append(addr)

    directorships = []
    # prokura or directors etc.
    for director in el.findall("./СведДолжнФЛ"):
        directorship = make_directorship(context, company, director)
        if directorship:
            directorships.append(directorship)

    ownerships = []
    for founder in el.findall("./СвУчредит/*"):
        ownership_result = make_owner(context, company, founder)
        if ownership_result:
            ownerships.append(ownership_result)

    successions = []
    for successor_el in el.findall("./СвПреем"):
        succ_entity = build_successor_predecessor(context, company, successor_el)
        if succ_entity is not None:
            succ = {
                "id": context.make_id(
                    str(company["id"]), "successor", succ_entity["id"]
                ),
                "seen_date": context.data_time.date(),
                "successor": succ_entity,
                "predecessor_id": company["id"],
                "successor_id": succ_entity["id"],
            }
            successions.append(succ)

    for predecessor_el in el.findall("./СвПредш"):
        pred_entity = build_successor_predecessor(context, company, predecessor_el)
        if pred_entity is not None:
            pred = {
                "id": context.make_id(
                    str(company["id"]), "predecessor", pred_entity["id"]
                ),
                "seen_date": context.data_time.date(),
                "predecessor": pred_entity,
                "predecessor_id": pred_entity["id"],
                "successor_id": company["id"],
            }
            successions.append(pred)

    return {
        "id": company["id"],
        "legal_entity": company,
        "directorships": directorships,
        "ownerships": ownerships,
        "successions": successions,
    }


def parse_sole_trader(context: Context, el: Element) -> Optional[Dict[str, Any]]:
    inn = el.get("ИННФЛ")
    ogrn = el.get("ОГРНИП")
    t = {
        "schema": "LegalEntity",
        "id": entity_id(context, inn=inn, ogrn=ogrn),
        "seen_date": context.data_time.date(),
        "country": "ru",
        "ogrn_code": ogrn,
        "inn_code": inn,
        "legal_form": el.get("НаимВидИП"),
    }
    if t["id"] is None:
        context.log.warn("No ID for sole trader")
        return None

    # Same OKVED structure as legal entities; see parse_company for the
    # note on the new СвОКВЭДОтч block in EGRIP 4.07.
    okved_codes: list[str] = []
    okved_root = el.find("./СвОКВЭД")
    if okved_root is not None:
        primary = okved_root.find("./СвОКВЭДОсн")
        if primary is not None:
            code = primary.get("КодОКВЭД")
            if code:
                okved_codes.append(code)
        for extra in okved_root.findall("./СвОКВЭДДоп"):
            code = extra.get("КодОКВЭД")
            if code:
                okved_codes.append(code)
    t["okved_codes"] = ";".join(okved_codes) if okved_codes else None

    return {"id": t["id"], "legal_entity": t}


def parse_xml(
    context: Context, handle: IO[bytes]
) -> Generator[Dict[str, Any], None, None]:
    doc = etree.parse(handle)
    res: Optional[Dict[str, Any]]
    for el in doc.findall(".//СвЮЛ"):
        res = parse_company(context, el)
        if res is not None:
            yield res
    for el in doc.findall(".//СвИП"):
        res = parse_sole_trader(context, el)
        if res is not None:
            yield res
