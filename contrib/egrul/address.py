from collections import defaultdict
from typing import List, Optional, Dict, Any

from lxml.etree import Element

from zavod import helpers as h
from zavod import Context


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

def parse_address(context: Context, el: Element) -> str:
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
    return address
