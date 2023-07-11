from functools import cache, lru_cache
from typing import Optional

from addressformatting import AddressFormatter
from followthemoney.types import registry
from followthemoney.util import join_text, make_entity_id
from nomenklatura.entity import CE
from normality import slugify
from zavod.context import GenericZavod
from zavod.dataset import ZD


@cache
def get_formatter() -> AddressFormatter:
    return AddressFormatter()


@lru_cache(maxsize=5000)
def format_address(
    summary: Optional[str] = None,
    po_box: Optional[str] = None,
    street: Optional[str] = None,
    house: Optional[str] = None,
    house_number: Optional[str] = None,
    postal_code: Optional[str] = None,
    city: Optional[str] = None,
    county: Optional[str] = None,
    state: Optional[str] = None,
    country_code: Optional[str] = None,
) -> str:
    data = {
        "attention": summary,
        "house": po_box,
        "road": street,
        "house": house,
        "house_number": house_number,
        "postcode": postal_code,
        "city": city,
        "county": county,
        "state_district": state,
    }
    return get_formatter().one_line(data, country=country_code)


def make_address(
    context: GenericZavod[CE, ZD],
    full: Optional[str] = None,
    remarks: Optional[str] = None,
    summary: Optional[str] = None,
    po_box: Optional[str] = None,
    street: Optional[str] = None,
    street2: Optional[str] = None,
    street3: Optional[str] = None,
    city: Optional[str] = None,
    place: Optional[str] = None,
    postal_code: Optional[str] = None,
    state: Optional[str] = None,
    region: Optional[str] = None,
    country: Optional[str] = None,
    country_code: Optional[str] = None,
    key: Optional[str] = None,
    lang: Optional[str] = None,
) -> CE:
    """Generate an address schema object adjacent to the main entity."""

    city = join_text(place, city, sep=", ")
    street = join_text(street, street2, street3, sep=", ")

    address = context.make("Address")
    address.add("full", full, lang=lang)
    address.add("remarks", remarks, lang=lang)
    address.add("summary", summary, lang=lang)
    address.add("postOfficeBox", po_box, lang=lang)
    address.add("street", street, lang=lang)
    address.add("city", city, lang=lang)
    address.add("postalCode", postal_code, lang=lang)
    address.add("region", region, lang=lang)
    address.add("state", state, quiet=True, lang=lang)
    address.add("country", country, lang=lang)
    address.add("country", country_code)

    country_code = address.first("country")
    if not full:
        full = format_address(
            summary=summary,
            po_box=po_box,
            street=street,
            postal_code=postal_code,
            city=city,
            state=join_text(region, state, sep=", "),
            country_code=country_code,
        )

    full_country = registry.country.clean(full)
    if full_country is not None:
        address.add("country", full_country, lang=lang)
        # full = None

    # full = clean_address(full)
    address.add("full", full, lang=lang)

    if full:
        norm_full = slugify(full)
        hash_id = make_entity_id(country_code, norm_full, key)
        if hash_id is not None:
            address.id = f"addr-{hash_id}"
    return address
