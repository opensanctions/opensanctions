from functools import lru_cache
from addressformatting import AddressFormatter


@lru_cache(maxsize=None)
def get_formatter():
    return AddressFormatter()


def make_address(
    context,
    full=None,
    remarks=None,
    summary=None,
    po_box=None,
    street=None,
    street2=None,
    city=None,
    postal_code=None,
    region=None,
    country=None,
    key=None,
):
    """Generate an address schema object adjacent to the main entity."""
    address = context.make("Address")
    address.add("full", full)
    address.add("remarks", remarks)
    address.add("summary", summary)
    address.add("postOfficeBox", po_box)
    address.add("street", street)
    address.add("street2", street2)
    address.add("city", city)
    address.add("postalCode", postal_code)
    address.add("region", region)
    address.add("country", country)

    if not address.has("full"):
        data = {
            "attention": summary,
            "house": street or po_box,
            "road": street2,
            "postcode": postal_code,
            "city": city,
            "state": region,
            "country": country,
        }
        cc = address.first("country")
        full = get_formatter().one_line(data, country=cc)
        address.add("full", full)

    if not address.has("full"):
        return

    address.make_id("Address", full, key)
    return address
