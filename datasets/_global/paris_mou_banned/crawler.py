from normality import stringify
from typing import Dict, Any, Optional

from zavod import Context, helpers as h

# Full URL for the Paris MoU Banned Ships
# https://portal.emsa.europa.eu/o/portlet-public/rest/ban/getBanShips.json?banSituationIds=VALIDATED&page=1&start=0&limit=200


def clean(value: Optional[str]) -> Optional[str]:
    value_str = stringify(value)
    if value_str and value_str.lower() in ("n/a", "unknown", "any"):
        return None
    return value_str


def crawl_vessel(context: Context, item: Dict[str, Any]) -> None:
    ship_id = item.pop("id")
    vessel = context.make("Vessel")
    vessel.id = context.make_slug("vessel", ship_id)

    vessel.add("name", item.pop("shipName"))
    vessel.add("imoNumber", item.pop("imoNumber"))

    flag = item.pop("flag", {})
    vessel.add("flag", flag.get("code"))
    vessel.add("country", flag.get("description"))

    ism = item.pop("ismCompany", {})
    name = clean(ism.get("name")) if ism else None
    if ism and name:
        company = context.make("Company")
        company.id = context.make_slug("org", ism.get("id"))
        company.add("name", ism.get("name"))
        company.add("imoNumber", ism.get("imoNumber"))
        company.add("country", ism.get("countryCode"))
        address = h.make_address(
            context,
            street=clean(ism.get("address")),
            city=clean(ism.get("city")),
            country=clean(ism.get("countryDescription")),
        )
        h.copy_address(company, address)
        link = context.make("UnknownLink")
        link.id = context.make_id(vessel.id, company.id, "linked")
        link.add("object", vessel.id)
        link.add("subject", company.id)
        link.add("role", "ISM company")
        context.emit(company)
        context.emit(link)

    ban_status = item.get("banOrderStatus", {}).get("active")
    if ban_status:
        vessel.add("topics", "reg.warn")

    sanction = h.make_sanction(context, vessel)
    h.apply_date(sanction, "startDate", item.pop("banDate"))
    sanction.add("reason", item.get("banReason", {}).get("description"))

    authority = item.get("banningAuthority", {})
    sanction.add("authority", authority.get("description"))
    sanction.add("duration", clean(item.get("minimumDuration", {}).get("message")))

    context.emit(vessel)
    context.emit(sanction)

    context.audit_data(
        item,
        [
            "banningAuthority",
            "minimumDuration",
            "banReason",
            "banOccurrence",
            "banOrderStatus",
        ],
    )


def crawl(context: Context) -> None:
    start = 0
    limit = 150
    total = None

    while True:
        url = (
            f"{context.data_url}?banSituationIds=VALIDATED&start={start}&limit={limit}"
        )
        data = context.fetch_json(url, cache_days=1)

        if total is None:
            total = data.get("total")
            if not total:
                context.log.warning("No total count found in response.")
            context.log.info(f"Total banned ships to fetch: {total}")

        results = data.get("results", [])
        if not results:
            break

        for item in results:
            crawl_vessel(context, item)

        start += limit
        if start >= total:
            break
