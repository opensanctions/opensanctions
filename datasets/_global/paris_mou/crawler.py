from typing import Dict, Any

from zavod import Context, helpers as h


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
    if ism:
        company = context.make("Company")
        company.id = context.make_slug("org", ism.get("id"))
        company.add("name", ism.get("name"))
        company.add("imoNumber", ism.get("imoNumber"))
        company.add("country", ism.get("countryCode"))
        context.emit(company)
        vessel.add("owner", company.id)

    # ban_status = item.get("banOrderStatus", {}).get("description")
    # if ban_status == "True":
    #     vessel.add("status", "")
    sanction = h.make_sanction(context, vessel)
    h.apply_date(sanction, "startDate", item.pop("banDate"))
    sanction.add("reason", item.get("banReason", {}).get("description"))

    authority = item.get("banningAuthority", {})
    sanction.add("authority", authority.get("description"))

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
    limit = 100
    page = 1
    total = None

    while True:
        url = f"{context.data_url}?banSituationIds=VALIDATED&page={page}&start={start}&limit={limit}"
        data = context.fetch_json(url)

        if total is None:
            total = data.get("total")
            context.log.info(f"Total banned ships to fetch: {total}")

        results = data.get("results", [])
        if not results:
            break

        for item in results:
            crawl_vessel(context, item)

        start += limit
        if start >= total:
            break
