from lxml.etree import _Element as Element

from zavod import Context, helpers as h


def extract_data(el: Element) -> dict[str, str]:
    data_dict: dict[str, str] = {}

    # Iterate through each p element
    for p in el:
        text_content = h.element_text(p)

        # Split on the first colon to get the key and value
        if ":" in text_content:
            # Extract key and value based on the first colon
            key, value = text_content.split(":", 1)
            key = key.strip()
            value = value.strip()

            # Store key-value pair in data_dict
            data_dict[key] = value
        else:
            # If no colon, assume continuation of the last added key's value
            if data_dict:
                last_key = list(data_dict)[-1]  # Get the most recent key
                data_dict[last_key] += " " + text_content

    return data_dict


def parse_period(period: str) -> tuple[str, str]:
    period = period.replace("\xa0", " ").replace("From ", "").replace(" To ", " to ")
    start_date_str, end_date_str = period.split(" to ", 1)

    return start_date_str.strip(), end_date_str.strip()


def crawl_item(context: Context, item: dict[str, str]) -> None:
    name = item.pop("Name")
    birth_date = item.pop("Date of Birth")
    address = item.pop("Address (at date of disqualification)")

    person = context.make("Person")
    person.id = context.make_id(name, birth_date, address)
    person.add("name", name)
    person.add("topics", "corp.disqual")
    person.add("country", "im")
    if address != "Not known":
        person.add("address", address)
    h.apply_date(person, "birthDate", birth_date)

    sanction = h.make_sanction(context, person)
    sanction.add("duration", item.pop("Period of Disqualification"))

    if "Dates of Disqualification" in item:
        start_date, end_date = parse_period(item.pop("Dates of Disqualification"))
        h.apply_date(sanction, "startDate", start_date)
        h.apply_date(sanction, "endDate", end_date)

    if "Notes (if any)" in item:
        sanction.add("summary", item.pop("Notes (if any)"))
    elif "Notes" in item:
        sanction.add("summary", item.pop("Notes"))

    if "Particulars of Disqualification Order or Undertaking" in item:
        sanction.add(
            "reason", item.pop("Particulars of Disqualification Order or Undertaking")
        )

    context.emit(person)
    context.emit(sanction)

    context.audit_data(
        item,
        ignore=[
            "Roles in respect of the Disqualification Order or Undertaking",
            "Means of Disqualification",
        ],
    )


def crawl(context: Context) -> None:
    response = context.fetch_html(context.data_url)

    items = h.xpath_elements(response, './/*[@class="accordion-item"]/div')
    # No items means a page change or a WAF block (200 "Request Rejected" page);
    # fail loudly instead of logging a clean run with zero entities.
    if len(items) == 0:
        context.log.error("No disqualified-director entries found")
        return
    for item in items:
        crawl_item(context, extract_data(item))
