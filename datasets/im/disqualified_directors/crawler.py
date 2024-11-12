from typing import Dict, Tuple

from zavod import Context, helpers as h


def extract_data(ele):
    data_dict = {}
    current_key = None

    # Iterate through each p element
    for p in ele:

        strong_tag = p.find("strong")

        # If we find a strong tag, then it's a new data entry
        if strong_tag is not None:
            current_key = strong_tag.text_content().strip().strip(":")
            data_dict[current_key] = (
                p.xpath("string()").replace(strong_tag.text_content(), "").strip()
            )
        # Otherwise, it's just a continuation of the previous one
        else:
            if current_key:
                data_dict[current_key] += " " + p.text_content().strip()
    return data_dict


def parse_period(period: str) -> Tuple[str, str]:
    """Given a string in the format "From DD MMMMM YYYY to DD MMMMM YYYY"
    returns the start and end dates.

    Args:
        period (str): String to be parsed

    Returns:
        start and end dates.
    """

    period = (
        period.replace("\xa0", " ")
        .replace("From ", "")
        .replace(" to ", " ")
        .replace(" To ", " ")
    )

    start_str, end_str = period.split(" ", 3)[0:3], period.split(" ", 3)[3:6]

    # Join the parts back to form date strings
    start_date_str = " ".join(start_str)
    end_date_str = " ".join(end_str)

    return start_date_str, end_date_str


def crawl_item(item: Dict[str, str], context: Context):

    name = item.pop("Name")
    if item["Date of Birth"] != "Not known":
        birth_date = item.pop("Date of Birth")
    else:
        birth_date = None
        item.pop("Date of Birth")
    address = item.pop("Address (at date of disqualification)")

    person = context.make("Person")
    person.id = context.make_id(name, birth_date, address)
    person.add("name", name)
    person.add("address", address)
    person.add("birthDate", birth_date)
    person.add("topics", "corp.disqual")
    person.add("country", "im")

    sanction = h.make_sanction(context, person)
    sanction.add("duration", item.pop("Period of Disqualification"))

    if "Dates of Disqualification" in item:
        start_date, end_date = parse_period(item.pop("Dates of Disqualification"))
        h.apply_date(sanction, "startDate", start_date)
        # sanction.add(
        #     "startDate", h.parse_date(start_date, formats=["%d %B %Y", "%d %b %Y"])
        # )
        h.apply_date(sanction, "endDate", end_date)
        # sanction.add(
        #     "endDate", h.parse_date(end_date, formats=["%d %B %Y", "%d %b %Y"])
        # )

    if "Notes (if any)" in item:
        sanction.add("summary", item.pop("Notes (if any)"))
    elif "Notes" in item:
        sanction.add("summary", item.pop("Notes"))

    if "Particulars of Disqualification Order or Undertaking" in item:
        sanction.add(
            "reason", item.pop("Particulars of Disqualification Order or Undertaking")
        )

    context.emit(person, target=True)
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

    for item in response.xpath('.//*[@class="accordion-item"]/div'):
        crawl_item(extract_data(item), context)
