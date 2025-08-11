from urllib.parse import urljoin
from rigour.mime.types import PDF
from rigour.names import remove_person_prefixes

from zavod import Context
from zavod import helpers as h
from zavod.shed.gpt import run_image_prompt

prompt = """
Extract structured data from the following page of a PDF document. Return
a JSON list (`holders`) in which each object represents an office-holder
(e.g. head of government, head of state, minister of foreign affairs).
Each object should have the following fields: `country`, `full_title`,
`honorary_prefix`, `person_name`, `date_of_appointment`.

When multiple people are listed in one section, return the raw date string 
exactly as it appears in the document for the `date_of_appointment` field - 
do not attempt to parse or split dates. Return each person as a separate object. 
Return an empty string for unset fields.
"""


def crawl_pdf_url(context: Context) -> str:
    html = context.fetch_html(context.data_url)
    for a in html.findall('.//div[@class="content"]//a'):
        if "list.pdf" in a.get("href", ""):
            return urljoin(context.data_url, a.get("href"))
    raise ValueError("No PDF found")


def process_holder_date(
    context, start_dates, last_concatenated_date=None, person_count_for_date=0
):
    if not start_dates:
        return "", last_concatenated_date, person_count_for_date
    if len(start_dates) < 18:
        # Single date - reset state and return as-is
        return start_dates, None, 0
    # Handle concatenated dates (length >= 18)
    if start_dates != last_concatenated_date:
        # New concatenated date - first person gets first 9 chars
        return start_dates[:9], start_dates, 1  # "09-Sep-22"
    # Same concatenated date as previous
    person_count_for_date += 1
    if person_count_for_date == 2:
        # Second person gets last 9 chars
        return start_dates[-9:], start_dates, person_count_for_date  # "07-May-13"
    # More than two people with same concatenated date - log warning and return empty
    context.log.warn(
        f"Found {person_count_for_date} people with same concatenated date: {start_dates}. Cannot determine date assignment."
    )
    return "", start_dates, person_count_for_date


def crawl(context: Context):
    pdf_url = crawl_pdf_url(context)
    path = context.fetch_resource("source.pdf", pdf_url)
    context.export_resource(path, PDF, title=context.SOURCE_TITLE)
    last_concatenated_date = None
    person_count_for_date = 0
    for page_path in h.make_pdf_page_images(path):
        data = run_image_prompt(context, prompt, page_path)
        assert "holders" in data, data
        for holder in data.get("holders", []):
            person_name = holder.get("person_name")
            person_name = remove_person_prefixes(holder.get("person_name"))
            person_name = context.lookup_value(
                "normalize_name", person_name, person_name
            )
            if h.is_empty(person_name):
                continue
            full_title = holder.get("full_title")
            country = holder.get("country")
            norm_name = context.lookup_value("names", person_name, person_name)
            if norm_name is None or len(norm_name.strip()) == 0:
                if full_title is not None and len(full_title.strip()):
                    context.log.info(
                        "No person name found",
                        title=full_title,
                        country=country,
                    )
                continue
            country = holder.get("country")
            entity = context.make("Person")
            entity.id = context.make_id(country, holder.get("person_name"))
            entity.add("topics", "role.pep")
            if norm_name != holder.get("person_name"):
                context.log.debug(
                    "Modified name: %r" % holder.get("person_name"),
                    clean=norm_name,
                )
            entity.add("name", norm_name)
            entity.add("title", holder.get("honorary_prefix"))
            entity.add("country", country)

            position = h.make_position(
                context,
                name=full_title,
                country=country,
                topics=["gov.national"],
            )

            start_date, last_concatenated_date, person_count_for_date = (
                process_holder_date(
                    context,
                    holder.get("date_of_appointment"),
                    last_concatenated_date,
                    person_count_for_date,
                )
            )
            occupancy = h.make_occupancy(
                context, entity, position, start_date=start_date
            )
            if occupancy is not None:
                context.emit(occupancy)

            context.emit(entity)
            context.emit(position)
            if occupancy is not None:
                context.emit(occupancy)
