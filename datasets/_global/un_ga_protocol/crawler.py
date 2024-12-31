from rigour.mime.types import PDF
from urllib.parse import urljoin

from zavod import Context
from zavod import helpers as h
from zavod.shed.gpt import run_image_prompt

prompt = """
Extract structured data from the following page of a PDF document. Return 
a JSON list (`holders`) in which each object represents an office-holder
(e.g. head of government, head of state, minister of foreign affairs).
Each object should have the following fields: `country`, `full_title`,
`honorary_prefix`, `person_name`, `date_of_appointment`.
Return an empty string for unset fields.
"""


def crawl_pdf_url(context: Context) -> str:
    html = context.fetch_html(context.data_url)
    for a in html.findall('.//div[@class="content"]//a'):
        if "list.pdf" in a.get("href", ""):
            return urljoin(context.data_url, a.get("href"))
    raise ValueError("No PDF found")


def crawl(context: Context):
    pdf_url = crawl_pdf_url(context)
    path = context.fetch_resource("source.pdf", pdf_url)
    context.export_resource(path, PDF, title=context.SOURCE_TITLE)
    for page_path in h.make_pdf_page_images(path):
        data = run_image_prompt(context, prompt, page_path)
        assert "holders" in data, data
        for holder in data.get("holders", []):
            person_name = holder.get("person_name")
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
            entity.id = context.make_id(country, person_name)
            entity.add("topics", "role.pep")
            entity.add("name", holder.get("person_name"))
            entity.add("title", holder.get("honorary_prefix"))
            entity.add("country", country)

            position = h.make_position(
                context,
                name=full_title,
                country=country,
                topics=["gov.national"],
            )
            occupancy = h.make_occupancy(
                context, entity, position, start_date=holder.get("date_of_appointment")
            )

            # entity.add("date_of_appointment", )
            context.emit(entity, target=True)
            context.emit(position)
            if occupancy is not None:
                context.emit(occupancy)
