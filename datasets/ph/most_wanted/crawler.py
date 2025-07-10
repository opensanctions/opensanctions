from pathlib import Path
from typing import List, Optional

from normality import slugify
from pydantic import BaseModel
from zavod import Context
from rigour.mime.types import PDF, PNG
from zavod import helpers as h
from zavod.shed.gpt import run_typed_image_prompt
from zavod.shed.zyte_api import fetch_resource
from zavod.stateful.extraction import extract_items


PROMPT = """
Extract all the wanted persons from the attached image.
Leave fields blank if that information is not present in the image.
"""


class WantedPerson(BaseModel):
    name: str
    alias: Optional[str] = None
    offenses: Optional[str] = None
    case_numbers: Optional[str] = None


class WantedPersonList(BaseModel):
    persons: List[WantedPerson]


def crawl_person(context: Context, person: WantedPerson):
    full_name = person.name
    offense = person.offenses
    case_number = person.case_numbers

    entity = context.make("Person")
    entity.id = context.make_id(full_name, case_number, offense)
    entity.add("name", full_name)
    entity.add("topics", "wanted")
    entity.add("country", "ph")
    entity.add("notes", offense)
    entity.add("notes", case_number)
    # Emit the entities
    context.emit(entity)


def crawl(context: Context):
    #path = fetch_resource(context, "source.pdf", context.data_url)
    path = Path(__file__).parent / "source.pdf"
    #context.export_resource(path, PDF, title=context.SOURCE_TITLE)
    for page_num, page_path in enumerate(h.make_pdf_page_images(path)):
        if page_num > 10:
            break
        # We want this to be consistent across crawls
        extraction_key = slugify([context.data_url, "page", page_num])
        assert extraction_key is not None
        # We want this to be distinct between versions so that garbage collecting a
        # version garbage collects only the resources for that version and not resources
        # referenced by versions we don't want to delete yet..
        archive_key = slugify([context.data_url, "page", page_num, context.version.id])
        image_url = context.archive_resource(page_path, PDF, archive_key)
        prompt_result = run_typed_image_prompt(
            context, PROMPT, page_path, WantedPersonList
        )
        accepted_result = extract_items(
            context,
            key=extraction_key,
            source_value=image_url,
            source_content_type=PNG,
            source_label="Screenshot of page in source PDF",
            orig_extraction_data=prompt_result,
            source_url=context.data_url,
        )
        if accepted_result is None:
            continue
        for person in accepted_result.persons:
            crawl_person(context, person)
