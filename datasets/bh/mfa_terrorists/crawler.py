from rigour.mime.types import PDF

from zavod import Context, helpers as h
from zavod.shed.gpt import run_image_prompt


# prompt = """
# Extract structured data from the following page of a PDF document. Return
# a JSON list (`entities`) in which each object represents a sanctioned entity
# and its alias (alias is always in the brackets). Each object should have the
# following fields: `entity`, `alias`. It can be either a person or an entity.
# If you cannot recognize the entity, return an empty string for both fields.
# """

  prompt = """
    Extract structured data from the following page of a PDF document. Return
    a JSON list (`entities`) in which each object represents an entry. 
    Each object should have the fields: `name`, `alias`, `nationality`, and `type`.
    If the entry is a person, set `type` to "person", and if it is an organization, 
    set to "organization". If you can't recognize, set all fields to an empty string.

    Example JSON output:
    {
        "entities": [
            {"name": "حاكم عبيسان الحميدي المطيري", "alias": "", "nationality": "سعودي - كويتي", "type": "person"},
            {"name": "Hezbollah", "alias": "", "type": "organization"},
            {"name": "", "alias": "", "type": ""}
        ]
    }
    If you cannot accurately determine or reconstruct a name or alias due to missing characters, 
    set the value as an empty string.
    """


def crawl(context: Context):
    path = context.fetch_resource("source.pdf", context.data_url)
    context.export_resource(path, PDF, title=context.SOURCE_TITLE)
    for page_path in h.make_pdf_page_images(path):
        data = run_image_prompt(context, prompt, page_path)
        assert "entities" in data, data
        for entity in data.get("entities", []):
            print(entity)
