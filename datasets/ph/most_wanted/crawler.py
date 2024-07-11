from rigour.mime.types import PDF
from urllib.parse import urljoin

from zavod import Context
from zavod import helpers as h
from zavod.shed.gpt import run_image_prompt


def crawl(context: Context):
    # Fetch the source data URL specified in the metadata to a local path:
    source_path = context.fetch_resource("source.jpg", context.dataset.data.url)

    # Assuming the image is fetched correctly, now we need to process it.
    with open(source_path, "rb") as fh:
        image_data = fh.read()

    # Use run_image_prompt to process the image and extract text
    extracted_text = run_image_prompt(image_data)

    # Log the length of the extracted text for debugging purposes
    context.log.info(f"Extracted text length: {len(extracted_text)}")

    # Assuming extracted_text contains the relevant data, you can now process it further
    # For the sake of this example, we'll just log the extracted text
    context.log.info(f"Extracted text: {extracted_text}")

    # You can also register the image file as a resource with the dataset that
    # will be included in the exported metadata index:
    context.export_resource(source_path, title="Source data image file")
