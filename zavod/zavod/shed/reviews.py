from lxml.html import HtmlElement, fromstring

from zavod.context import Context
from zavod.shed.gpt import run_typed_text_prompt
from zavod.stateful.review import ModelType, Review, model_hash, html_to_text_hash


class ChangeTracker:
    """
    A utility for tracking changes to source data and raising to prevent export if
    changes were detected.
    """

    def __init__(self, context: Context):
        self.context = context
        self.checks = 0
        self.changes = 0

    def increment_checks(self) -> None:
        self.checks += 1

    def increment_changes(self) -> None:
        self.changes += 1

    def raise_for_changes(self) -> None:
        if self.changes > 0:
            raise Exception(f"Changes detected: {self.changes} of {self.checks} checks")


class HtmlChangeTracker(ChangeTracker):
    """
    Track changes to HTML source, logging whether only the source, or also the
    LLM-prompted extraction result, has changed.
    """

    def check_changes(
        self, review: Review[ModelType], element: HtmlElement, html: str, prompt: str
    ) -> None:
        """
        Check for changes to HTML source, and if so, to LLM-extracted data.

        Args:
            review: The review object containing the original source and extracted data.
            element: The HtmlElement to use for hash comparison.
            html: The HTML serialized from the element for the prompt in the crawler.
            prompt: The prompt used for extracting data.
        """
        self.increment_checks()

        seen_element = fromstring(review.source_value)
        seen_content_hash = html_to_text_hash(seen_element)
        crawl_content_hash = html_to_text_hash(element)

        if seen_content_hash != crawl_content_hash:
            self.increment_changes()

            prompt_result = run_typed_text_prompt(
                self.context, prompt, html, review.data_model
            )

            if model_hash(prompt_result) == model_hash(review.orig_extraction_data):
                self.context.log.warning(
                    "The source content has changed but the extracted data has not.",
                    url=review.source_url,
                    seen_source_value=review.source_value,
                    new_source_value=html,
                )
            else:
                self.context.log.warning(
                    "The source content and extracted data have changed.",
                    url=review.source_url,
                    orig_extracted_data=review.orig_extraction_data.model_dump(),
                    prompt_result=prompt_result.model_dump(),
                )
