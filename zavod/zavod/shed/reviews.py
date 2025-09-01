from difflib import Differ
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

        This is useful for understanding the scope of changes to the source data
        and whether/how a crawler should be adapted to re-request reviews for corrections
        to specific source documents, or adapted to handle broad changes, e.g. a website
        layout change.

        The diff in the issue can be read more comfortably with e.g.
        ```
        cat issues.json | jq '.issues[] | .data.url + "\n" + .data.diff' --raw-output
        ```

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
                    diff=self.diff_text_content(seen_element, element),
                )
            else:
                self.context.log.warning(
                    "The source content and extracted data have changed.",
                    url=review.source_url,
                    orig_extracted_data=review.orig_extraction_data.model_dump(),
                    prompt_result=prompt_result.model_dump(),
                    diff=self.diff_text_content(seen_element, element),
                )

    @staticmethod
    def diff_text_content(seen_element: HtmlElement, new_element: HtmlElement) -> str:
        seen_text_lines = seen_element.text_content().splitlines(keepends=True)
        new_text_lines = new_element.text_content().splitlines(keepends=True)
        differ = Differ()
        diff = list(differ.compare(seen_text_lines, new_text_lines))
        return "\n".join(diff)
