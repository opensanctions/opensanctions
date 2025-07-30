# Data reviews

When we don't believe that automated extraction will be sufficiently accurate,
we can use Data Reviews to request reviews by human reviewers who can fix extraction
issues before accepting an extraction result.


## Context

- Re-prompting an LLM with 1000 text attachments is perhaps around $1-2
- Re-reviewing 1000 items is a big task

We want the following properties from reviews:

- If the source data changes, we can evaluate whether the change should trigger re-extraction and/or re-reviewing.
  - We want to be notified when this occurs.
- If we change the data model, e.g. to extract additional fields, we want to be able to decide whether existing
  reviews should be redone or can be backward compatible.
- We want user-edited data changes to be validated early (ideally in the UI) to prevent painful slow review/editing turnaround time.
- We want incompatible data model changes to fail early and loudly.
- We want to be notified when there are new reviews that need attention


## Implementation

The basic workflow is:

1. define a pydantic model for the data

2. check if a review exists for the given review key (with a minimum compatible version)

3. if the review exists, and it is accepted, use the extracted data in the crawler

4. if not

    4.1. Perform some automated data extraction, perhaps using an LLM

    4.2. Request a review (optionally with accepted=True if the risk of bad extraction is extremely low)

5. Optionally assert that all the reviews related to a given crawler are accepted - if the dataset prioritises completeness over liveness.

For example, imagine a crawler crawling web pages with regulatory notices.

```python
from zavod.shed.gpt import run_typed_text_prompt
from zavod.stateful.review import request_review, get_review

VERSION = 1
MIN_VERSION = 1

Schema = Literal["Person", "Company", "LegalEntity"]

class Defendant(BaseModel):
    entity_schema: Schema = Field(
        description="Use LegalEntity if it isn't clear whether the entity is a person or a company."
    )
    name: str


class Defendants(BaseModel):
    defendants: List[Defendant]


PROMPT = """
Extract the defendants in the attached article. Only include names mentioned
in the article text.
"""

def crawl_page(context: Context, url: str, page: _Element) -> None:
    article = doc.xpath(".//article")[0]
    html = tostring(article, pretty_print=True).decode("utf-8")

    # This tries to fetch a review and update its last-seen version. It will validate
    # existing data against the provided Defendants model.
    review = get_review(context, Defendants, url, MIN_VERSION)
    if review is None:
        prompt_result = run_typed_text_prompt(
            context, PROMPT, html, response_type=Defendants
        )
        # This requests a review, replacing an existing review if one exists
        # (e.g. with an older crawler version)
        review = request_review(
            context,
            key=notice_id(url),
            source_value=html,
            source_data_hash=text_hash,
            source_content_type="text/html",
            source_label="Enforcement Action Notice",
            source_url=url,
            orig_extraction_data=prompt_result,
            crawler_version=VERSION,
        )
    if not review.accepted:
        return

    for item in review.extracted_data.defendants:
        entity = context.make(item.entity_schema)
        entity.id = context.make_id(item.name)
        entity.add("name", item.name)
        context.emit(entity)

def crawl(context: Context) -> None:
    ...
    for url in urls:
        ...
        crawl_page(context, url, page)

    # This will raise an exception unless all the reviews fetched or requested
    # during a given crawl have `accepted == True`
    assert_all_accepted(context)
```


## Best practices


### Review keys

The key should uniquely identify a given piece of data extraction/review content. Ideally it should be consistent in spite of changes to the content, but this isn't always possible.

For free text in a CSV cell that doesn't have a consistent identifier, e.g. `John, Sally, and partners`, just use the string as the key.

For web pages, e.g. of regulatory notices, try and use the notice ID if one can be extracted reliably, rather than the web page URL, because the the URL can change if they reorganise the website, and the notice ID could likely be extracted consistently despite such a change.


### Model Documentation

Use model documentation to explain how fields should be extracted. This gets
included in the JSON schema which is seen by both the LLM and the human reviewer.


## Handling changes

The following changes are anticipanted:

| Change                   | Example                                | Strategy |
|--------------------------|----------------------------------------|----------|
| The source data changes  | A spelling mistake in a name was fixed | Find the review in the review UI by its key, mark it as unaccepted, and let the next crawl re-request a review |
| The source data changes  | They changed the markup on their site  | To start with, try and reduce the impact of markup changes by making sure the hash is of only the content that impacts the extraction, e.g. consider whether the hash of the slug of the article text is sufficient. If a change occurs that affects hundreds or thousands of reviews and we can (1) identify the change automatically and (2) safely disregard it in all occurrences, write a [data migration](#data-migrations) to update the crawler_version, source_value, source_data_hash.|
| We change the data model in a backward-compatible way | We add an optional field | Make sure to define a default value in the model (e.g. `None`) so that the existing data can be used to create valid instances of the model. |
| We change the data model in an incompatible way | We add a new required field | Increment MIN_VERSION to trigger re-extraction and re-review |


## Data Migrations

We might want to perform a bulk updates to reviews to handle changes without requiring re-reviewing hundreds or thousands of items.

This could be e.g.

- to handle source content changes that affected the content hash but ought not to affect the extracted content, and therefore shouldn't trigger re-review

- to handle data model changes that are backward incompatible but can't be handled by default values in the model

    - e.g. removing an enum value? Maybe this can be handled by a custom validator in the model?

!!! warning "Consider the cost"
    It might be cheaper to just re-review than to to spend a day or two to implement
    the perfect migration.

    Consider just calling `request_review` on the affected items instead of implementing
    a migration.


Imagine we got warnings in `get_review` for 1000 pages where the content changed.
Looking at the source content in the review UI and the HTML on the website, we
see they've rearranged the article layout a bit. Despite using the article text
in the hash, the change means some standard text has now moved. This seems to affect
all the pages for the years 2012-present. We can identify the change, and if we strip
the first few lines of each page, we can tell that the rest of the content hasn't changed.
Consider updating the example above with function that carries out the migration:

```python
def migrate_content_2026_08_23(context: Context, key: str) -> None
    # Fetch, check and update inside transaction
    with get_engine().begin() as conn:
        tx_review = Review.by_key(review.key)
        old_text = strip_header(tx_review.source_value.text_content())
        new_text = strip_header()
        if slugify(old_text) == slugify(new_text):
            review.source_value = new_text
            review.source_data_hash = hash(new_text)
            review.save()


def crawl_page(context: Context, url: str, page: _Element) -> None:
    article = doc.xpath(".//article")[0]
    html = tostring(article, pretty_print=True).decode("utf-8")
    text_hash = hash(slugify(article.text_content()))

    #===========
    # We run the migration before fetching the review
    migrate_content_2026_08_23(context, notice_id(url))
    #===========

    review = get_review(context, Defendants, url, MIN_VERSION, text_hash)
    if review is None:
        prompt_result = run_typed_text_prompt(
            context, PROMPT, html, response_type=Defendants
        )
        review = request_review(
            ...
        )
    if not review.accepted:
        return
```
