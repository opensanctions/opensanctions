# Data Reviews

When we don't believe that automated extraction will be sufficiently accurate,
we can use Data Reviews to request reviews by human reviewers who can fix extraction
issues before accepting an extraction result.


## Context

- Re-prompting an LLM with 1000 text attachments is perhaps around $10
- Re-reviewing 1000 items is a big task

We want the following properties from reviews:

- We want to be notified when there are new reviews that need attention
- If the source data changes, we can evaluate whether the change should trigger re-extraction and/or re-reviewing.

    - We want to be notified when this occurs.

- If we change the data model, e.g. to extract additional fields, we want to be able to decide whether existing
  reviews should be redone or can be backward compatible.

    - We want incompatible data model changes to fail early and loudly.

- We want user-edited data changes to be validated early (ideally in the UI) to prevent painful slow review/editing turnaround time.


## Implementation

The basic workflow is:

1. define a pydantic model for the data
2. check if a review exists for the given review key (with a minimum compatible version)
3. if the review exists, and it is accepted, use the extracted data in the crawler
4. if not

    4.1. Perform some automated data extraction, perhaps using an LLM

    4.2. Request a review (optionally with accepted=True if the risk of bad extraction is extremely low)

5. Assert that all the reviews related to a given crawler are accepted, opting to either emit a warning, or raise an exception. An exception is useful to prevent publishing a partial dataset - if we would prefer to hold off publishing a new release until the data has been accepted.
6. After the crawler has run, reviewers can review the data in Zavod UI and correct/accept extraction results. The crawler can then use the accepted data in its next run.

For example, imagine a crawler crawling web pages with regulatory notices.

```python
from zavod.shed.gpt import run_typed_text_prompt
from zavod.stateful.review import request_review, get_review

VERSION = 1
MIN_VERSION = 1

Schema = Literal["Person", "Company", "LegalEntity"]

schema_field = Field(
    description=(
        "Use LegalEntity if it isn't clear whether the entity is a person or a company."
    )
)


class Defendant(BaseModel):
    entity_schema: Schema = schema_field
    name: str


class Defendants(BaseModel):
    defendants: List[Defendant]


PROMPT = f"""
Extract the defendants in the attached article. ONLY include names mentioned
in the article text.

Instructions for specific fields:

  - entity_schema: {schema_field.description}
"""

def crawl_page(context: Context, url: str, page: _Element) -> None:
    article = doc.xpath(".//article")[0]
    html = tostring(article, pretty_print=True).decode("utf-8")

    # This tries to fetch a review and update its last-seen version. It will
    # validate existing data against the provided Defendants model.
    review = get_review(context, Defendants, url, MIN_VERSION)
    if review is None:
        prompt_result = run_typed_text_prompt(
            context, PROMPT, html, response_type=Defendants
        )
        # This requests a review, replacing an existing review if one exists
        # (e.g. with an older crawler version)
        review = request_review(
            context,
            key_parts=notice_id(url),
            source_value=html,
            source_mime_type="text/html",
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

    # This will raise an exception unless all the reviews fetched
    # during a given crawl have `accepted == True`.
    assert_all_accepted(context)
```

### ::: zavod.stateful.review.request_review

### ::: zavod.stateful.review.get_review

### ::: zavod.stateful.review.assert_all_accepted

## Best practices


### Review keys

The key should uniquely identify a given piece of data extraction/review content. Ideally it should be consistent in spite of changes to the content, but this isn't always possible. Key input gets slugified by the reviews functions.

For free text in a CSV cell that doesn't have a consistent identifier, e.g. `John, Sally, and partners`, just use the string as the key.

For web pages, e.g. of regulatory notices, try and use the notice ID if one can be extracted reliably, rather than the web page URL, because the the URL can change if they reorganise the website, and the notice ID could likely be extracted consistently despite such a change.


### Model Documentation

Use model documentation (e.g. `fieldname: MyEnum = Field(description="...")`) to explain how fields should be extracted. This gets
included in the JSON schema so it's made available to the human reviewer in Zavod UI.

OpenAI's structued output API doesn't seem to support JSON schema description properties yet so include it explicitly in the prompt.


## Handling changes

The following changes are anticipanted:

| Change                   | Example                                | Strategy |
|--------------------------|----------------------------------------|----------|
| The source data changes  | A spelling mistake in a name was fixed | Find the review in the review UI by its key, mark it as unaccepted, and let the next crawl re-request a review |
| The source data changes  | They changed the markup on their site  | We're not sure how consistently LLMs will extract data. We've seen variation from one query to the next for the same prompt and data when more complex data was requested, e.g. the relationship between entities in a document. For simple things like splitting 100 names in a CSV, it's probably safe to just throw at GPT and review changes that come up. For thousands of enforcement press releases, consider detecting changes before triggering re-reviews to understand the scale. See [Detecting changes](#detecting-changes) below. |
| We change the data model in a backward-compatible way | We add an optional field | Make sure to define a default value in the model (e.g. `None`) so that the existing data can be used to create valid instances of the model. |
| We change the data model in an incompatible way | We add a new required field | Increment MIN_VERSION to trigger re-extraction and re-review |


### Detecting changes

You could take a couple of strategies to detect the scale of changes in source and extracted data before allowing the crawler to trigger massive sets of re-reviews:


#### Warn about all changes without publishing

Until we know how stable GPT extraction is for a given crawler, we might want to
get notified of changes in the source _and_ changes in the GPT extraction response.
We can do that by hashing the data (appropriately normalised) and comparing to the
stored versions.

```python
something_changed = False

def crawl_page():
    for page in pages:
        ...
        seen_article = lxml.html.fromstring(review.source_value)
        if h.element_text_hash(seen_article) != h.element_text_hash(article_element):
            global something_changed
            something_changed = True

            prompt_result = run_typed_text_prompt(
                context, PROMPT, article_html_string, Defendants
            )
            if model_hash(prompt_result) != model_hash(review.orig_extraction_data):
                context.log.warning(
                    "The extracted data has changed",
                    url=url,
                    orig_extracted_data=review.orig_extraction_data.model_dump(),
                    prompt_result=prompt_result.model_dump(),
                )
            else:
                context.log.warning(
                    "The source content has changed but the extracted data has not",
                    url=url,
                    seen_source_value=review.source_value,
                    new_source_value=article_html_string,
                )
                return

def crawl():
    ...
    global something_changed
    assert not something_changed, "See what changed to determine whether to trigger re-review."
```


#### Request re-review if the extraction result changed

If we've seen an abundance of evidence that

- the automated extraction won't miss real data changes, e.g. corrections by the source
- the automated extraction doesn't falsely give different responses when queried with effectively the same input, e.g. a bulk markup change with no content change

then we can allow the crawler to trigger re-extraction and re-reviews automatically if the data changed:

```python
for page in pages:
    ...
    seen_article = lxml.html.fromstring(review.source_value)
    if h.element_text_hash(seen_article) != h.element_text_hash(article_element):
        prompt_result = run_typed_text_prompt(
            context, PROMPT, html, response_type=Defendants
        )
        if model_hash(prompt_result) != model_hash(review.orig_extracted_data):
            review = request_review(...)
```
