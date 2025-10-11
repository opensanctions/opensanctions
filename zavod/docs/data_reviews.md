# Data Reviews

When we don't believe that automated extraction will be sufficiently accurate,
we can use Data Reviews to request reviews by human reviewers who can fix extraction
issues before accepting an extraction result.


## Context

We want the following properties from reviews:

- We want to be notified when there are new reviews that need attention
- Data removed from the source should also drop out of the dataset.
- If the source data changes in a way that the automated extraction changes, e.g. for a correction, we want to update and re-review the data.
- If we change the data model, e.g. to extract additional fields, we want to be able to decide whether existing
  reviews should be redone or can be backward compatible.

    - We want incompatible data model changes to fail early and loudly.

- We want user-edited data changes to be validated early (ideally in the UI) to prevent painful slow review/editing turnaround time.


## Implementation

The basic workflow is:

1. define a pydantic model for the data
2. Perform the automated extraction
3. Call `review = review_extraction()`
4. If `review.accepted` is true, use `review.extracted_data`
5. Assert that all the reviews related to a given crawler are accepted using `assert_all_accepted`, opting to either emit a warning, or raise an exception. An exception is useful to prevent publishing a partial dataset - if we would prefer to hold off publishing a new release until the data has been accepted.
6. After the crawler has run, reviewers can review the data in Zavod UI and correct/accept extraction results. The crawler can then use the accepted data in its next run.

For example, imagine a crawler crawling web pages with regulatory notices.

```python
from zavod.shed.gpt import run_typed_text_prompt
from zavod.stateful.review import review_extraction, assert_all_accepted, HtmlSourceValue


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
    source_value = HtmlSourceValue(
        key_parts=notice_id(url),
        label="Notice of regulatory action taken",
        element=article_element,
        url=url,
    )
    prompt_result = run_typed_text_prompt(
        context,
        prompt=PROMPT,
        string=source_value.value_string,
        response_type=Defendants,
    )
    # If a review has previously been requested for the same source_value.key_parts,
    # it'll be found here.
    review = review_extraction(
        context,
        source_value=source_value
        original_extraction=prompt_result,
        origin=gpt.DEFAULT_MODEL,
    )
    # Once it's been accepted by a reviewer, we can use it
    if not review.accepted:
        return

    for item in review.extracted_data.defendants:
        entity = context.make(item.entity_schema)
        entity.id = context.make_id(item.name)
        entity.add("name", item.name, origin=review.origin)
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

### ::: zavod.stateful.review.review_extraction

### ::: zavod.stateful.review.assert_all_accepted

### ::: zavod.stateful.review.HtmlSourceValue

### ::: zavod.stateful.review.TextSourceValue


## Best practices


### Review keys

The key should uniquely identify a given piece of data extraction/review content. Ideally it should be consistent in spite of changes to the content, but this isn't always possible. Key input gets slugified by `review_extraction`.

For free text in a CSV cell that doesn't have a consistent identifier, e.g. `John, Sally, and partners`, just use the string as the key.

For web pages, e.g. of regulatory notices, try and use the notice ID if one can be extracted reliably, rather than the web page URL, because the the URL can change if they reorganise the website, and the notice ID could likely be extracted consistently despite such a change.


### Model Documentation

Use model documentation (e.g. `fieldname: MyEnum = Field(description="...")`) to explain how fields should be extracted. This gets
included in the JSON schema so it's made available to the human reviewer in Zavod UI.

OpenAI's structured output API doesn't seem to support JSON schema description properties yet so also include it explicitly in the prompt. See example above.
