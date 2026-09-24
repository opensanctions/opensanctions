# Article Crawler Examples

Code for the steps in `SKILL.md`. The rules behind it are in
`zavod/docs/extract/enforcements.md`.

## Index traversal

### Pattern A: newest-first index — stop paginating

The common case. The per-page function reports that it reached the age limit; `crawl()`
owns the loop.

```python
def crawl_index_page(context: Context, doc: Element) -> bool:
    """Crawl one index page. Returns False once we reach notices older than the limit."""
    table = h.xpath_element(doc, ".//div[contains(@class, 'view-content')]//table")
    for row in h.parse_html_table(table):
        # The index is sorted newest-first (the date column is "date_sort_descending"),
        # so the first out-of-age row ends the crawl.
        published_at = h.element_text(row["date"])
        if not h.within_max_age(context, published_at):
            return False
        url = h.xpath_string(row["enforcement_action"], ".//a/@href")
        crawl_notice(context, url, published_at)
    return True


def crawl(context: Context) -> None:
    next_url: str | None = context.data_url
    while next_url is not None:
        context.log.info("Crawling index page", url=next_url)
        doc = context.fetch_html(next_url, absolute_links=True)
        next_urls = h.xpath_strings(doc, ".//a[@rel='next']/@href")
        assert len(next_urls) <= 1, next_urls
        next_url = next_urls[0] if next_urls else None
        if not crawl_index_page(context, doc):
            break

    assert_all_accepted(context)
```

### Pattern B: index not sorted by date — skip the row

Same helper, opposite control flow. Breaking here would truncate the dataset at the first
old row.

```python
def crawl_row(context: Context, row: dict[str, Element]) -> None:
    str_row = h.cells_to_str(row)
    published_at = str_row.pop("date_sort_ascending")
    assert published_at is not None
    # The index is oldest-first, so an out-of-age row says nothing about the next one:
    # skip it and carry on rather than stopping the crawl.
    if not h.within_max_age(context, published_at):
        return
    ...
```

### Pattern C: the date is only on the article

The check moves after the fetch. `within_max_age` raises on an unparseable date: catch
that only where the source genuinely publishes non-dates.

```python
def crawl_notice(context: Context, url: str) -> None:
    doc = context.fetch_html(url, cache_days=7, absolute_links=True)
    # The date is the last bold paragraph of the body — a few notices have none.
    dates = h.xpath_strings(doc, "//div[@itemprop='articleBody']/p[strong][last()]/strong/text()")
    assert len(dates) <= 1, dates
    if not dates:
        context.log.warning("No publication date on notice", url=url)
        return
    if not h.within_max_age(context, dates[0]):
        return
    ...
```

## The extraction model

Full reference: `zavod/docs/data_reviews.md`. The `Field(description=...)` text reaches
both the prompt and the human reviewer, so write it for someone holding the article.

```python
Schema = Literal["Person", "Company", "LegalEntity"]


class RelatedCompany(BaseModel):
    name: str
    relationship: str


class Defendant(BaseModel):
    entity_schema: Schema = Field(
        description="Use LegalEntity if it isn't clear whether the entity is a person or a company."
    )
    name: str
    aliases: list[str] = Field(
        default=[],
        description=(
            "ONLY extract aliases that follow an explicit indication of an _alternative_ "
            'name, such as "also known as", "alias", "formerly", "aka", "fka". '
            "Acronyms or shortened forms used later in the article are NOT aliases."
        ),
    )
    country: list[str] = []
    related_companies: list[RelatedCompany] = []


class Defendants(BaseModel):
    defendants: list[Defendant]


PROMPT = f"""
Extract the defendants subject to the enforcement action in the attached notice.
NEVER include relief defendants, investigators or enforcement officers.
NEVER infer, assume, or generate values that are not directly stated in the source text.

Instructions for specific fields:

- entity_schema: {Defendant.model_fields["entity_schema"].description}
- aliases: {Defendant.model_fields["aliases"].description}
- country: Any countries the entity is indicated to reside, operate, or have been
  registered in. Leave empty if not explicitly stated.
"""
```

## One article, several entities

The Article is made once, before the loop. Each `Thing` the notice names gets its own
Documentation; the `Sanction` doesn't — it isn't a `Thing`, and carries the URL instead.

```python
def crawl_notice(context: Context, url: str, published_at: str) -> None:
    doc = context.fetch_html(url, cache_days=7, absolute_links=True)
    body = h.xpath_element(doc, ".//article[contains(@class, 'notice')]")
    title = h.element_text(h.xpath_element(doc, ".//h1[@class='page-title']"))

    source_value = HtmlSourceValue(
        # Key on the release ID, not the URL: URLs change when a site is reorganised
        # and every review would be re-requested.
        key_parts=get_release_id(url),
        label="Enforcement Action Notice",
        element=body,
        url=url,
    )
    prompt_result = run_typed_text_prompt(
        context, PROMPT, source_value.value_string, Defendants
    )
    review = review_extraction(
        context,
        source_value=source_value,
        original_extraction=prompt_result,
        origin=DEFAULT_MODEL,
    )
    if not review.accepted:
        return

    article = h.make_article(context, url, title=title, published_at=published_at)
    context.emit(article)

    for item in review.extracted_data.defendants:
        entity = context.make(item.entity_schema)
        entity.id = context.make_id(item.name, *item.country)
        entity.add("name", item.name, origin=review.origin)
        entity.add("alias", item.aliases, origin=review.origin)
        entity.add("country", item.country, origin=review.origin)
        entity.add("topics", "reg.action")

        sanction = h.make_sanction(context, entity, key=published_at)
        h.apply_date(sanction, "date", published_at)
        sanction.set("sourceUrl", url)

        context.emit(entity)
        context.emit(sanction)
        context.emit(h.make_documentation(context, entity, article))
```

A company the notice names alongside the defendant gets one too — it was read out of the
same article. The `UnknownLink` joining them is an edge, not a `Thing`, and gets none:

```python
        for related in item.related_companies:
            company = context.make("Company")
            company.id = context.make_id(related.name)
            company.add("name", related.name, origin=review.origin)

            link = context.make("UnknownLink")
            link.id = context.make_id("Related company", entity.id, company.id)
            link.add("subject", entity)
            link.add("object", company)
            link.add("role", related.relationship, origin=review.origin)

            context.emit(company)
            context.emit(link)
            context.emit(h.make_documentation(context, company, article))
```

## Heuristic first, review only what looks irregular

When most strings are one clean name and only a handful are lists, don't pay for an LLM
call per row: test each string with `h.is_name_irregular` or
`rigour.names.contains_split_phrase`, split the irregular ones naively with
`h.multi_split`, and pass only those to `review_extraction` with `origin="heuristic"` for
a reviewer to correct. For names, `h.apply_reviewed_name_string(context, entity,
string=raw_name)` does the detection, review and application in one call.

## Lookups for the parts that don't parse

**Article relevance**, on the source's own topic labels. `null` skips the article; an
unknown label warns, so a new category is somebody's decision:

```yaml
lookups:
  topics:
    # Topics mapped to null cause the article to be skipped: out of OpenSanctions'
    # scope. Add new labels here as the source introduces them.
    options:
      - match: Money laundering
        value: crime.fin
      - match: [Annual Report, Honours]
        value: null
```

```python
    topics: list[str] = []
    for raw_topic in h.xpath_strings(doc, "//li[@itemprop='keywords']/a/text()"):
        topic = context.lookup_value("topics", raw_topic.strip(), warn_unmatched=True)
        if topic is not None:
            topics.append(topic)
    if not topics:
        return
```

**Entity type**, where the source labels it — enumerate every value, and let a miss warn
and skip rather than falling back to a default schema:

```python
    schema = context.lookup_value("schema_type", entity_type, warn_unmatched=True)
    if schema is None:
        return
    entity = context.make(schema)
```

**Values that are well-formed but wrong** — a source typo, or a placeholder the model
emits instead of an empty list — go in a `type.*` lookup, applied automatically by
`entity.add()`:

```yaml
  type.country:
    lowercase: true
    options:
      # Placeholder emitted by the extraction model instead of an empty country list.
      - match: No country indicated
        value: null
  type.date:
    options:
      - match: 23 Janaury 2025. # january typo in the source
        value: "2025-01-23"
```

## Failing on an unrecognised notice

### Structural: crash

The title, body container and release ID are the same on every notice. If they aren't,
the parse is invalid.

```python
REGEX_RELEASE_ID = re.compile(r"(\w{2,8}-\w{2,4}[\w #-]*)$")


def get_release_id(url: str) -> str:
    path_suffix = url.split("/")[-1]
    match = REGEX_RELEASE_ID.search(path_suffix)
    assert match, f"Invalid release ID: {path_suffix}"
    return match.group(1)


def get_title(article: Element) -> str:
    # Every release carries the release number in the first h1, the title in the second.
    titles = h.xpath_elements(article, ".//h1", expect_exactly=2)
    assert "Release Number" in h.element_text(titles[0]), titles
    return h.element_text(titles[1])
```

### Per-notice: warn and skip

Some notices are published as a PDF behind a redirect link, or as a stub. The rest of the
crawl is still valid, so warn with the URL and return.

```python
MIN_BODY_LENGTH = 200


def fetch_article_body(context: Context, url: str) -> Element | None:
    doc = context.fetch_html(url, cache_days=30, absolute_links=True)
    body = h.xpath_element(doc, ".//article")
    if len(h.element_text(body)) < MIN_BODY_LENGTH:
        # TODO: parse the PDF notices.
        context.log.warning("Notice body is a stub, likely a PDF", url=url)
        return None
    return body
```

It never `assert`s on an individual notice, and it never tests a specific URL — a one-off
exclusion belongs in a lookup with a comment saying why.
