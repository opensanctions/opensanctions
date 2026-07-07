# Common Patterns

Several of the patterns below (detecting unhandled data, assertions, warnings) exist to serve one principle: [strict interpretation](strict_interpretation.md) — every source value within the crawler's scope is handled, explicitly ignored, or raises a signal.

The following are some patterns that have proved useful:

## Common crawler code structure

Our typical crawler structure consists of

1. a `crawl` function as the entrypoint which
    - fetches the data
    - converts it into an iterable of dicts, one per record
    - a loop over those records, calling...
2. a function called once per record, e.g. `crawl_item` or `crawl_person` which
    - unpacks the record dict
    - ensures the necessary cleaning takes place
    - creates one or more entities for the record (LegalEntity, Sanction, Position, relations, etc)
    - emits the created entities

We have a number of helpers to turn common formats into an iterable of dicts:

- [`h.parse_html_table()`][zavod.helpers.parse_html_table]
- [`h.parse_pdf_table()`][zavod.helpers.parse_pdf_table]
- [`h.parse_xlsx_sheet()`][zavod.helpers.parse_xlsx_sheet]
- [`h.parse_xls_sheet()`][zavod.helpers.parse_xls_sheet]
- [`csv.DictReader`](https://docs.python.org/3/library/csv.html#csv.DictReader)

We typically `from zavod import helpers as h`.

When concise-enough to fit on a single line and only used once, we pop and add values on the same line:

```python
entity.add("name", row.pop("full_name"))
entity.add("birthPlace", row.pop("place_of_birth"))
```

The method `entity.add` works seamlessly with both a single string and a list of strings. In the long run, however, we want to make the typing of `entity.add` more strict to accept only one argument at a time. With this in mind, it's generally better to add values individually if they are already in that form, rather than forcing them into a list unnecessarily.

  ```python
for name in h.multi_split(names, SPLITS):
    entity.add(name)
  ```

## Code structuring nitpicks

- `Ruff` can help with sorting imports in ascending order, ensuring consistency across your codebase. The convention is to group standard library imports first, followed by third-party imports, and then project-specific imports.

    Each group should be separated by a blank line for clarity. For example (don't include the comments):

    ```python
    # Standard library imports
    import os
    import sys

    # Third-party imports
    from normality import squash_spaces, stringify

    # Local application imports
    from zavod import helpers as h
    ```

    The project-specific imports (like `from zavod import helpers as h`) should appear under all other imports and be separated by a blank line for clarity.

    To enforce this convention, run the following `Ruff` command:

    ```bash
    ruff check --fix --select I /path/to/crawler.py
    ```

- Keep a value's meaning next to where it's used. Prefer inlining a literal at its single call site over hoisting it into a module-level constant referenced only once. A constant referenced once adds a hop: the reader hits `make_position(..., topics=TOPICS)` or a `COLUMNS = [...]` reference and has to scroll back to the top of the file to recover what is, in the end, a one-off literal.

    Declare a named constant only when it earns its keep:

    - **Performance or correctness.** Precompile regular expressions as constants at the top of the module. They are compiled once and reused, so the constant pays for itself even at a single call site.

        ```python
        REGEX_DETAILS = re.compile(r"your_regex_pattern_here")
        ```

    - **Genuine reuse.** The value appears at two or more call sites, so a single source of truth keeps the copies from drifting apart.
    - **Enumeration that is itself the data.** A mapping such as a `POSITIONS` dict that collects all the known cases in one table.

    Otherwise, inline it. Single-use `TOPICS`, `COLUMNS`, header-name lists and the like read better as literals at the point of use.

- When naming functions for data extraction or processing tasks, it's important to be specific and clear. For example, use `crawl_entity()` instead of a generic name like `process_data()`.

    ```python
    def crawl_entity():  # Better than process_data()
        pass
    ```

    !!! note
        We typically use the `crawl_thing` convention (e.g., `crawl_person`, `crawl_row`, `crawl_index`) for functions that lead to entities being emitted (directly or via a nested `crawl_` function call).

- Define helper functions at module level, not nested inside another function. Give the helper a clear, specific name and pass it what it needs as arguments rather than capturing values from the enclosing scope. A module-level function is easier to name, test, and read than a closure buried in a larger function.

    ```python
    # Avoid: a closure buried inside another function
    def crawl(context):
        def parse(row):
            ...

    # Prefer: a named function at module level
    def parse_row(context, row):
        ...

    def crawl(context):
        ...
    ```

- To improve readability and maintainability, break down deeply nested logic into smaller, focused functions.

    ```python
    for link in main_grid.xpath(".//a/@href"):
        # Break down the handling of different data types into separate functions
        if data_type == "vessel":
            # A separate function to handle vessel data processing
            crawl_vessel(context, link, program)
        elif data_type == "legal_entity":
            # A separate function to handle legal entity data processing
            crawl_legal_entity(context, link, program)
    ```

    It's nice to handle cases where we can return early first, often by inverting an `if A else return None ` with `if not A return None; B`. This pattern also reduces indentation for the `B` clause.

    ```python
    # Extract required fields from the row
    name = row.pop("name")
    listing_date = row.pop("listing_date")

    # Proceed only if both 'name' and 'listing_date' are available
    if not (name and listing_date):
        return

    # Create the entity
    entity = context.make("LegalEntity")
    entity.id = context.make_id(name, listing_date)
    ```

- Instead of using `urljoin` from `urllib.parse`, leverage `.make_links_absolute()` for cleaner URL resolution. This ensures all relative URLs are converted to absolute URLs within the crawler.

    ```python
    # Make all relative links in the document absolute using the data_url as the base
    doc = context.fetch_html(context.data_url)
    doc.make_links_absolute(context.data_url)
    ```

## Addresses

See the [addresses guide](addresses.md) for the full pattern, country handling, and the choice between `copy_address` and `apply_address`.

## Detect unhandled data

If a variable number of fields can extracted automatically (e.g. from a list or table):

* Capture all fields provided by the data source in a `dict`.
* `dict_obj.pop()` individual fields when adding them to entities.
* Log warnings if there are unhandled fields remaining in the `dict` so that we notice and improve the crawler. The context method [`context.audit_data()`][zavod.context.Context.audit_data] can be used to warn about extra fields in a `dict`. It takes the `ignore` argument to explicitly list fields that are unused.

## Fields with no FollowTheMoney equivalent

Source data sometimes includes descriptive fields (physical appearance, internal classification codes, administrative metadata, etc.) that don't map to any FTM property. Resist the temptation to pack these into narrative fields like `notes` or `description` as formatted strings (e.g. `f"Hair colour: {hair_colour}"`). This mixes structured data into free text where it can't be queried or validated, and clutters entity descriptions with noise.

Instead, explicitly ignore them via `context.audit_data`:

```python
context.audit_data(row, ignore=["hair_colour", "skin_tone", "internal_ref"])
```

If a field has a clear use case for commercial screening or geopolitical research users — and structured support for it would add real value — propose adding it to the FollowTheMoney schema by opening an issue or PR in the [FollowTheMoney repository](https://github.com/opensanctions/followthemoney) with concrete examples from the source data.

## Logging and crawler feedback

It is good design to be told about issues, instead of having to go look to discover them.

Logs are essential for monitoring progress and debugging, but info-level and lower is only seen when we choose to go and look at a crawler's logs, so we might not notice from them if something is wrong except during debugging/development. Use the appropriate log level for the purpose.

* Debug Logs: Enable verbose output for detailed tracking during development. Use `zavod --debug` to activate debug logs.

    ```python
    context.log.debug(f"Unique ID {person.id}")
    ```

* Info Logs: Monitor the crawler’s progress, especially on large sites.

    ```python
    context.log.info(f"Processed {page_number} pages")
    ```

* Warning Logs: Indicate potential issues that don't stop the crawl but may require attention. These are surfaced to the dev team on the [Issues](https://www.opensanctions.org/issues/) page and checked daily.

    Don't use warnings for things we know we won't fix, e.g. a permanent 404 that we can't do anything about. Do use warnings for things we should take action on, e.g. to notice a new entity type which we haven't mapped to a Schema yet.

    ```python
    context.log.warning("Unhandled entity type", type=entity_type)
    ```

## Data assertions

Build crawlers with robust assertions to catch missing data during runtime. Instead of manually inspecting logs, implement checks to ensure that expected data is present or that invalid data doesn't slip through:

```python
# Ensure a valid date of birth (dob)
assert dob is None, (dob, entity_name)

# Validate Position Name
assert position_name != "Socialdemokratiet"

# Check for Non-None Position Name
assert position_name is not None, entity.id
```

## Capture text in its original language

Useful fields like the reason someone is sanctioned should be captured regardless of the language it is written in. Don't worry about translating fields where arbitrary text would be written. If the language is known, include the three-letter language code in the `lang` parameter to `Entity.add()`, e.g.:

```python
reason = data.pop("expunerea-a-temeiului-de-includere-in-lista-a-operatorului-economic")
sanction.add("reason", reason, lang="rom")
```

## Handling special space characters in strings

Be aware of different types of space characters and how they affect text comparison. For example, a non-breaking space (`\xa0`) or zero-width space do not match a normal space character and can affect string comparison or processing.

An editor like VS Code highlights characters like this by default, and a hex editor is an effective way to see more precisely which values are present in strings that are surprising you. Remember that a hex editor is looking at the data encoded e.g. to `utf-8` while Python strings are `unicode` code points.

To handle these cases, you can use string cleaning methods such as:

- `normality.squash_spaces`
- `normality.remove_unsafe_chars`
- `.replace`

```python
import normality

# Replace non-breaking space with regular space
text = text.replace("\xa0", " ")

# When the source data contains messy or excessively repeated whitespace,
# e.g., collapsing whitespace from text extracted from HTML
cleaned_text = normality.squash_spaces(text)
```

## Pagination

Pagination logic should be easy to read, and fail early and loudly if the source
changes in a way that makes the logic invalid.

It's often nice to implement pagination
in a way that closely reflects the controls presented to the user, e.g.

- loop until the current page number is the max page number
- loop while there is a next URL (as opposed to looping until the next button isn't found, see below)

Think about how we ensure we visit all pages, but we don't end up in an infinite loop.
If there are many pages and entities, it's easy for dataset assertions to catch
if we're visiting too few pages.

Prefer code that fails in a way that clearly indicates what went wrong. e.g.
a KeyError if the API response changes:

```python
next_url: Optional[str] = context.data_url
while next_url:
    response = context.fetch_json(next_url)
    next_url = response["links"]["next"]  # KeyError if structure changes; None on last page
    for item in response["data"]:
        crawl_row(context, item)
```

Watch out for code where we might miss breaking out of the loop.
`while True` in general easily results in infinite loops.

e.g. an HTML source might change the class used to indicate that the "next page"
button is disabled. Looping until the last page button is disabled might result
in an infinite loop if we loop until a disabled next button can be selected using xpath.

```python
while True:
    next_disabled = doc.xpath(".//span[text()='Next' and contains(@class, 'disabled')]")
    if next_disabled:
        break
```

If another obvious cue is available like a max page number, consider using that.

If there is no more robust way to implement it than a while True loop, count the pages
and assert that we haven't reached some extreme case, e.g.

```python
pages = 0
while True:
    pages += 1
    # We expect about 10 pages. If we've reached 100, something's broken.
    assert pages < 100, pages
```


## Use datapatch lookups to clean or map values from external forms to OpenSanctions

See [Datapatches](datapatch_lookups.md)

e.g.

- Fixing typos in dates
- Translating column headings to English
- Mapping source data entity types to FollowTheMoney entity types
- Mapping relationship descriptions to FollowTheMoney relation entity types
