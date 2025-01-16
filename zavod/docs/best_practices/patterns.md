# Usage patterns

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
    from normality import collapse_spaces, stringify

    # Local application imports
    from zavod import helpers as h
    ```

    The project-specific imports (like `from zavod import helpers as h`) should appear under all other imports and be separated by a blank line for clarity.

    To enforce this convention, run the following `Ruff` command:

    ```bash
    ruff check --fix --select I /path/to/crawler.py
    ```

- Define and precompile regular expressions as constants at the top of the module.

    ```python
    REGEX_DETAILS = re.compile(r"your_regex_pattern_here")
    ```

- When naming functions for data extraction or processing tasks, it's important to be specific and clear. For example, use `crawl_entity()` instead of a generic name like `process_data()`.

    ```python
    def crawl_entity():  # Better than process_data()
        pass
    ```

    !!! note
        We typically use the `crawl_thing` convention (e.g., `crawl_person`, `crawl_row`, `crawl_index`) for functions that lead to entities being emitted (directly or via a nested `crawl_` function call). 

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

    You can also invert the condition and return early when the check fails. This pattern minimizes the need for additional indentation.
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

- Utilize `h.copy_address()` to manage address processing.

    ```python
    # Create an address entity using the helper function
    address_ent = h.make_address(context, full=addr, city=city, lang="zhu")
    # Copy address details to the entity
    h.copy_address(entity, address_ent)
    ```

## Detect unhandled data

If a variable number of fields can extracted automatically (e.g. from a list or table):

* Capture all fields provided by the data source in a `dict`.
* `dict_obj.pop()` individual fields when adding them to entities.
* Log warnings if there are unhandled fields remaining in the `dict` so that we notice and improve the crawler. The context method [`context.audit_data()`][zavod.context.Context.audit_data] can be used to warn about extra fields in a `dict`. It takes the `ignore` argument to explicitly list fields that are unused.

## Logging and crawler feedback

It is good design to be told about issues, instead of having to go look to discover them.

Logs are essential for monitoring progress and debugging, but info-level logs don't help us notice if something is missing because we only see them when we choose to go and look at a crawler's logs. Use the appropriate log level for the purpose for cases that don’t stop the process.

* Debug Logs: Enable verbose output for detailed tracking during development. Use `zavod --debug` to activate debug logs.

    ```python
    context.log.debug(f"Unique ID {person.id}")
    ```

* Info Logs: Monitor the crawler’s progress, especially on large sites.

    ```python
    context.log.info(f"Processed {page_number} pages")
    ```

* Warning Logs: Indicate potential issues that don't stop the crawl but may require attention. These are surfaced to the dev team on the [Issues](https://www.opensanctions.org/issues/) page and checked daily.
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

## Generating consistent unique identifiers

Make sure entity IDs are unique within the source. Avoid using only the name of the entity because there might eventually be two persons or two companies with the same name. [It is preferable](https://www.opensanctions.org/docs/identifiers) to have to deduplicate two Follow the Money entities for the same real world entity, rather than accidentally merge two entities. 

Good values to use as identifiers are:

* An ID in the source dataset, e.g. a sanction number, company registration number. These can be turned into a readable ID with the dataset prefix using the [`context.make_slug`][zavod.context.Context.make_slug] function.
* Some combination of consistent attributes, e.g. a person's name and normalised date of birth in a dataset that holds a relatively small proportion of the population so that duplicates are extremely unlikely. These attributes can be turned into a unique hash describing the entity using the [`context.make_id`][zavod.context.Context.make_id] function.
* A combination of identifiers for the entities related by another entity, e.g. an 
  owner and a company, in the form `ownership.id = context.make_id(owner.id, "owns", company.id)`

!!! note

    Remember to make sure distinct sanctions, occupancies, positions, relationships, etc get distinct IDs.

!!! note

    Do not reveal personally-identifying information such as names, ID numbers, etc in IDs, e.g. via `context.make_slug`.

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

- `normality.collapse_spaces`
- `normality.remove_unsafe_chars`
- `.replace`

```python
import normality

# Replace non-breaking space with regular space
text = text.replace("\xa0", " ")

# When the source data contains messy or excessively repeated whitespace,
# e.g., collapsing whitespace from text extracted from HTML
cleaned_text = normality.collapse_spaces(text)

# Remove unsafe characters if needed
cleaned_text_safe = normality.remove_unsafe_chars(text)
```

## Use datapatch lookups to clean or map values from external forms to OpenSanctions

See [Datapatches](datapatch_lookups.md)

e.g.

- Fixing typos in dates
- Translating column headings to English
- Mapping source data entity types to FollowTheMoney entity types
- Mapping relationship descriptions to FollowTheMoney relation entity types
