---
name: typechecker-fixes
description: Fix mypy --strict type errors in crawler files. Use when the user asks to make the typechecker happy, fix types, or add type annotations to a crawler.
argument-hint: "[crawler.py path]"
---

# Typechecker Fixes for Crawlers

Fix mypy strict-mode errors in opensanctions crawler files. Run `mypy --strict` on the target file to find errors, then apply the patterns below.

## Read first

- `zavod/zavod/helpers/html.py` — typed HTML helpers (`xpath_strings`, `xpath_elements`, `xpath_element`, `element_text`)
- `zavod/zavod/util.py` — `Element` and `ElementOrTree` type aliases
- `zavod/zavod/extract/zyte_api.py` — typed Zyte API helpers

## Workflow

1. Run `mypy --strict <crawler.py>` to see current errors
2. Apply fixes using the patterns below
3. Run `mypy --strict <crawler.py>` again to verify errors resolved

## Patterns (most common first)

### 1. Add `-> None` return type to all functions that don't return a value

This is the single most common fix. Every `crawl()`, `crawl_row()`, `crawl_item()`, `parse_*()`, and helper function that doesn't return needs `-> None`.

```python
# Before
def crawl(context: Context):
def crawl_row(context: Context, row: dict):
def apply_identifier(context: Context, entity: Entity, id_number_line: str):

# After
def crawl(context: Context) -> None:
def crawl_row(context: Context, row: dict[str, Any]) -> None:
def apply_identifier(context: Context, entity: Entity, id_number_line: str) -> None:
```

### 2. Add type annotations to all untyped parameters

```python
# Before
def crawl_item(input_html, context: Context):
def crawl_term(context, link: HtmlElement, ...):

# After
def crawl_item(input_html: Element, context: Context) -> None:
def crawl_term(context: Context, link: HtmlElement, ...) -> None:
```

### 3. Replace bare `dict` with `dict[str, Any]` or a more specific type

```python
# Before
def crawl_item(input_dict: dict, context: Context):
json_data = { ... }

# After
from typing import Any
def crawl_item(input_dict: dict[str, Any], context: Context) -> None:
json_data: dict[str, Any] = { ... }
```

Use lowercase `dict`, `list`, `set`, `tuple` — not the deprecated `Dict`, `List`, `Set`, `Tuple` from `typing`. While fixing types, also migrate any existing `typing.Dict` etc. to builtins.

### 4. Replace lxml `.xpath()` calls with typed `h.xpath_*` helpers

The raw lxml `.xpath()` returns `Any`. Use the zavod helpers instead:

```python
# Before — returns Any
links = doc.xpath(".//a/@href")
elements = doc.xpath('.//div[@class="item"]')
text = doc.xpath(".//h1/text()")[0]

# After — properly typed
links = h.xpath_strings(doc, ".//a/@href")
elements = h.xpath_elements(doc, './/div[@class="item"]')
text = h.xpath_string(doc, ".//h1/text()")
```

When iterating over elements only to extract an attribute (e.g. `.get("href")`), move the attribute into the xpath and use `h.xpath_strings` instead:
```python
# Before
for anchor in doc.xpath('//a[contains(@class, "name")]'):
    url = anchor.get("href")
    crawl_page(context, url)

# Also before (already migrated to xpath_elements but still using .get)
for anchor in h.xpath_elements(doc, '//a[contains(@class, "name")]'):
    url = anchor.get("href")
    crawl_page(context, url)

# After
for url in h.xpath_strings(doc, '//a[contains(@class, "name")]/@href'):
    crawl_page(context, url)
```

Use `h.xpath_element()` (singular) when you expect exactly one match:
```python
# Before
divs = doc.xpath(divs_xpath)
assert len(divs) == 1
content = divs[0]

# After
content = h.xpath_element(doc, divs_xpath)
```

### 5. Replace `.text_content()` with `h.element_text()`

`h.element_text()` calls `text_content()` internally and applies `collapse_spaces` + `strip`. If the original code was calling `squash_spaces` or `collapse_spaces` on the result, that's now redundant and should be removed. If the extracted text is used for lookups (check the `lookups:` section in the crawler's `.yml` file) or exact comparisons, pass `squash=False` to preserve the original whitespace.

```python
# Before
name_info = summary.text.strip()
body = body_els[0].text_content().strip()
category = squash_spaces(row.pop("category").text_content())

# After
name_info = h.element_text(summary)
body = h.element_text(body_els[0])
category = h.element_text(row.pop("category"))

# When exact text matters (used in lookups or comparisons):
label = h.element_text(el, squash=False)
```

### 6. Use `from zavod.util import Element, ElementOrTree` for lxml type annotations

Don't use `lxml.etree._Element` (private API) or `xml.etree.ElementTree`. Use the re-exported types:

```python
# Before
from lxml import etree
def parse_record(context: Context, el: etree._Element):

# After
from zavod.util import Element
def parse_record(context: Context, el: Element) -> None:
```

### 7. Import zyte_api as a module, not individual functions

```python
# Before
from zavod.extract.zyte_api import fetch_html, fetch_json, fetch_resource

# After
from zavod.extract import zyte_api
# then: zyte_api.fetch_html(...), zyte_api.fetch_json(...), zyte_api.fetch_resource(...)
```

### 11. Use `Iterator` instead of `Generator` when only yielding

```python
# Before
from typing import Generator
def parse_csv(context: Context, path: str) -> Generator[Item, None, None]:

# After
from typing import Iterator
def parse_csv(context: Context, path: str) -> Iterator[Item]:
```

### 12. Add return types to small helper functions

```python
# Before
def clean_address(text):
def extract_passport_no(text):

# After
def clean_address(text: str | None) -> list[str] | None:
def extract_passport_no(text: str | None) -> list[str] | None:
```

### 13. Use keyword-only arguments for complex function signatures

When a function has many parameters, add `*` to force keyword arguments — this catches argument-order bugs at the call site:

```python
# Before
def emit_linked_org(context, vessel_id, names, role, date):
    ...
emit_linked_org(context, vessel.id, related_ros, "Related Recognised Organization", start_date)

# After
def emit_linked_org(context: Context, *, vessel_id: str | None, names: str, role: str, date: str | None) -> None:
    ...
emit_linked_org(context, vessel_id=vessel.id, names=related_ros, role="Related Recognised Organization", date=start_date)
```

### Add a comment to functions that return tuples

If the function returns a tuple, add a docstring comment to briefly
describe the contents of the tuple. This is not a typechecker fix, but it helps readability since tuples don't have named fields.

## General principles

- **Never change logic.** These are type-annotation-only fixes. Do not change control flow, data transformations, or output. Each fix must be resolved by exactly and narrowly applying one of the rules above. If a type error cannot be resolved that way without altering behavior, leave the error. It is fine to leave some errors unfixed rather than risk changing what the crawler emits.
- Prefer the narrowest correct type. `dict[str, str]` is better than `dict[str, Any]` when all values are strings.
- Use `str | None` union syntax, not `Optional[str]`, and lowercase `dict`/`list`/`set` not `Dict`/`List`/`Set` — but only when you're already editing the line for another reason. Do not make cosmetic-only changes to lines that have no type errors.
- The `context: Context` parameter should always be first in crawler functions.
