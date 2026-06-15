# XPath and HTML

XPath selectors are a common source of brittle, hard-to-debug crawlers; the helpers in `zavod.helpers.html` add typed return values and explicit count guards on top of `lxml`.

## Use the typed helpers in `zavod.helpers.html`

`lxml`'s `Element.xpath()` has a loose return type. The same call can return a list of elements, a list of strings, a single string, a number, or a boolean, depending on the expression. The type stubs reflect this by typing the result as `Any`. A typo in the expression or an upstream HTML change can therefore produce a result of the wrong type, with the failure surfacing far away from the offending line.

The helpers in `zavod.helpers.html` wrap `.xpath()` with concrete return types and runtime checks. Prefer them over calling `.xpath()` directly:

| Helper | Returns | Use for |
| --- | --- | --- |
| [`h.xpath_elements`][zavod.helpers.xpath_elements] | `list[Element]` | Any element-returning expression. |
| [`h.xpath_element`][zavod.helpers.xpath_element] | `Element` | Exactly one element; raises on zero or multiple matches. |
| [`h.xpath_strings`][zavod.helpers.xpath_strings] | `list[str]` | Text-returning expressions (`.../text()`, `string(...)`). |
| [`h.xpath_string`][zavod.helpers.xpath_string] | `str` | Exactly one string result. |
| [`h.element_text`][zavod.helpers.element_text] | `str` | Concatenated text content, whitespace-squashed by default. |

```python
from zavod import helpers as h

# Typed result; raises if the table is not unique.
table = h.xpath_element(doc, './/div[@id="block-content"]//table')

# Typed list of rows with a count guard.
rows = h.xpath_elements(table, "./tbody/tr", expect_exactly=12)

# Clean string extraction with whitespace squashing.
title = h.element_text(h.xpath_element(doc, ".//h1"))
```

For tabular content, use [`h.parse_html_table`][zavod.helpers.parse_html_table] rather than walking rows by hand. It yields one `dict[str, Element]` per row, keyed by slugified header text; pair it with [`h.cells_to_str`][zavod.helpers.cells_to_str] when string values are enough:

```python
for row in h.parse_html_table(table):
    cells = h.cells_to_str(row)
    name = cells["full_name"]
    # ... or reach into row["full_name"] when the cell contains links or markup
```

[`h.links_to_dict`][zavod.helpers.links_to_dict] is occasionally useful when a cell contains labelled anchor elements that should become a `{label: href}` mapping.

`lxml`'s own `.findall()` is typed and a fine choice for trivial relative selectors (`row.findall("./td")`, `el.findall(".//a")`); reach for the `xpath_*` helpers when the selector actually needs XPath predicates, axes, or `contains(...)`.

## Write specific but semantic selectors

Selectors need to be specific enough to pick out the intended content, but semantic and concise enough to survive irrelevant HTML changes. A selector that is too loose can put the wrong text into a person's `name` property. A selector pinned to layout details breaks on the next restyle.

```python
# Good:
table = h.xpath_element(doc, './/div[@id="block-content"]//table')

# Avoid:
table = h.xpath_element(doc, './/div[@id="block-content"]//div[3]//table')
```

Prefer `.//div[contains(@class, 'abc')]` over `.//div[@class='abc']` because the `class` attribute can hold multiple whitespace-separated values. Watch for conflicting class names like `abc-footer`, which also match `contains(@class, 'abc')`.

## Fail loudly when a selection does not match expectations

Selections often turn out to be different from what was intended. Common symptoms:

- the loop runs over an empty selection and the crawler silently produces no entities;
- content added to the page matches the expression unintentionally and ends up in entity properties;
- the wrong table is selected, and the failure surfaces several function calls later.

Errors should surface as close to the offending code as possible. The typed helpers above enforce shape (list vs. single, element vs. string). For count expectations, pass `expect_exactly` or assert on the returned list:

```python
# Exactly one match expected; raises on zero or many.
table = h.xpath_element(doc, ".//table")

# Known fixed count.
rows = h.xpath_elements(table, "./tbody/tr", expect_exactly=12)

# Open-ended but must not be empty.
items = h.xpath_elements(doc, ".//h2[text()='The Section']/following-sibling::ul/li")
assert len(items) > 0, items
```

## Use content hashes to flag silent changes

To get a warning when page content changes from a previously hardcoded hash, use [`h.assert_dom_hash`][zavod.helpers.assert_dom_hash]. This catches drift that would otherwise need to be noticed manually: data extracted by hand, or a new section that should trigger a review of the parser. Scope the hash to a specific block of content, not the whole document, so frequently changing parts of the site (headers, footers, ads) do not trigger spurious warnings.

By default, `h.assert_dom_hash` logs a warning and returns `False` when the hash does not match, allowing the crawler to continue. Use the return value to log a maintainer-facing message that explains what to recheck before updating the hash:

```python
expected = "30aca6ba4b245649db4bee16e0798d661080bd9a"
if not h.assert_dom_hash(article, expected, text_only=True):
    context.log.warning(
        "Page hash changed: confirm the referenced lists are still the "
        "UNSC Taliban and Al-Qaida lists before updating the hash."
    )
```

Pass `text_only=True` to ignore markup churn (class renames, added wrappers) and hash only the visible text. Pass `raise_exc=True` to turn the check into a hard failure when continuing past a change is unacceptable. For checking a whole page by URL rather than a sub-tree, use [`h.assert_html_url_hash`][zavod.helpers.assert_html_url_hash].
