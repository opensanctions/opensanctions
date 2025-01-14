# XPath and HTML

## Selector Design
Use semantic selectors that are resilient to HTML changes:

```python
# Good: 
table = doc.xpath('.//div[@id="block-content"]//table')

# Avoid:
table = doc.xpath('.//div[@id="block-content"]//div[3]//table')
```

## Assert the XPath Result
Ensure that the XPath query returns exactly one element, especially when selecting from multiple possible matches (e.g., tables on a page). If more than one element is selected and we access `selection[0]`, we’re assuming the first one in the DOM order is the one we want, which may not be reliable with vague selectors. Always assert the correct number of elements to avoid errors.

```python
table = doc.xpath(".//table")
assert len(table) == 1, table
```

## Assert DOM Hash
Use `h.assert_dom_hash` to ensure the integrity of a specific DOM element (e.g., a table). This is helpful when you need to track significant changes without being alerted for every minor update. It notifies you of structural changes or new sections.

```python
# Use a hash to ensure the element’s content is correct and hasn't changed unexpectedly.
h.assert_dom_hash(table[0], "26de80467eb7b4a93c0ad5c5c5b8cd75b07a38e0")
```

The reason we use this is usually when we want to be notified of changes to the page - either because we manually extracted the data and need to update it manually, or if we want to be notified when new sections are added to the page.