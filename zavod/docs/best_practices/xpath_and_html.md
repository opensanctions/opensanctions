# XPath and HTML

## Selector Design
Try to make the selectors specific enough to reliably select the correct content (to avoid e.g. the wrong thing being stuffed in a person's `name` property) while making them as semantic, concise, and general as possible to be maintainable and resillient against irrelevant changes in the HTML.

```python
# Good: 
table = doc.xpath('.//div[@id="block-content"]//table')

# Avoid:
table = doc.xpath('.//div[@id="block-content"]//div[3]//table')
```
Prefer `.//div[contains(@class, 'abc')]` over `.//div[@class='abc']` because classes can contain multiple values, but beware of conflicting class names like `abc-footer`.

## Fail loudly if the selection is different from what was expected

Beware of selections being different from what you intend. Common issues are:

* missing a bunch of entities because we're looping over a selection which turned out to be empty;
* emitting garbage because content was added to the page which matched unintentionally;
* cryptic errors because the wrong table is selected.

    It's ideal for an error to occur as close as possible to the offending code.

One way to guard against these issues is assertions on the number of matches:

```python
items = doc.xpath(".//h2[@text()='The Section']/following-sibling::ul/li)
assert len(items) > 0, items
```

```python
table = doc.xpath(".//table")
assert len(table) == 1, table
```

## Get notified of changes to content  

Use `h.assert_*_hash` to get a warning when the content has changed from when you last hardcoded the hash. We usually use this when we want to be notified of changes to the page - either because we manually extracted the data and need to update it manually, or if we want to be notified when new sections are added to the page.

`assert_dom_hash` is useful for focusing on a specific block of content, excluding more frequently changing parts of the site like headers, footers, etc.

```python
# Use a hash to ensure the element’s content is correct and hasn't changed unexpectedly.
h.assert_dom_hash(table[0], "26de80467eb7b4a93c0ad5c5c5b8cd75b07a38e0")
```


