# Change detection

Change detectors turn a source revision into an explicit maintenance signal. Use them
when a crawler depends on content that cannot be exhaustively checked by deterministic
parsing: a hand-maintained extraction from a PDF, a page whose prose determines crawler
configuration, or a document whose replacement needs review even when its URL is stable.

A hash is an acknowledgement marker, not a correctness check. It proves that the bytes
or text are the version a maintainer reviewed; it does not prove that the crawler
interpreted them correctly.

## Choose the smallest stable input

Monitor only the content whose change requires action. Headers, footers, timestamps,
advertising and generated markup make whole-page hashes noisy and train maintainers to
ignore warnings.

- [`h.assert_dom_hash`][zavod.helpers.assert_dom_hash] monitors a parsed HTML/XML node.
  Prefer `text_only=True` when markup changes are irrelevant.
- [`h.assert_html_url_hash`][zavod.helpers.assert_html_url_hash] fetches an HTML page and
  monitors the whole document or a selected node.
- [`h.assert_url_hash`][zavod.helpers.assert_url_hash] hashes the raw response body at a
  URL. Use it for a stable document URL whose bytes are expected to change only when a
  new source version is published.
- [`h.assert_file_hash`][zavod.helpers.assert_file_hash] hashes an already-downloaded
  file. Use it when the crawler must fetch or unpack the file before choosing what to
  monitor.

The helpers return `False` and log a warning when the hash differs. Set
`raise_exc=True` only when continuing would emit wrong or ambiguous data.

```python
expected = "30aca6ba4b245649db4bee16e0798d661080bd9a"
if not h.assert_dom_hash(article, expected, text_only=True):
    context.log.warning(
        "Page hash changed: update the hand-maintained programme mapping "
        "before accepting the new hash."
    )
```

Put a short runbook next to the assertion when resolving it requires more than inspecting
the diff. Name the repository-owned static file, crawler configuration or external system
that must be updated. Record known facts and actions, not guesses about how the publisher
might behave.

## Respond to a changed hash

Inspect the changed source before editing the expected hash. For unstructured pages and
documents, including PDFs, it is fine to extract the changed information through careful
human or model inference when deterministic parsing is impractical.

Classify the change before acting:

1. **The source data changed.** Incorporate the revision into the crawler, its
   repository-owned static data, or the documented external data store. Update the
   expected hash in the same maintenance change, then verify the resulting dataset.
2. **Only irrelevant presentation or metadata changed.** Confirm that the crawler output
   and any hand-maintained extraction remain correct. The new hash may then be accepted,
   but explain in the PR what changed and why it has no data impact. If the same kind of
   noise is likely to recur, narrow the monitored node or use `text_only=True` instead.
3. **The impact is unresolved.** Leave the old hash in place. Do not suppress the warning
   or accept a source revision that has not been understood.

Never update a hash merely because the new value appears in the warning. A substantive
source revision and its dataset update belong together; a cosmetic revision needs an
explicit, evidence-based explanation.

Change detection complements [strict interpretation](strict_interpretation.md). Prefer a
deterministic structural guard when one can express the invariant: required dictionary
keys, typed XPath selectors with count expectations, categorical lookups and dataset
assertions produce more useful failures than a broad content hash.
