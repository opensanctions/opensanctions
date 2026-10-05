Change detectors identify source revisions that require review when a crawler cannot
interpret every relevant change deterministically.

A detector does not need to extract the changed data itself. It can watch a stable sentinel
such as a document URL, publication identifier, version date, or content hash and ask for
review when it encounters one that has not been acknowledged.

Use change detection when a crawler depends on content that cannot be exhaustively checked
by deterministic parsing: a hand-maintained extraction from a PDF, a page whose prose
determines crawler configuration, or a document whose replacement needs review even when
its URL is stable.

## Separate discovery from extraction

If a crawler can discover revisions reliably but cannot interpret them reliably in code,
keep the reviewed extraction in a repository-owned static file and poll the source for
stable sentinels that identify revisions. See [curating a dataset in the
repository](curated_datasets.md) for the conventions that file follows, how discovery
produces a review queue, and how removals are detected.

This pattern is a good fit when discovery is deterministic, a stable sentinel exists, and
the curated extraction is small enough to review in Git. If the source publishes structured
data that can be interpreted directly, parse it instead and use structural guards to detect
unexpected values or shapes.

A hash is one kind of acknowledgment marker, not a correctness check. It proves that the
bytes or text are the version a maintainer reviewed; it does not prove that the crawler
interpreted them correctly.

## Choose the smallest stable input

Monitor only the content whose change requires action. Headers, footers, timestamps,
advertising, and generated markup make whole-page hashes noisy and train maintainers to
ignore warnings.

- [`h.assert_dom_hash`][zavod.helpers.assert_dom_hash] monitors a parsed HTML/XML node.
  If markup changes are irrelevant, prefer `text_only=True`.
- [`h.assert_html_url_hash`][zavod.helpers.assert_html_url_hash] fetches an HTML page and
  monitors the whole document or a selected node.
- [`h.assert_url_hash`][zavod.helpers.assert_url_hash] hashes the raw response body at a
  URL. Use it for a stable document URL whose bytes are expected to change only when a
  new source version is published.
- [`h.assert_file_hash`][zavod.helpers.assert_file_hash] hashes an already-downloaded
  file. If the crawler must fetch or unpack the file before choosing what to monitor, use
  this helper.

The helpers return `False` and log a warning when the hash differs. If continuing would
emit wrong or ambiguous data, set `raise_exc=True`.

```python
expected = "30aca6ba4b245649db4bee16e0798d661080bd9a"
if not h.assert_dom_hash(article, expected, text_only=True):
    context.log.warning(
        "Page hash changed: update the hand-maintained program mapping "
        "before accepting the new hash."
    )
```

If resolving the warning requires more than inspecting the diff, put a short runbook next
to the assertion. Name the repository-owned static file, crawler configuration, or external
system that must be updated. Record known facts and actions, not guesses about how the
publisher might behave.

## Respond to a changed hash

Inspect the changed source before editing the expected hash. If deterministic parsing is
impractical, you can extract changed information from unstructured pages and documents,
including PDFs, through human or model inference.

Classify the change before acting:

- **The source data changed.** Incorporate the revision into the crawler, its
  repository-owned static data, or the documented external data store. Update the expected
  hash in the same maintenance change, then verify the resulting dataset.
- **Only irrelevant presentation or metadata changed.** Confirm that the crawler output
  and any hand-maintained extraction remain correct. The new hash may then be accepted,
  but explain in the PR what changed and why it has no data impact. If the same kind of
  noise is likely to recur, narrow the monitored node or use `text_only=True` instead.
- **The impact is unresolved.** Leave the old hash in place. Do not suppress the warning
  or accept a source revision that has not been understood.

Never update a hash merely because the new value appears in the warning. A substantive
source revision and its dataset update belong together; a cosmetic revision needs an
explicit, evidence-based explanation.

Change detection complements [strict interpretation](strict_interpretation.md). If a
deterministic structural guard can express the invariant, prefer it: required dictionary
keys, typed XPath selectors with count expectations, categorical lookups, and dataset
assertions produce more useful failures than a broad content hash.
