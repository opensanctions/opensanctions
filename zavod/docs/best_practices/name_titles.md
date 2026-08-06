# Stripping name prefixes and suffixes

Remove a known set of honorific prefixes and post-nominal suffixes from source names with per-dataset configuration, without a human review.

Many official sources publish names with titles attached: `Hon. Jane Doe, MP`, `Dr. John Smith`, `Sen. (Rtd) Amina Yusuf`. These titles are not part of the name and reduce match quality. When a source uses a fixed, predictable set of titles, strip them deterministically with [`h.strip_name_titles`][zavod.helpers.strip_name_titles] instead of routing the names through the [name review system](../extract/names.md).

## Prefer deterministic stripping when the affix set is known

If the honorifics and post-nominals a source uses are a closed, enumerable list (`Hon.`, `Dr.`, `Prof.`, `, MP`, `, CS`), declare them in the dataset metadata and call `h.strip_name_titles`. Do not send predictable titles through the review system: that adds LLM cost and human-review latency to a problem the crawler can solve on its own, and every new member waits on a review before their name is clean.

Reach for the name review system only when the name is genuinely ambiguous: multiple names packed into one string, alias or previous-name categorization, or transliteration variants. Those cases are covered under [when to use the review system instead](#when-to-use-the-review-system-instead).

## The pattern

Declare the titles under a `names` block in the dataset YAML, listing prefixes and suffixes separately:

```yaml
names:
  prefixes_strip:
    - Hon.
    - "Hon "
    - Dr.
    - (Dr.)
    - Prof.
    - Sen.
    - (Rtd)
  suffixes_strip:
    - ", MP"
    - ", CS"
```

In the crawler, pass each raw name through `h.strip_name_titles`. It reads the configured affixes from the dataset metadata, so the call takes only the context and the name:

```python
clean_name = h.strip_name_titles(context, raw_name)
original_name = raw_name if clean_name != raw_name else None
person.add("name", clean_name, lang="eng", original_value=original_name)
```

`datasets/ke/national_assembly/crawler.py` and `datasets/ke/senate/crawler.py` follow this idiom directly. `datasets/ug/parliament/crawler.py` shows the variant that feeds the cleaned string into [`h.apply_name`][zavod.helpers.apply_name]:

```python
clean_name = h.strip_name_titles(context, name)
h.apply_name(person, full=clean_name, lang="eng")
```

## Configure the exact strings the source uses

`strip_name_titles` matches configured affixes as exact strings, case-insensitively. It does no punctuation folding, so every surface form the source uses is its own list entry:

- **Punctuation variants are separate.** `Dr.` does not match `(Dr.)`. List both if the source uses both.
- **Trailing-space variants are separate.** A prefix that appears both as `Hon.` and as `Hon ` (no dot, space before the name) needs both `Hon.` and `"Hon "`. Quote entries with leading or trailing spaces so YAML preserves them.
- **Suffix entries include the separator.** A post-nominal published as `Jane Doe, MP` is configured as `", MP"`, with the comma and space, because that whole run is what gets removed.

Affixes are tried **longest first**, so a longer form is stripped before any shorter string it contains. This is why `(Prof.)` must be listed explicitly: relying on `Prof.` alone would strip the letters and leave a stray `)`.

Stripping is **iterative**: the helper peels one matching affix per pass and repeats until nothing matches. Stacked titles are handled without extra configuration:

```text
"Hon. Dr. Jane Doe, MP"  ->  "Jane Doe"
```

## Preserve the original value

When the cleaned name differs from the source string, keep the original on the statement with `original_value` so the source form stays traceable:

```python
original_name = raw_name if clean_name != raw_name else None
person.add("name", clean_name, lang="eng", original_value=original_name)
```

Set `original_value` only when the strings differ. Passing the raw value when nothing was stripped records a redundant provenance entry.

## When to use the review system instead

Deterministic stripping handles a fixed affix list and nothing else. Use [the name review system](../extract/names.md) when a name needs judgment rather than a lookup:

- **Multiple names in one string**, such as `John Smith; Jonny Smith` or a name plus its former name.
- **Categorization** into `alias`, `weakAlias`, or `previousName`.
- **Splitting** or handling transliteration variants like `Aleksandr(Oleksandr) KALYUSSKY`.
- **Open-ended dirt** that isn't a predictable, enumerable set of titles.

The two approaches compose. Strip the known titles deterministically, and the irregularity checks in [`h.is_name_irregular`][zavod.helpers.is_name_irregular] and the `apply_reviewed_*` helpers still apply to whatever remains.
