# Stripping name prefixes and suffixes

Remove the honorifics and post-nominals a source attaches to names (`Hon. Jane Doe, MP`, `Dr. John Smith`) with per-dataset configuration.

Stripping titles is a nice-to-have, not a requirement. A title that is part of how an official website writes a person's name barely affects matching, because screening works around it. So strip the short list of titles that actually recur with [`h.strip_name_titles`][zavod.helpers.strip_name_titles], accept that a rare one slips through, and don't grow a long table chasing every degree or rank. Cleaning titles alone is not a reason to bring in the [name review system](../extract/names.md) or LLM cleaning; those are for [structural problems](#when-to-use-the-review-system-instead).

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

A bare word affix only matches at a **word boundary**. A prefix such as `Hon` must be followed by whitespace, so it does not truncate `Honorata`. An affix that ends in punctuation, such as `Hon.`, delimits itself. Thai, Lao, Khmer and Myanmar do not separate words with spaces, so affixes in these scripts match without whitespace: `นาย` strips from `นายสมชาย ใจดี`.

## Preserve the original value

When the cleaned name differs from the source string, keep the original on the statement with `original_value` so the source form stays traceable:

```python
original_name = raw_name if clean_name != raw_name else None
person.add("name", clean_name, lang="eng", original_value=original_name)
```

Set `original_value` only when the strings differ. Passing the raw value when nothing was stripped records a redundant provenance entry.

## When to use the review system instead

Use [the name review system](../extract/names.md) when a name string has a structural problem that would break matching:

- **Several people or names in one string**, such as `John Smith; Jonny Smith` or a name plus its former or maiden name.
- **Categorization** into `alias`, `weakAlias`, or `previousName`.
- **Embedded notes**, such as a death date or by-election remark in brackets, and transliteration variants like `Aleksandr(Oleksandr) KALYUSSKY`.

The two compose: strip the known titles first, and [`h.is_name_irregular`][zavod.helpers.is_name_irregular] and the `apply_reviewed_*` helpers apply to whatever remains.
