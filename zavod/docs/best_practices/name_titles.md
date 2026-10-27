# Stripping name prefixes and suffixes

Remove a known set of honorific prefixes and post-nominal suffixes from source names with per-dataset configuration, without a human review.

Many official sources publish names with titles attached: `Hon. Jane Doe, MP`, `Dr. John Smith`, `Sen. (Rtd) Amina Yusuf`. These titles are not part of the name. Where a few of them recur, strip them with [`h.strip_name_titles`][zavod.helpers.strip_name_titles].

## The pattern

Declare the titles that recur in the source's names under a `names` block in the dataset YAML, listing prefixes and suffixes separately. The list need not be complete: a rare title left on a name is an acceptable loss.

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
person.add("name", clean_name, lang="eng", original_value=raw_name)
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
