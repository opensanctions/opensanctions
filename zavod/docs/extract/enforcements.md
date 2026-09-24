# Developing article-based crawlers

Some sources publish a stream of documents rather than a register: enforcement notices,
press releases, debarment announcements, disciplinary findings. The entities we care
about are named in prose inside those documents, and one document may name none, one, or
a dozen of them. The unit of crawl is the **article**, not the row. The crawler walks a
listing page, follows it to each article, and reads the entities out of the article's
text.

The general rules still apply. Structured parts of the source — the index table, a CSV of
actions — are parsed destructively and audited, per
[strict interpretation](../best_practices/strict_interpretation.md). The article body is
free-form HTML, where strictness lives in the selectors.


## Limit crawls to the enforcement age limit

Enforcements pages often go back into decades of historical notices whose relevance
is questionable while potentially adding a big maintenance burden (in crawler code
and review effort).

We have a standard support period defined for enforcement actions and a helper to
check whether an action date is within scope:

```python
    for row in h.parse_html_table(table):
        enforcement_date = h.element_text(row["date"])
        if not h.within_max_age(context, enforcement_date):
            return
```

### ::: zavod.helpers.dates.within_max_age


### Stop the loop, or skip the row?

Getting this wrong is the most common defect in an article crawler, and it is silent
either way: stopping too early truncates the dataset, while never stopping walks the
source's whole archive on every run. The answer depends on the order of the index, which
you establish by reading it rather than assuming:

- **The index is newest-first.** The first out-of-age item means every later one is older
  too, so **stop paginating**. Have the per-page function return `False` on that item, and
  leave the loop to `crawl()`.

- **The index is in any other order** — oldest-first, grouped by programme, sorted by
  name — then an out-of-age item says nothing about the next one. **Skip the item and
  carry on**; never break out of the loop over an out-of-age item. Only pagination
  running out ends the crawl.

- **The index carries no date**, only the article does. The age check then moves into the
  per-article function, after the fetch, and pagination has no signal to stop on.

The two tests look identical in code, so note which case applies in a comment, and name
the evidence: the index's sort column, or the order you observed.

### Overriding the default period

`MAX_ENFORCEMENT_DAYS` (five years) is the default and most sources should keep it.
Override it only for a reason that is a property of the source, and write that reason
next to the constant:

```python
# Notices issued before 5 July 2016 are PDFs, which this crawler doesn't parse.
MAX_AGE_DAYS = (date.today() - date(2016, 7, 5)).days
```

A longer window because the data "seems useful" is not such a reason: it buys history at
the cost of review effort.

### Dates must parse before they can be compared

`within_max_age` parses with `fallback_to_original=False`, so an unparseable date raises
`ValueError` rather than quietly passing the filter. Declare every format the source uses
in [`dates.formats`](../best_practices/dates_meta.md), and let the exception stand: an
index whose date column stopped parsing is a structural change.

Where a source genuinely publishes undated notices, catch it at the point you read the
date, warn, and skip the notice — an article with no date has no `publishedAt` and cannot
be age-filtered, so it isn't publishable either way.


## Create Article and Documentation entities

Create an Article for each notice or press release, and a Documentation for each
distinct entity emitted based on that article.

The point of the article is not normally to emit the content of the article, but
rather to easily find all the significant entities mentioned in the same document.

### ::: zavod.helpers.articles.make_article

### ::: zavod.helpers.articles.make_documentation


### One Article per notice, one Documentation per entity

Build the Article **once per notice**, outside the loop over the entities it names, and
give each entity its own Documentation inside it:

```python
article = h.make_article(context, url, title=title, published_at=published_at)
context.emit(article)

for item in defendants:
    entity = context.make(item.entity_schema)
    entity.id = context.make_id(item.name, *item.country)
    entity.add("name", item.name)
    context.emit(entity)
    context.emit(h.make_documentation(context, entity, article))
```

`make_article` is keyed on the URL, so building it inside the loop still produces the
same entity ID. The cost is that it re-emits one entity per defendant and obscures the
one-article-many-entities structure the Documentation entities exist to express.

Give every `Thing` the notice causes you to emit a Documentation, not only the principal
subject: a defendant's related company, a vessel named alongside its owner. The entity
was read out of that document, and the Documentation records where it came from.

`Documentation:entity` has range `Thing`, so it covers `Person`, `Company`,
`LegalEntity`, `Vessel` and their kin. `Sanction` (an `Interval`) and relationship edges
such as `UnknownLink` and `Ownership` are not `Thing`s and take no Documentation — they
are already anchored to entities that have one. Put the notice URL on
`Sanction:sourceUrl` instead.

`key_extra` is there for when the URL alone doesn't identify what you're making an entity
for, e.g. one page carrying several distinct notices.


## Reading entities out of prose

An index table gives you cells; an article gives you sentences, and "the defendant" may
be a person, a company, or three of each in a list. Take the first rung that holds:

1. **Deterministic parsing**, when the prose is formulaic — a heading that is always
   `In the Matter of <name>`, a parenthetical `(born 12 March 1970)`. Assert the shape
   you rely on, so a change in the source fails loudly instead of passing silently.
2. **A lookup**, for a bounded set of values that don't parse. Two distinct uses:
    - a **categorical** lookup for interpretation-bearing values — notice type, entity
      type, whether the article is in scope at all. Enumerate every known value and make
      a miss loud (`warn_unmatched=True`, or `required: true`); never fall through to a
      default. Mapping a value to `null` is how an article is filtered out of scope, and
      that option needs a comment saying so.
    - a **`type.*`** lookup for values that are well-formed but wrong: a misspelled month
      in a published date, a country string the source invented.
3. **The review framework**, when the names and entity types can only be had by reading
   the text (see [data reviews](../data_reviews.md) for the full mechanism). This is the
   normal answer for enforcement prose. Do not reach for regular expressions to split a
   list of defendants out of a sentence: the cases that matter are the ones a regex gets
   wrong — `A, B and C, d/b/a D`, an alias in parentheses, a company whose name contains
   ` and `.

Rungs 2 and 3 combine. A crawler can run a cheap heuristic over every name and route only
the irregular ones to review: [`h.is_name_irregular`][zavod.helpers.is_name_irregular]
and `rigour.names.contains_split_phrase` both flag whether a name needs a human to look
at it. Names have their own helpers,
[`h.apply_reviewed_name_string`][zavod.helpers.apply_reviewed_name_string] and
[`h.apply_reviewed_names`][zavod.helpers.apply_reviewed_names]; see
[name cleaning](names.md).

Record where each value came from: `origin=review.origin` for extracted values, an
explicit origin (`"hand-extracted"`) for values a maintainer wrote into a lookup by hand.
A crawler using LLM extraction sets `ci_test: false` (no API key in CI) and ends
`crawl()` with [`assert_all_accepted`][zavod.stateful.review.assert_all_accepted].


## When a notice doesn't fit the expected shape

Free-form HTML gives you no record to audit, so the decision — crash, warn and skip, or
add a lookup — is made per selector. What matters is **how many notices the deviation
affects**, not how bad any single one looks.

- **Crash** when the breakage is structural, meaning it would affect every notice on the
  site and the parse is invalid if the assumption is false. Select the article container,
  title and release ID with [`h.xpath_element`][zavod.helpers.xpath_element] or
  `expect_exactly=`, which raise on the wrong count, and assert what you rely on.

- **Warn and skip** when one notice deviates and its siblings still parse — a release
  published as a PDF, a stub page, a notice with no date. Use `context.log.warning` with
  the URL and `continue`, never an `assert` over this kind of deviation: an assertion
  here stops the crawl over one bad document and loses the hundreds that parsed.

- **Add a lookup** when the deviation is a known, enumerable value rather than a one-off
  failure: a notice type the source added, an entity type label, a topic that is out of
  scope, a date the source misspelled. The lookup entry is the documented decision, it
  lives in the YAML where a non-programmer can extend it, and the unmatched case stays
  loud for the next new value.

Never encode a single notice's exception as a condition in the crawler:

```python
# Anti-pattern: this is a lookup, written in the wrong file.
if url == "https://www.example.gov/press-releases/7274-15":
    return None
```

A URL test in code has no audit trail, no comment explaining what was wrong with that
notice, and no room for the second one.

An article that names no entities is not an error, either. Plenty of press releases are
about policy, personnel or statistics. Filter out-of-scope categories on the source's own
topic labels via a lookup, and let an unknown label warn, so a new category is a decision
someone makes rather than a silent inclusion.
