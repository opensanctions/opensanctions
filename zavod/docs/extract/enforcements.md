# Developing Enforcements crawlers


## Limit enforcements to the enforcement age limit

Enforcements pages often go back into decades of historical notices whose relevance
is questionable while potentially adding a big maintenance burden (in crawler code
and review effort).

We have a standard support period defined for enforcement actions and a helper to
check whether an action date is within scope:

```python
    for row in h.parse_html_table(table):
        enforcement_date = h.element_text(row["date"])
        if not enforcements.within_max_age(context, enforcement_date):
            return
```

### ::: zavod.shed.enforcements.within_max_age


## Create Article and Documentation entities

Create an Article for each notice or press release, and a Documentation for each
distinct entity emitted based on that article.

The point of the article is not normally to emit the content of the article, but
rather to easily find all the significant entities mentioned in the same document.

### ::: zavod.helpers.articles.make_article

### ::: zavod.helpers.articles.make_documentation

