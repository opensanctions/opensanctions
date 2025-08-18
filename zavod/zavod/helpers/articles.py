from typing import Optional

from zavod.context import Context
from zavod.entity import Entity
from zavod import helpers as h


def make_article(
    context: Context,
    url: str,
    key_extra: Optional[str] = None,
    title: Optional[str] = None,
    published_at: Optional[str] = None,
) -> Entity:
    """
    Create an article entity based on the URL where it was published.

    Args:
        context: The runner context with dataset metadata.
        url: The URL where the article was published.
        key_extra: An optional value to be included in the generated Article ID hash.
        title: The title the article.
        published_at: The publication date of the article.
    """

    article = context.make("Article")
    article.id = context.make_id("Article", url, key_extra)
    article.add("sourceUrl", url)
    article.add("title", title)
    h.apply_date(article, "publishedAt", published_at)

    return article


def make_documentation(
    context: Context,
    entity: Entity,
    article: Entity,
    key_extra: Optional[str] = None,
    date: Optional[str] = None,
) -> Entity:
    """
    Creates a documentation entity to link an article to a related entity.
    The article's publishedAt date is added to the Documentation date property
    unless the date argument is provided.

    This is useful to link one or more entities to an article they were mentioned in.

    Create a distinct Documentation entity for each entity-article pair.

    Args:
        context: The runner context with dataset metadata.
        entity: The entity related to the article.
        article: The related article.
        key_extra: An optional value to be included in the generated Documentation ID hash.
        date: The publication date of the article, added to the Documentation date property.
    """

    documentation = context.make("Documentation")
    assert entity.id is not None
    assert article.id is not None
    documentation.id = context.make_id(
        "Documentation", entity.id, article.id, key_extra
    )
    documentation.add("entity", entity)
    documentation.add("document", article)

    if date:
        h.apply_date(documentation, "date", date)
    else:
        documentation.set("date", article.get("publishedAt"))
    return documentation
