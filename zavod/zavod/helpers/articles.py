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
    """Create an article based on a published URL."""

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
) -> Entity:
    """
    Create a documentation entity to link an article to a related entity.

    This is useful to link one or more entities to an article they were mentioned in.
    """

    documentation = context.make("Documentation")
    assert entity.id is not None
    assert article.id is not None
    documentation.id = context.make_id(
        "Documentation", entity.id, article.id, key_extra
    )
    documentation.add("entity", entity)
    documentation.add("document", article)
    return documentation
