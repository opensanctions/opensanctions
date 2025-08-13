from typing import Optional, Tuple

from zavod.context import Context
from zavod.entity import Entity
from zavod import helpers as h


def make_related_article(
    context: Context,
    entity: Entity,
    url: str,
    article_key_extra: Optional[str] = None,
    documentation_key_extra: Optional[str] = None,
    title: Optional[str] = None,
    published_at: Optional[str] = None,
) -> Tuple[Entity, Entity]:
    """Create an article entity and a documentation entity to link it to a related entity.

    This is useful to link a number of entities mentioned in the same article, whether
    they are otherwise related or not.
    """

    article = context.make("Article")
    article.id = context.make_id(url, article_key_extra)
    article.add("sourceUrl", url)
    article.add("title", title)
    h.apply_date(article, "publishedAt", published_at)

    documentation = context.make("Documentation")
    assert entity.id is not None
    documentation.id = context.make_id(url, entity.id, documentation_key_extra)
    documentation.add("entity", entity)
    documentation.add("document", article)

    return article, documentation
