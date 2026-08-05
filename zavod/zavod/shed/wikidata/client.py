from nomenklatura.wikidata import WikidataClient

from zavod import Context, settings

WIKIDATA_QUERY_CACHE = 10
WIKIDATA_ITEM_CACHE = 60


def create_wikidata_client(context: Context) -> WikidataClient:
    """Create a Wikidata client with the current context's cache and logging."""
    return WikidataClient(
        context.cache,
        session=context.http,
        cache_days=WIKIDATA_ITEM_CACHE,
        reference_time=settings.RUN_TIME,
    )
