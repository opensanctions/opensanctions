from typing import Optional
from requests.exceptions import RequestException
from zavod.context import Context
from nomenklatura.wikidata.client import WikidataClient

from rigour.ids.wikidata import is_qid


def deref_wikidata_id(
    context: Context, qid: Optional[str], cache_days: int = 60
) -> Optional[str]:
    """Check if a Wikidata QID is a redirect, and return the target QID if so.

    This is used with static data sources that reference Wikidata items that may have
    been merged or redirected.

    Args:
        context: The zavod context to use for fetching.
        qid: The Wikidata QID to dereference.
        cache_days: Number of days to cache the fetch result.

    Returns:
        The target QID if the input was a redirect, otherwise the original QID.
    """
    if qid is None or not is_qid(qid):
        return None

    try:
        params = {
            "format": "json",
            "ids": qid,
            "action": "wbgetentities",
            "props": "info",
        }
        res = context.fetch_json(
            WikidataClient.WD_API,
            params=params,
            cache_days=cache_days,
        )
        entity = res.get("entities", {}).get(qid, {})
        target = entity.get("redirected", {}).get("to")
        print("DEREF", qid, "->", target)
        if target is not None:
            return target
    except RequestException as exc:
        context.log.warning(f"Failed to dereference Wikidata ID {qid}: {exc}")
    return qid
