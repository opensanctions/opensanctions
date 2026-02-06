import os
from typing import Set, Tuple
from anthropic import Anthropic

from nomenklatura import Resolver
from zavod.logs import configure_logging, get_logger
from zavod.integration import get_resolver
from zavod.meta import get_catalog
from zavod.store import get_store, View

log = get_logger("team-of-analysts")
client = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
ignore: Set[Tuple[str, str]] = set()


def resolve_next(resolver: Resolver, view: View) -> None:
    for left_id, right_id, score in resolver.get_candidates():
        left_id = resolver.get_canonical(left_id)
        right_id = resolver.get_canonical(right_id)
        if (left_id, right_id) in ignore:
            continue
        if score is None:
            ignore.add((left_id, right_id))
            continue
        if not resolver.check_candidate(left_id, right_id):
            ignore.add((left_id, right_id))
            continue
        left = view.get_entity(left_id)
        right = view.get_entity(right_id)

        # prompt the team of analysts


def auto_resolve(scope: str) -> None:
    catalog = get_catalog()
    dataset = catalog.require(scope)
    resolver = get_resolver()
    store = get_store(dataset, resolver)
    resolver.get_candidates()
    while True:
        view = store.view(dataset, external=True)
        resolve_next(resolver, view)


if __name__ == "__main__":
    configure_logging()
    auto_resolve("debarment")
