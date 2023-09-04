from typing import Generator, Set
from nomenklatura.statement import Statement

from zavod.meta import Dataset
from zavod.dedupe import get_dataset_resolver
from zavod.archive import iter_dataset_statements


def iter_output_statements(
    scope: Dataset, external: bool = True
) -> Generator[Statement, None, None]:
    """Return all the statements in the given dataset that are ready for
    export. That means they are unique, have a valid ID, and their
    canonical ID has been resolved.

    Args:
        dataset: The dataset to load from the archive.
        external: Include statements that are enrichment candidates.

    Returns:
        A generator of statements.
    """
    assert not scope.is_collection
    resolver = get_dataset_resolver(scope)
    seen_ids: Set[str] = set()
    for stmt in iter_dataset_statements(scope, external=external):
        if stmt.id is None or stmt.id in seen_ids:
            continue
        if stmt.entity_id is None:
            continue

        stmt.canonical_id = resolver.get_canonical(stmt.entity_id)
        yield stmt
        seen_ids.add(stmt.id)
