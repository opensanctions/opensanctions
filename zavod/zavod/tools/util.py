from typing import Generator, Set
from followthemoney import Statement
from nomenklatura.resolver import Linker

from zavod.meta import Dataset
from zavod.entity import Entity
from zavod.archive import iter_dataset_statements


def iter_output_statements(
    scope: Dataset, linker: Linker[Entity], external: bool = True
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
    seen_ids: Set[str] = set()
    for stmt in iter_dataset_statements(scope, external=external):
        if stmt.id is None or stmt.id in seen_ids:
            continue
        if stmt.entity_id is None:
            continue

        stmt.canonical_id = linker.get_canonical(stmt.entity_id)
        yield stmt
        seen_ids.add(stmt.id)
