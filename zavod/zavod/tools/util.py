from collections.abc import Generator, Iterable
from followthemoney import Statement
from nomenklatura.resolver import Linker

from zavod.entity import Entity
from zavod.runtime.manifest import Manifest


def iter_output_statements(
    dataset_name: str,
    manifest: Manifest,
    linker: Linker[Entity],
    external: bool = True,
) -> Generator[Statement, None, None]:
    """Return all the statements in the given dataset that are ready for
    export. That means they have a valid ID and their canonical ID has been
    resolved.

    Statement IDs are not guaranteed to be unique in this stream; wrap it in
    `unique_statements` when writing to a sink that cannot reject duplicates.

    Args:
        dataset_name: The pinned dataset to load from the archive.
        manifest: The manifest pinning the dataset version to read.
        external: Include statements that are enrichment candidates.

    Returns:
        A generator of statements.
    """
    for stmt in manifest.statements(external=external, dataset=dataset_name):
        if stmt.entity_id is None:
            continue

        stmt = linker.apply_statement(stmt)
        if stmt.id is None:
            continue

        yield stmt


def unique_statements(
    statements: Iterable[Statement],
) -> Generator[Statement, None, None]:
    """Drop statements whose ID has already been seen in the given stream.

    Canonicalisation can collapse two source statements into one when both point
    at entities that have since been merged, so a sink which cannot reject
    duplicates itself - a file, mainly - needs this. Buffers every ID it has
    seen, some 125 bytes per statement, so prefer a database-side conflict
    clause where one is available.

    Args:
        statements: The statements to deduplicate.

    Returns:
        A generator of statements with distinct IDs.
    """
    seen_ids: set[str] = set()
    for stmt in statements:
        if stmt.id is None or stmt.id in seen_ids:
            continue

        yield stmt
        seen_ids.add(stmt.id)
