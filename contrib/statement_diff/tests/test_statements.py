import pandas as pd
from followthemoney import Statement

from statement_diff import DiffResult, _DF_TEXT_COLS, compute_diff


def _stmt(
    entity_id: str,
    prop: str,
    value: str,
    schema: str = "Thing",
    dataset: str = "test",
) -> Statement:
    return Statement(
        entity_id=entity_id,
        prop=prop,
        schema=schema,
        value=value,
        dataset=dataset,
    )


def _df(*stmts: Statement) -> pd.DataFrame:
    """Build a statement DataFrame (indexed by stmt_id) from Statement objects."""
    records = [
        (
            stmt.id,
            stmt.entity_id,
            stmt.schema,
            stmt.prop,
            stmt.value,
            stmt.dataset,
            stmt.first_seen or "",
            stmt.last_seen or "",
            stmt.lang or "",
            stmt.original_value or "",
            stmt.origin or "",
            stmt.external,
        )
        for stmt in stmts
        if stmt.id is not None
    ]
    df = pd.DataFrame(records, columns=["stmt_id", *_DF_TEXT_COLS, "external"])
    return df.set_index("stmt_id")


_EMPTY = _df()


# ---------------------------------------------------------------------------
# Basic counts
# ---------------------------------------------------------------------------


def test_both_empty() -> None:
    result = compute_diff(_EMPTY, _EMPTY)
    assert isinstance(result, DiffResult)
    assert result.rows == []
    assert result.removed_count == 0
    assert result.added_count == 0
    assert result.unchanged_count == 0


def test_all_removed() -> None:
    a = _stmt("e1", "name", "Alice")
    b = _stmt("e1", "name", "Bob")
    result = compute_diff(_df(a, b), _EMPTY)
    assert result.removed_count == 2
    assert result.added_count == 0
    assert result.unchanged_count == 0
    assert all(marker == "-" for marker, _ in result.rows)


def test_all_added() -> None:
    a = _stmt("e1", "name", "Alice")
    result = compute_diff(_EMPTY, _df(a))
    assert result.removed_count == 0
    assert result.added_count == 1
    assert result.unchanged_count == 0
    assert result.rows[0][0] == "+"


def test_all_unchanged() -> None:
    a = _stmt("e1", "name", "Alice")
    b = _stmt("e1", "name", "Bob")
    left = _df(a, b)
    result = compute_diff(left, left)
    assert result.rows == []
    assert result.removed_count == 0
    assert result.added_count == 0
    assert result.unchanged_count == 2


def test_mixed() -> None:
    shared = _stmt("e1", "name", "Alice")
    removed = _stmt("e1", "name", "Alicia")
    added = _stmt("e2", "name", "Bob")
    result = compute_diff(_df(shared, removed), _df(shared, added))
    assert result.removed_count == 1
    assert result.added_count == 1
    assert result.unchanged_count == 1
    assert len(result.rows) == 2
    markers = {marker for marker, _ in result.rows}
    assert markers == {"-", "+"}


# ---------------------------------------------------------------------------
# Sort order
# ---------------------------------------------------------------------------


def test_sort_by_entity_then_prop_then_value() -> None:
    s1 = _stmt("e2", "name", "Zara")
    s2 = _stmt("e1", "name", "Alice")
    s3 = _stmt("e1", "name", "Bob")
    result = compute_diff(_df(s1, s2, s3), _EMPTY)
    values = [row.value for _, row in result.rows]
    assert values == ["Alice", "Bob", "Zara"]


def test_removed_before_added_same_key() -> None:
    # Two statements with the same sort key (entity/schema/prop/value) but different
    # datasets produce different IDs; one removed, one added.
    left_stmt = Statement(
        entity_id="e1", prop="name", schema="Thing", value="Alice", dataset="left"
    )
    right_stmt = Statement(
        entity_id="e1", prop="name", schema="Thing", value="Alice", dataset="right"
    )
    result = compute_diff(_df(left_stmt), _df(right_stmt))
    assert result.removed_count == 1
    assert result.added_count == 1
    markers = [marker for marker, _ in result.rows]
    assert markers == ["-", "+"]
