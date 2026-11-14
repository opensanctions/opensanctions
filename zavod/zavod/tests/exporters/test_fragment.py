from zavod.entity import Entity
from zavod.exporters.fragment import ViewFragment
from zavod.meta import Dataset, get_catalog
from zavod.tests.util import finish_statements, get_test_view, make_context


def emit(ds: Dataset, schema: str, id: str, properties: dict[str, list[str]]) -> None:
    context = make_context(ds)
    context.begin()
    entity = Entity.from_data(
        context.dataset, {"schema": schema, "id": id, "properties": properties}
    )
    context.emit(entity)
    finish_statements(context)
    context.close()


def test_fragment_returns_only_consolidated_entities() -> None:
    catalog = get_catalog()
    # One dataset per entity: each emit runs a whole crawl of its dataset.
    ds_person = Dataset({"name": "test_fragment_ds_person"})
    ds_sanction = Dataset({"name": "test_fragment_ds_sanction"})
    ds_a = Dataset({"name": "test_fragment_ds_a"})
    ds_b = Dataset({"name": "test_fragment_ds_b"})
    collection = Dataset(
        {
            "name": "test_fragment_collection",
            "children": [ds_person.name, ds_sanction.name, ds_a.name, ds_b.name],
        }
    )
    for ds in (ds_person, ds_sanction, ds_a, ds_b, collection):
        catalog.add(ds)

    # Each entity has statements that consolidation removes.
    emit(
        ds_person,
        "Person",
        "a-person",
        {
            "name": ["Jane Doe"],
            "birthDate": ["1972", "1972-04-12"],
            "addressEntity": ["z-address"],
        },
    )
    emit(
        ds_sanction,
        "Sanction",
        "s-sanction",
        {"entity": ["a-person"], "startDate": ["2020", "2020-05-01"]},
    )
    emit(
        ds_a,
        "Address",
        "z-address",
        {"full": ["1 Main St"], "modifiedAt": ["2023-01-01"]},
    )
    emit(
        ds_b,
        "Address",
        "z-address",
        {"full": ["1 Main St"], "modifiedAt": ["2024-01-01"]},
    )

    view = get_test_view(collection)
    try:
        raw_person = view.get_entity("a-person")
        assert raw_person is not None
        assert sorted(raw_person.get("birthDate")) == ["1972", "1972-04-12"]
        fragment = ViewFragment(view, raw_person)

        # The entity the fragment was made for.
        assert fragment.entity.get("birthDate") == ["1972-04-12"]
        assert fragment.get_entity("a-person") is fragment.entity

        # Forward lookups.
        address = fragment.get_entity("z-address")
        assert address is not None
        assert address.get("modifiedAt") == ["2024-01-01"]

        # Inverted lookups: first through the store view, then from the
        # fragment's own cache.
        for _ in range(2):
            sanctions = [e for _, e in fragment.get_inverted("a-person")]
            assert [s.get("startDate") for s in sanctions] == [["2020-05-01"]]

        # Both directions together.
        adjacent = {e.id: e for _, e in fragment.get_adjacent(fragment.entity)}
        assert adjacent["z-address"].get("modifiedAt") == ["2024-01-01"]
        assert adjacent["s-sanction"].get("startDate") == ["2020-05-01"]

        # The store view's raw entities keep all their statements.
        raw_address = view.get_entity("z-address")
        assert raw_address is not None
        assert sorted(raw_address.get("modifiedAt")) == ["2023-01-01", "2024-01-01"]
    finally:
        view.store.close()
