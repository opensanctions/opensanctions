from typing import Any

import pytest
from rigour.territories import get_territory

from zavod.meta import get_catalog
from zavod.meta.dataset import Dataset
from zavod.meta.tags import EU_MEMBERS, PUBLISHER_BLOCS, derive_tags


def make_dataset(
    name: str = "test",
    tags: list[str] | None = None,
    publisher: dict[str, Any] | None = None,
) -> Dataset:
    data: dict[str, Any] = {"name": name, "title": "Test Dataset", "tags": tags or []}
    if publisher is not None:
        data["publisher"] = {"name": "Test Publisher", **publisher}
    return get_catalog().make_dataset(data)


def test_official_publisher_country():
    ds = make_dataset(publisher={"country": "us", "official": True})
    assert derive_tags(ds) == ["publisher.us"]


def test_publisher_country_eu():
    ds = make_dataset(publisher={"country": "eu", "official": True})
    assert derive_tags(ds) == ["publisher.eu", "publisher.europe"]


@pytest.mark.parametrize("country", ["fr", "li", "mc", "rs", "ua"])
def test_publisher_bloc(country: str):
    ds = make_dataset(publisher={"country": country, "official": True})
    assert derive_tags(ds) == [f"publisher.{country}", "publisher.europe"]


@pytest.mark.parametrize("country", ["tr", "ru", "by", "am", "az", "ge", "gb", "je"])
def test_not_in_europe_bloc(country: str):
    ds = make_dataset(publisher={"country": country, "official": True})
    assert derive_tags(ds) == [f"publisher.{country}"]


def test_member_state_gets_no_publisher_eu():
    ds = make_dataset(
        tags=["list.sanction"], publisher={"country": "fr", "official": True}
    )
    assert "publisher.eu" not in derive_tags(ds)


def test_publisher_bloc_members_are_territories():
    assert len(EU_MEMBERS) == 27
    for members in PUBLISHER_BLOCS.values():
        for code in members:
            assert get_territory(code) is not None, code


@pytest.mark.parametrize(
    "publisher",
    [
        {"country": "zz", "official": True},
        {"official": True},
        {"country": "fr", "official": False},
        {"country": "fr"},
    ],
)
def test_no_publisher_tag(publisher: dict[str, Any]):
    ds = make_dataset(publisher=publisher)
    assert derive_tags(ds) == []


def test_no_publisher_block():
    ds = make_dataset()
    assert derive_tags(ds) == []


def test_no_publisher_tag_for_peps():
    ds = make_dataset(tags=["list.pep"], publisher={"country": "fr", "official": True})
    assert derive_tags(ds) == ["list.pep"]


def test_hand_set_tags_kept_first_without_duplicates():
    ds = make_dataset(
        tags=["list.sanction", "publisher.us", "publisher.west"],
        publisher={"country": "us", "official": True},
    )
    assert derive_tags(ds) == ["list.sanction", "publisher.us", "publisher.west"]


def test_collection_gets_no_derived_tags():
    make_dataset()
    coll = get_catalog().make_dataset(
        {
            "name": "coll",
            "title": "Collection",
            "datasets": ["test"],
            "publisher": {"name": "Test", "country": "fr", "official": True},
        }
    )
    assert derive_tags(coll) == []


def test_export_includes_derived_tags():
    ds = make_dataset(
        tags=["list.sanction"], publisher={"country": "fr", "official": True}
    )
    data = ds.to_opensanctions_dict(get_catalog())
    assert data["tags"] == [
        "list.sanction",
        "publisher.fr",
        "publisher.europe",
    ]
    # Derived tags are export-only:
    assert ds.model.tags == ["list.sanction"]
    assert ds.to_dict()["tags"] == ["list.sanction"]
