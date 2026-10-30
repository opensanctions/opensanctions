from typing import Any

import pytest
from rigour.territories import get_territory

from zavod.meta import get_catalog
from zavod.meta.dataset import Dataset
from zavod.meta.tags import EU_MEMBERS, PUBLISHER_BLOCS, derive_tags
from zavod.stateful.programs import Program


def make_dataset(
    name: str = "test",
    tags: list[str] | None = None,
    publisher: dict[str, Any] | None = None,
) -> Dataset:
    data: dict[str, Any] = {"name": name, "title": "Test Dataset", "tags": tags or []}
    if publisher is not None:
        data["publisher"] = {"name": "Test Publisher", **publisher}
    return get_catalog().make_dataset(data)


@pytest.fixture(autouse=True)
def no_programs(monkeypatch: pytest.MonkeyPatch) -> None:
    """Isolate publisher tests from the real program metadata."""
    monkeypatch.setattr("zavod.meta.tags.get_all_programs_by_key", lambda: {})


def set_programs(monkeypatch: pytest.MonkeyPatch, programs: list[Program]) -> None:
    by_key = {p.key: p for p in programs}
    monkeypatch.setattr("zavod.meta.tags.get_all_programs_by_key", lambda: by_key)


def test_official_publisher_country():
    ds = make_dataset(publisher={"country": "us", "official": True})
    assert derive_tags(ds) == ["publisher.us"]


def test_publisher_country_eu():
    ds = make_dataset(publisher={"country": "eu", "official": True})
    assert derive_tags(ds) == ["publisher.eu"]


@pytest.mark.parametrize("country", ["fr", "li"])
def test_publisher_bloc(country: str):
    ds = make_dataset(publisher={"country": country, "official": True})
    assert derive_tags(ds) == [f"publisher.{country}", "publisher.eu_efta"]


def test_member_state_sanctions_list_gets_publisher_eu():
    ds = make_dataset(
        tags=["list.sanction"], publisher={"country": "fr", "official": True}
    )
    assert derive_tags(ds) == [
        "list.sanction",
        "publisher.fr",
        "publisher.eu",
        "publisher.eu_efta",
    ]


@pytest.mark.parametrize(
    "tags,publisher",
    [
        # Not a sanctions list:
        (["list.wanted"], {"country": "fr", "official": True}),
        # Not an EU member state:
        (["list.sanction"], {"country": "ch", "official": True}),
        # Not official:
        (["list.sanction"], {"country": "fr", "official": False}),
    ],
)
def test_no_publisher_eu(tags: list[str], publisher: dict[str, Any]):
    ds = make_dataset(tags=tags, publisher=publisher)
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


def test_target_tags_from_programs(monkeypatch: pytest.MonkeyPatch):
    set_programs(
        monkeypatch,
        [
            Program(
                key="XX-RUS",
                title="Active",
                dataset="test",
                target_territories=["ru", "by"],
            ),
            Program(
                key="XX-LEGACY",
                title="Legacy",
                dataset="test",
                status="legacy",
                target_territories=["ua", "ru"],
            ),
            Program(
                key="XX-ENDED",
                title="Ended",
                dataset="test",
                status="ended",
                target_territories=["lr"],
            ),
            Program(
                key="XX-GLOBAL",
                title="Global",
                dataset="test",
                target_territories=["zz", "ip"],
            ),
            Program(
                key="XX-OTHER",
                title="Other dataset",
                dataset="other",
                target_territories=["cn"],
            ),
        ],
    )
    ds = make_dataset(tags=["target.ru"], publisher={"country": "us", "official": True})
    assert derive_tags(ds) == [
        "target.ru",
        "publisher.us",
        "target.by",
        "target.ua",
        "target.ip",
    ]


def test_collection_gets_no_derived_tags(monkeypatch: pytest.MonkeyPatch):
    set_programs(
        monkeypatch,
        [
            Program(
                key="XX-RUS", title="Active", dataset="coll", target_territories=["ru"]
            )
        ],
    )
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
        "publisher.eu",
        "publisher.eu_efta",
    ]
    # Derived tags are export-only:
    assert ds.model.tags == ["list.sanction"]
    assert ds.to_dict()["tags"] == ["list.sanction"]
