from pathlib import Path

import pytest
import yaml
from followthemoney.dataset import evaluate_query, parse_query

from zavod import settings
from zavod.exc import ConfigurationException
from zavod.meta import get_catalog, load_directory_catalog

FIXTURES = {
    # Named so the collection globs before its members, exercising
    # order-independent child linking without archive fetches:
    "aaa_collection.yml": {
        "title": "Test Collection",
        "children": ["ds_plain", "ds_tagged"],
    },
    "ds_plain.yml": {
        "title": "Plain Dataset",
    },
    "ds_tagged.yml": {
        "title": "Tagged Dataset",
        "tags": ["foo.bar", "foo.bar.sub"],
    },
    "nested/ds_other.yml": {
        "title": "Other Tagged Dataset",
        "tags": ["foo.bar"],
    },
}


@pytest.fixture()
def datasets_path(tmp_path: Path) -> Path:
    for rel, data in FIXTURES.items():
        path = tmp_path / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(yaml.safe_dump(data))
    return tmp_path


def test_load_directory_catalog(datasets_path: Path) -> None:
    catalog = load_directory_catalog(datasets_path)
    for name in ("aaa_collection", "ds_plain", "ds_tagged", "ds_other"):
        assert catalog.has(name), name

    # Children are linked even though the collection loaded first:
    collection = catalog.require("aaa_collection")
    assert collection.is_collection
    child_names = {c.name for c in collection.children}
    assert child_names == {"ds_plain", "ds_tagged"}


def test_load_directory_catalog_query(datasets_path: Path) -> None:
    catalog = load_directory_catalog(datasets_path)
    names = {d.name for d in evaluate_query(catalog, parse_query("#foo.bar"))}
    assert names == {"ds_tagged", "ds_other"}
    query = parse_query("#foo.bar - #foo.bar.sub")
    names = {d.name for d in evaluate_query(catalog, query)}
    assert names == {"ds_other"}
    names = {d.name for d in evaluate_query(catalog, "aaa_collection")}
    assert names == {"ds_plain", "ds_tagged"}


def test_load_directory_catalog_is_fresh(datasets_path: Path) -> None:
    # Every call builds a new catalog; the process-wide singleton stays empty:
    first = load_directory_catalog(datasets_path)
    second = load_directory_catalog(datasets_path)
    assert first is not second
    assert first.names == second.names
    assert first is not get_catalog()
    assert not get_catalog().has("ds_plain")


def test_load_directory_catalog_skips_invalid(datasets_path: Path) -> None:
    (datasets_path / "broken.yml").write_text("title: {invalid")
    (datasets_path / "not_a_dataset.yml").write_text("- just\n- a\n- list\n")
    catalog = load_directory_catalog(datasets_path)
    assert catalog.has("ds_plain")
    assert not catalog.has("broken")
    assert not catalog.has("not_a_dataset")


def test_load_directory_catalog_missing_path(tmp_path: Path) -> None:
    with pytest.raises(ConfigurationException, match="does not exist"):
        load_directory_catalog(tmp_path / "nope")


def test_default_datasets_path() -> None:
    # Defaults to the datasets folder of the repository checkout containing
    # this zavod installation:
    repo_root = Path(settings.__file__).resolve().parent.parent.parent
    assert settings.DATASETS_PATH == repo_root / "datasets"
    assert settings.DATASETS_PATH.is_dir()
