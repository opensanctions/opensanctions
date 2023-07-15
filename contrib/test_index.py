import sys
import yaml


def check_index(path: str):
    with open(path, "r") as fh:
        data = yaml.safe_load(fh)
    for dataset in data["datasets"]:
        assert dataset.get("type") in ["source", "collection", "external"]

        assert "parents" not in dataset
        assert "children" not in dataset
        assert "datasets" not in dataset

        if dataset.get("type") == "collection":
            assert "sources" in dataset
        if dataset.get("type") == "external":
            assert "collections" in dataset
        if dataset.get("type") == "source":
            assert "collections" in dataset

        if dataset.get("name") == "default":
            assert dataset.get("type") == "collection"
            assert dataset.get("hidden") is False
            assert dataset.get("export") is True
            assert "us_ofac_sdn" in dataset.get("sources")
            assert "us_wofac_sdn" not in dataset.get("externals")
            assert "opencorporates" in dataset.get("externals")
            assert "opencorporates" not in dataset.get("sources")
            assert "opencorporates" in dataset.get("scopes")
            assert "us_ofac_sdn" in dataset.get("scopes")
            assert "collections" not in dataset

        if dataset.get("name") == "us_ofac_sdn":
            assert dataset.get("type") == "source"
            assert dataset.get("hidden") is False
            assert dataset.get("export") is True
            assert "all" in dataset.get("collections")
            assert "default" in dataset.get("collections")
            assert "sanctions" in dataset.get("collections")
            assert "crime" not in dataset.get("collections")
            assert "sources" not in dataset
            assert "externals" not in dataset
            assert "scopes" not in dataset

        if dataset.get("name") == "opencorporates":
            assert dataset.get("type") == "external"
            assert dataset.get("hidden") is False
            assert dataset.get("export") is False
            assert "all" in dataset.get("collections")
            assert "default" in dataset.get("collections")
            assert "sanctions" not in dataset.get("collections")
            assert "crime" not in dataset.get("collections")
            assert "sources" not in dataset
            assert "externals" not in dataset
            assert "scopes" not in dataset


if __name__ == "__main__":
    check_index(sys.argv[1])
