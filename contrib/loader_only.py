from opensanctions.core import Dataset
from opensanctions.core.resolver import get_resolver
from opensanctions.core.loader import Database


def load(name: str):
    resolver = get_resolver()
    ds = Dataset.require(name)
    db = Database(ds, resolver, cached=True)
    loader = db.view(ds)


if __name__ == "__main__":
    load("sanctions")
