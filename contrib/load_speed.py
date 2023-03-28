from opensanctions.core import Dataset
from opensanctions.core.resolver import get_resolver
from opensanctions.core.loader import Database


def load(name: str):
    resolver = get_resolver()
    ds = Dataset.require(name)
    db = Database(ds, resolver, cached=False)
    loader = db.view(ds)
    for idx, entity in enumerate(loader):
        if idx % 10000 == 0:
            print("Load", idx)


if __name__ == "__main__":
    load("sanctions")
