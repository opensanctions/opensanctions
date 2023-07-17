from opensanctions.core import get_catalog

# all = Dataset.require("all")
dataset = get_catalog().require("all")

print("CHILDREN", len(dataset.children))
print("DATASETS", len(dataset.datasets))
print("LEAVES", len(dataset.leaves))

for ds in dataset.catalog.datasets:
    if ds not in dataset.datasets:
        print(ds.name)
