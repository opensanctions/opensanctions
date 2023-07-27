from zavod.meta import Dataset
from zavod.tools.meta_index import export_index


def test_export_index(vcollection: Dataset):
    export_index(vcollection)