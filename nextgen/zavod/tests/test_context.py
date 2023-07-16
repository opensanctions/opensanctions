from zavod.context import Context
from zavod.meta import Dataset


def test_run_dataset(vdataset: Dataset):
    context = Context(vdataset)

    context.close()
