from zavod.context import Context
from zavod.meta import Dataset
from zavod.runtime.loader import load_entry_point


def test_run_dataset(vdataset: Dataset):
    context = Context(vdataset)
    func = load_entry_point(vdataset)
    func(context)
    context.close()
