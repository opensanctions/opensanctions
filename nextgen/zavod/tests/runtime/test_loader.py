from types import FunctionType

from zavod.runtime.loader import load_entry_point, example_function


def test_load_entry_point(vdataset):
    func = load_entry_point(vdataset)
    assert func is not None
    assert isinstance(func, FunctionType)


def test_load_module_by_name(vdataset):
    vdataset.entry_point = "zavod.runtime.loader:example_function"
    func = load_entry_point(vdataset)
    assert func is not None
    assert func == example_function
