import pytest
from types import FunctionType

from zavod.meta import Dataset
from zavod.runtime.loader import load_entry_point, example_function


def test_load_entry_point(testdataset1: Dataset):
    func = load_entry_point(testdataset1)
    assert func is not None
    assert isinstance(func, FunctionType)


def test_load_module_by_name(testdataset1: Dataset, analyzer: Dataset):
    testdataset1.entry_point = "zavod.runtime.loader:example_function"
    func = load_entry_point(testdataset1)
    assert func is not None
    assert func == example_function

    with pytest.raises(RuntimeError):
        testdataset1.entry_point = "zavod.runtime.loader:does_not_exist"
        load_entry_point(testdataset1)

    with pytest.raises(RuntimeError):
        testdataset1.entry_point = None
        load_entry_point(testdataset1)
