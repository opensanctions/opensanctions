from zavod.tools.summarize import summarize
from zavod.store import get_view
from zavod.meta import Dataset
from zavod.crawl import crawl_dataset

expected = """
Jakob Maria Mierscheid
Oswell E. Spencer
  link: startDate: ['1968'] endDate: ['2003'] 
  Umbrella Corporation
    name: ['Umbrella Corporation'] 
"""


def test_summarize(testdataset1: Dataset, capsys) -> None:
    crawl_dataset(testdataset1)
    view = get_view(testdataset1)
    summarize(
        view, "Person", "ownershipOwner", ["startDate", "endDate"], "asset", ["name"]
    )
    stdout = capsys.readouterr().out

    assert expected in stdout, stdout
