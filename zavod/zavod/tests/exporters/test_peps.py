from json import load

from zavod.tests.exporters.util import harnessed_export
from zavod.meta import Dataset
from zavod.exporters.peps import PEPSummaryExporter, observe_occupancy
from zavod.context import Context
from zavod import helpers as h
from zavod import settings
from zavod.archive import clear_data_path
from zavod.crawl import crawl_dataset


def test_pep_positions(testdataset2_export: Dataset):
    dataset_path = settings.DATA_PATH / "datasets" / testdataset2_export.name
    clear_data_path(testdataset2_export.name)

    crawl_dataset(testdataset2_export)
    harnessed_export(PEPSummaryExporter, testdataset2_export)

    with open(dataset_path / "pep-positions.json") as peps_file:
        stats = load(peps_file)

    assert len(stats["countries"]) == 2

    us = stats["countries"]["us"]
    assert us["counts"]["total"] == 6
    assert us["counts"]["current"] == 3
    assert us["counts"]["ended"] == 2
    assert us["counts"]["unknown"] == 1

    assert len(us["positions"]) == 2
    rep = us["positions"]["td2-export-44fdcec78a4b6038bcea7903aa5448d59c4aebaf"]
    assert rep["position_name"] == "United States representative"
    assert rep["counts"]["total"] == 3
    assert rep["counts"]["current"] == 1
    assert rep["counts"]["ended"] == 2
    assert rep["counts"]["unknown"] == 0

    fr = stats["countries"]["fr"]
    assert fr["counts"]["total"] == 1
    assert fr["counts"]["current"] == 0
    assert fr["counts"]["ended"] == 0
    assert fr["counts"]["unknown"] == 1


def test_observe_occupancy(vcontext: Context) -> None:
    pos = vcontext.make("Position")
    pos.id = "pos"

    curr = vcontext.make("Occupancy")
    curr.id = "curr"
    curr.add("status", h.OccupancyStatus.CURRENT.value)

    ended = vcontext.make("Occupancy")
    ended.id = "ended"
    ended.add("status", h.OccupancyStatus.ENDED.value)

    unknown = vcontext.make("Occupancy")
    unknown.id = "unknown"
    unknown.add("status", h.OccupancyStatus.UNKNOWN.value)

    occupancies = {}

    observe_occupancy(occupancies, unknown, pos)
    assert occupancies[pos.id] == (unknown, pos)

    observe_occupancy(occupancies, ended, pos)
    assert occupancies[pos.id] == (ended, pos)

    observe_occupancy(occupancies, curr, pos)
    assert occupancies[pos.id] == (curr, pos)

    observe_occupancy(occupancies, ended, pos)
    assert occupancies[pos.id] == (curr, pos)

    observe_occupancy(occupancies, unknown, pos)
    assert occupancies[pos.id] == (curr, pos)