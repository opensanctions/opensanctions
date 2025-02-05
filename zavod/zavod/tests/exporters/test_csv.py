import re

from zavod import settings
from zavod.archive import clear_data_path
from zavod.context import Context
from zavod.crawl import crawl_dataset
from zavod.exporters.simplecsv_02 import (
    SimpleCSV_02Exporter,
    hash_id,
    prefer_latin_name,
)
from zavod.entity import Entity
from zavod.integration.dedupe import get_dataset_linker
from zavod.meta.dataset import Dataset
from zavod.store import get_store


NO_LATIN = {
    "schema": "Person",
    "id": "id1",
    "caption": "Влади́мир Влади́мирович ПУ́ТИН",
    "properties": {
        "name": [
            "Влади́мир Влади́мирович ПУ́ТИН",
            "ウラジーミル・プーチン",
        ]
    },
}

NON_LATIN_CAPTION = {
    "schema": "Person",
    "id": "id2",
    # non-latin name was chosen e.g. because it's more of a centroid
    "caption": "Влади́мир Влади́мирович ПУ́ТИН",
    "properties": {
        "name": [
            "Vladímír Pútín",  # latin-1 chars, not ascii
            "Влади́мир Влади́мирович ПУ́ТИН",
            "Путін Володимир Володимирович",
            "Уладзімір Уладзіміравіч Пуцін",
            "ウラジーミル・プーチン",
        ],
        "address": ["1 the street,\nCity,\nCountry"],
        "topics": ["sanction"],  # Some target topic
    },
}


def test_hash_id():
    id_ = hash_id("Q7747")
    assert re.match(r"^\d{32}$", id_)
    assert id_ == "04124194357288488917144704985560"
    assert hash_id("NK-b32TGfwW8Yk82mdxvaXt3C") == "49540327081994372221107446015357"
    assert (
        hash_id("us-cia-russia-vladimir-vladimirovich-putin-president")
        == "98684556158406294838570835729284"
    )


def test_prefer_latin_name(testdataset1: Dataset):
    non_latin_caption = Entity.from_data(testdataset1, NON_LATIN_CAPTION)
    assert prefer_latin_name(non_latin_caption) == "Vladímír Pútín"
    no_latin = Entity.from_data(testdataset1, NO_LATIN)
    assert prefer_latin_name(no_latin) == "Влади́мир Влади́мирович ПУ́ТИН"


def test_simplecsv_02(testdataset1: Dataset):
    entity = Entity.from_data(testdataset1, NON_LATIN_CAPTION)

    # Export
    dataset_path = settings.DATA_PATH / "datasets" / testdataset1.name
    clear_data_path(testdataset1.name)
    crawl_dataset(testdataset1)
    context = Context(testdataset1)
    context.begin(clear=False)
    store = get_store(testdataset1, get_dataset_linker(testdataset1))
    view = store.default_view()
    exporter = SimpleCSV_02Exporter(context, view)
    exporter.setup()
    exporter.feed(entity)
    exporter.finish()
    context.close()
    store.close()

    with open(dataset_path / "targets.simple-02.csv") as nested_file:
        lines = nested_file.read().split("\n")
    assert len(lines) == 3
    assert lines[2] == ""  # empty line at the
    # All fields are always quoted
    headings = lines[0].split('","')
    vlad_line = lines[1]
    vlad_fields = vlad_line.split('","')
    vlad = dict(zip(headings, vlad_fields))
    # ID is hashed to 32-digit wide string of digits
    # We haven't stripped quotes from first and last field ends
    assert vlad['"id'] == '"00911055549423107825995924846505'
    # The latin script name is used.
    assert vlad["name"] == "Vladímír Pútín"
    # Newlines are replaced with spaces
    # CSV writer double quotes field containing delimiter
    assert vlad["addresses"] == '""1 the street, City, Country""'
    assert vlad["topics"] == "sanction"
