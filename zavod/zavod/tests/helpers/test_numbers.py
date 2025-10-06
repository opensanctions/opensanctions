from zavod.helpers.numbers import apply_number

from zavod.entity import Entity
from zavod.meta import Dataset


def test_apply_number(testdataset1: Dataset):
    data = {"id": "ship", "schema": "Vessel", "properties": {"name": ["JOLLY ROGER"]}}
    entity = Entity(testdataset1, data)

    apply_number(entity, "tonnage", "1000")
    assert entity.get("tonnage") == ["1000"]
    entity.pop("tonnage")

    apply_number(entity, "tonnage", 999)
    assert entity.get("tonnage") == ["999"]
    entity.pop("tonnage")

    apply_number(entity, "tonnage", 999.791)
    assert entity.get("tonnage") == ["999.79"]
    entity.pop("tonnage")

    apply_number(entity, "tonnage", "2000tons")
    assert entity.get("tonnage") == ["2000 t"]
    entity.pop("tonnage")
