from decimal import Decimal

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
    assert entity.get("tonnage") == ["999.791"]
    entity.pop("tonnage")

    apply_number(entity, "tonnage", "2000tons")
    assert entity.get("tonnage") == ["2000 t"]
    entity.pop("tonnage")


def test_apply_number_preserves_precision(testdataset1: Dataset):
    data = {"id": "ship", "schema": "Vessel", "properties": {"name": ["JOLLY ROGER"]}}
    entity = Entity(testdataset1, data)

    apply_number(entity, "tonnage", 0.004)
    assert entity.get("tonnage") == ["0.004"]
    entity.pop("tonnage")

    apply_number(entity, "tonnage", "0.005")
    assert entity.get("tonnage") == ["0.005"]
    entity.pop("tonnage")

    apply_number(entity, "tonnage", Decimal("5"))
    assert entity.get("tonnage") == ["5"]
    entity.pop("tonnage")

    apply_number(entity, "tonnage", Decimal("12.75"))
    assert entity.get("tonnage") == ["12.75"]
    entity.pop("tonnage")

    apply_number(entity, "tonnage", Decimal("0.004"))
    assert entity.get("tonnage") == ["0.004"]
    entity.pop("tonnage")
