from pydantic import BaseModel


class NumbersSpec(BaseModel):
    """A standardised configuration for number parsing in the context of a dataset."""

    decimal: str = "."
    separator: str = ","
