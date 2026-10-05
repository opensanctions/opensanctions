from pydantic import BaseModel


class ValidatorsSpec(BaseModel):
    """Per-dataset switches for the validators run after a crawl.

    Every validator here is on by default. Turn one off only when its finding is
    understood and accepted for that specific dataset, and say why in a YAML
    comment - a silenced validator is invisible in the run output.
    """

    entity_reference: bool = True
    """Check that entity-type properties resolve, and point at an entity whose
    schema matches the property's declared range."""
