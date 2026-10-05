from nomenklatura.store import View

from zavod.context import Context
from zavod.meta.dataset import Dataset
from zavod.entity import Entity
from zavod.runtime.statistics import Statistics


class BaseValidator:
    """A check on the final output of a dataset, run as part of the export traversal."""

    def __init__(self, context: Context, stats: Statistics) -> None:
        self.context = context
        self.stats = stats
        self.abort = False

    @classmethod
    def enabled(cls, dataset: Dataset) -> bool:
        """Whether this validator should run for the given dataset. Validators
        that can be switched off in the `validators:` metadata block override
        this; the rest always run."""
        return True

    def feed(self, entity: Entity, view: View[Dataset, Entity]) -> None:
        raise NotImplementedError()

    def finish(self) -> None:
        return None
