from zavod.context import Context
from zavod.meta.dataset import Dataset
from zavod.store import View
from zavod.entity import Entity


class BaseValidator:
    def __init__(self, context: Context, view: View) -> None:
        self.context = context
        self.view = view
        self.abort = False

    @classmethod
    def enabled(cls, dataset: Dataset) -> bool:
        """Whether this validator should run for the given dataset. Validators
        that can be switched off in the `validators:` metadata block override
        this; the rest always run."""
        return True

    def feed(self, entity: Entity) -> None:
        raise NotImplementedError()

    def finish(self) -> None:
        return None
