from zavod.context import Context
from zavod.store import View
from zavod.entity import Entity


class BaseValidator(object):

    def __init__(self, context: Context, view: View) -> None:
        self.context = context
        self.view = view
        self.abort = False

    def feed(self, entity: Entity) -> None:
        raise NotImplementedError()

    def finish(self) -> None:
        return None
