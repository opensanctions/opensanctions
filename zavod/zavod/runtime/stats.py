# from zavod.entity import Entity


class ContextStats(object):
    """A simple object for tracking the number of statements, entities and targets
    emitted by a dataset context while running the dataset method."""

    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self.statements = 0
        self.entities = 0
        self.targets = 0
