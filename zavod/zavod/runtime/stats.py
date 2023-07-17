# from zavod.entity import Entity


class ContextStats(object):
    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self.statements = 0
        self.entities = 0
        self.targets = 0
