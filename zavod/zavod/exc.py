class ZavodException(Exception):
    """Generic root exception for Zavod"""

    pass


class RunFailedException(ZavodException):
    pass


class ConfigurationException(ZavodException):
    def __init__(self, message: str) -> None:
        self.message = message
