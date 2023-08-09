import os
import shutil
from pathlib import Path
from functools import cache
from typing import cast, Any, Optional, IO, Protocol
from google.cloud.storage import Client, Blob  # type: ignore

from zavod import settings
from zavod.logs import get_logger


log = get_logger(__name__)


class ProtocolBlob(Protocol):
    def open(self: Blob, mode: str, chunk_size: int) -> None:
        ...

    def download_to_filename(self, dst: str) -> None:
        ...

    def reload(self) -> None:
        ...


class Backend(Protocol):
    def get_blob(self, name: str) -> ProtocolBlob:
        ...


class ConfigurationException(Exception):
    def __init__(self, message: str) -> None:
        self.message = message


class GoogleCloudBackend:
    def __init__(self) -> None:
        if settings.ARCHIVE_BUCKET is None:
            raise ConfigurationException("No backfill bucket configured")
        client = Client()
        self.bucket = client.get_bucket(settings.ARCHIVE_BUCKET)

    def get_blob(self, name: str) -> Blob:
        return self.bucket.get_blob(name)


# Other than being a bit icky, there's no real demand for abstracting away
# the Google Cloud Storage API right now, so this is a blatant mirror for now.
class FileSystemBlob:
    def __init__(self, base_path: Path, name: str) -> None:
        self.path = base_path / name
        self.name = name

    def open(self, mode: str, chunk_size: int) -> IO[Any]:
        log.info(f"Opening {self.path}")
        return open(self.path, mode, buffering=chunk_size)

    def download_to_filename(self, dst: str) -> None:
        log.info(f"Copying file {self.path} to {dst}")
        shutil.copyfile(self.path, dst)

    def reload(self) -> None:
        pass


class FileSystemBackend:
    def get_blob(self, name: str) -> Optional[FileSystemBlob]:
        path = settings.ARCHIVE_PATH / name
        if os.path.isfile(path):
            return FileSystemBlob(settings.ARCHIVE_PATH, name)
        else:
            log.warning(f"File {path.as_posix()} doesn't exist.")
            return None


backends = {
    "GoogleCloudBackend": GoogleCloudBackend,
    "FileSystemBackend": FileSystemBackend,
}


@cache
def get_archive_backend() -> Optional[Backend]:
    if settings.ARCHIVE_BACKEND is None:
        log.info("No backfill backend configured.")
        return None
    try:
        return cast(Backend, backends[settings.ARCHIVE_BACKEND]())
    except ConfigurationException as error:
        log.warning(error.message)
        return None
