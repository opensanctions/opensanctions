import shutil
import warnings
from pathlib import Path
from functools import cache
from typing import cast, Dict, Optional, Type, TextIO
from google.cloud.storage import Client, Blob  # type: ignore

from zavod import settings
from zavod.logs import get_logger
from zavod.exc import ConfigurationException


log = get_logger(__name__)
BLOB_CHUNK = 40 * 1024 * 1024
warnings.filterwarnings(
    "ignore", "Your application has authenticated using end user credentials"
)


class ArchiveObject(object):
    def __init__(self, name: str) -> None:
        self.name = name

    def exists(self) -> bool:
        raise NotImplementedError

    def size(self) -> int:
        raise NotImplementedError

    def backfill(self, dest: Path) -> None:
        raise NotImplementedError

    def publish(
        self,
        source: Path,
        mime_type: Optional[str] = None,
        ttl: Optional[int] = False,
    ) -> None:
        raise NotImplementedError

    def republish(self, source: str) -> None:
        """Copy the object inside the archive, avoid re-uploads"""
        pass

    def open(self) -> TextIO:
        raise NotImplementedError


class ArchiveBackend(object):
    def get_object(self, name: str) -> ArchiveObject:
        raise NotImplementedError


class GoogleCloudObject(ArchiveObject):
    """Google Cloud Storage object.

    This is built to use object generations on GCS, which only makes complete sense
    if you're using it on a versioned bucket which will allow a user to continue
    streaming blobs even after they've been replaced with a new version.
    """

    def __init__(self, backend: "GoogleCloudBackend", name: str) -> None:
        self.backend = backend
        self.name = name
        self._blob: Optional[Blob] = None

    @property
    def blob(self) -> Optional[Blob]:
        if self._blob is None:
            self._blob = self.backend.bucket.get_blob(self.name)
        return self._blob

    def exists(self) -> bool:
        return self.blob is not None

    def size(self) -> int:
        if self.blob is None:
            return 0
        return self.blob.size or 0

    def open(self) -> TextIO:
        if self.blob is None:
            raise RuntimeError("Object does not exist: %s" % self.name)
        self.blob.reload()
        return cast(TextIO, self.blob.open(mode="r", chunk_size=BLOB_CHUNK))

    def backfill(self, dest: Path) -> None:
        if self.blob is None:
            raise RuntimeError("Object does not exist: %s" % self.name)
        self.blob.download_to_filename(dest)

    def publish(
        self,
        source: Path,
        mime_type: Optional[str] = None,
        ttl: Optional[int] = None,
    ) -> None:
        self._blob = self.backend.bucket.blob(self.name)
        if ttl is not None:
            self._blob.cache_control = f"public, max-age={ttl}"
        log.info(f"Uploading blob: {source.name}", blob_name=self.name, max_age=ttl)
        self._blob.upload_from_filename(source, content_type=mime_type)

    def republish(self, source: str) -> None:
        source_blob = self.backend.bucket.get_blob(source)
        if source_blob is None:
            raise RuntimeError("Object does not exist: %s" % source)
        # TODO: add if_generation_match
        log.info(f"Copying blob: {self.name}", source=source)
        self._blob = self.backend.bucket.copy_blob(
            source_blob,
            self.backend.bucket,
            self.name,
        )


class GoogleCloudBackend(ArchiveBackend):
    def __init__(self) -> None:
        if settings.ARCHIVE_BUCKET is None:
            raise ConfigurationException("No backfill bucket configured")
        self.client = Client()
        self.bucket = self.client.get_bucket(settings.ARCHIVE_BUCKET)

    def get_object(self, name: str) -> GoogleCloudObject:
        return GoogleCloudObject(self, name)


class AnonymousGoogleCloudBackend(GoogleCloudBackend):
    def __init__(self) -> None:
        if settings.ARCHIVE_BUCKET is None:
            raise ConfigurationException("No backfill bucket configured")
        self.client = Client.create_anonymous_client()
        self.bucket = self.client.get_bucket(settings.ARCHIVE_BUCKET)


class FileSystemObject(ArchiveObject):
    def __init__(self, backend: "FileSystemBackend", name: str) -> None:
        self.backend = backend
        self.path = settings.ARCHIVE_PATH / name
        self.name = name

    def exists(self) -> bool:
        return self.path.exists()

    def size(self) -> int:
        if not self.path.exists():
            return 0
        return self.path.stat().st_size

    def open(self) -> TextIO:
        return open(self.path, "r", buffering=BLOB_CHUNK)

    def backfill(self, dest: Path) -> None:
        log.info(
            f"Copying file: {self.path.stem}",
            source=self.path.as_posix(),
            dest=dest.as_posix(),
        )
        shutil.copyfile(self.path, dest)

    def publish(
        self,
        source: Path,
        mime_type: str | None = None,
        ttl: Optional[int] = None,
    ) -> None:
        log.info(
            f"Copying file: {self.path.name} to archive",
            source=source,
            dest=self.path,
        )
        self.path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, self.path)

    def republish(self, source: str) -> None:
        source_path = settings.ARCHIVE_PATH / source
        log.info(
            f"Copying file: {self.path.name} to archive",
            source=source_path,
            dest=self.path,
        )
        self.path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source_path, self.path)


class FileSystemBackend(ArchiveBackend):
    def get_object(self, name: str) -> FileSystemObject:
        return FileSystemObject(self, name)


backends: Dict[str, Type[ArchiveBackend]] = {
    "GoogleCloudBackend": GoogleCloudBackend,
    "AnonymousGoogleCloudBackend": AnonymousGoogleCloudBackend,
    "FileSystemBackend": FileSystemBackend,
}


@cache
def get_archive_backend() -> ArchiveBackend:
    if settings.ARCHIVE_BACKEND not in backends:
        msg = "Invalid archive backend: %s" % settings.ARCHIVE_BACKEND
        raise ConfigurationException(msg)
    return backends[settings.ARCHIVE_BACKEND]()
