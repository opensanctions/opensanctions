from __future__ import annotations

from typing import Optional
from urllib.parse import urljoin

import zavod
from zavod import settings
from zavod.archive import DATASETS, ARTIFACTS


def make_published_url(dataset_name: str, path: str) -> str:
    """Generate a public URL for a file within the dataset context."""
    prefix = f"{DATASETS}/{settings.RELEASE}/{dataset_name}/"
    url = urljoin(settings.ARCHIVE_SITE, prefix)
    return urljoin(url, path)


def make_artifact_url(dataset_name: str, version: str, path: str) -> str:
    """Generate a public URL for a file within the dataset context."""
    prefix = f"{ARTIFACTS}/{dataset_name}/{version}/"
    url = urljoin(settings.ARCHIVE_SITE, prefix)
    return urljoin(url, path)


def make_entity_url(entity: "zavod.entity.Entity") -> Optional[str]:
    """Generate a public URL for a file within the dataset context."""
    # TODO: implement check if the entity is in default, if not return None
    if entity.id is None:
        return None
    path = f"entities/{entity.id}"
    return urljoin(settings.WEB_SITE, path)
