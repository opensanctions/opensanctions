from __future__ import annotations

from typing import Optional

import zavod
from zavod import settings
from zavod.archive import DATASETS, ARTIFACTS


def make_published_url(dataset_name: str, path: str) -> str:
    """Generate a public URL for a file within the dataset context."""
    return (
        f"{settings.ARCHIVE_SITE}/{DATASETS}/{settings.RELEASE}/{dataset_name}/{path}"
    )


def make_artifact_url(dataset_name: str, version: str, path: str) -> str:
    """Generate a public URL for a file within the dataset context."""
    return f"{settings.ARCHIVE_SITE}/{ARTIFACTS}/{dataset_name}/{version}/{path}"


def make_entity_url(entity: "zavod.entity.Entity") -> Optional[str]:
    """Generate a public URL for a file within the dataset context."""
    # TODO: implement check if the entity is in default, if not return None
    if entity.id is None:
        return None
    return f"{settings.WEB_SITE}/entities/{entity.id}"
