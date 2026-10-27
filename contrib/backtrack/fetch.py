"""Download every distinct version of a dataset's source files from the archive.

Source files live in two places in the archive bucket:

* `datasets/{YYYYMMDD}/{dataset}/` holds one copy per day, written by the last
  run of that day; it runs from 2021-05 to 2026-08.
* `artifacts/{dataset}/{version}/` holds one copy per run, from 2026-06 on.

We use the per-run copies wherever they exist and the daily copies before
that. Changes are detected from object MD5 hashes in the bucket listing, so
only the files whose content differs from the previous run are downloaded.

Each version is saved as `{timestamp}-{resource}`, where the timestamp is
`YYYYMMDDHHMMSS` of the first run that had this content. Daily copies only
carry a date, so their timestamp is padded to midnight. Files are written to
`<data path>/backtrack/<dataset>/`; those already present are skipped, so a
re-run only fetches what is new.

A change of the file hash does not imply a change of the list: re-published
files with a new issue date or container timestamps differ byte-wise only.
"""

import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

import click
from google.cloud.storage import Blob, Bucket, Client  # type: ignore

from zavod import settings

log = logging.getLogger("backtrack.fetch")
BUCKET = "data.opensanctions.org"


@dataclass
class SourceVersion:
    """One archived copy of a source file."""

    resource: str
    timestamp: str
    blob: Blob
    daily: bool

    @property
    def file_name(self) -> str:
        return f"{self.timestamp}-{self.resource}"


def list_dates(bucket: Bucket) -> list[str]:
    """List the date stamps of the legacy `datasets/{YYYYMMDD}/` folders."""
    pages = bucket.list_blobs(prefix="datasets/", delimiter="/")
    for _ in pages:
        pass
    stamps = [p.split("/")[1] for p in pages.prefixes]
    return sorted(s for s in stamps if s.isdigit() and len(s) == 8)


def list_daily(bucket: Bucket, dataset: str, prefix: str) -> list[SourceVersion]:
    """List the source files in the legacy date-stamped folders."""

    def list_day(date: str) -> list[SourceVersion]:
        blobs = bucket.list_blobs(prefix=f"datasets/{date}/{dataset}/{prefix}")
        return [
            SourceVersion(blob.name.rsplit("/", 1)[-1], f"{date}000000", blob, True)
            for blob in blobs
        ]

    with ThreadPoolExecutor(32) as pool:
        days = pool.map(list_day, list_dates(bucket))
        return [version for day in days for version in day]


def list_artifacts(bucket: Bucket, dataset: str, prefix: str) -> list[SourceVersion]:
    """List the source files in the per-run artifact folders."""
    versions: list[SourceVersion] = []
    glob = f"artifacts/{dataset}/*/{prefix}*"
    for blob in bucket.list_blobs(prefix=f"artifacts/{dataset}/", match_glob=glob):
        _, _, version_id, resource = blob.name.split("/")
        timestamp = version_id.split("-", 1)[0]
        if len(timestamp) != 14 or not timestamp.isdigit():
            raise ValueError(f"Unexpected version ID: {blob.name}")
        versions.append(SourceVersion(resource, timestamp, blob, False))
    return versions


def find_changes(versions: list[SourceVersion]) -> list[SourceVersion]:
    """Merge daily and per-run copies of one resource and keep only the changes."""
    run_days = [v.timestamp[:8] for v in versions if not v.daily]
    # The daily copy of a day with per-run copies is the last run of that day,
    # so it would sort before the runs it duplicates.
    cutoff = min(run_days) if run_days else "99999999"
    timeline = sorted(
        (v for v in versions if not v.daily or v.timestamp[:8] < cutoff),
        key=lambda v: v.timestamp,
    )
    changes: list[SourceVersion] = []
    for version in timeline:
        if version.blob.md5_hash is None:
            raise ValueError(f"No MD5 hash in bucket listing: {version.blob.name}")
        if changes and changes[-1].blob.md5_hash == version.blob.md5_hash:
            continue
        changes.append(version)
    return changes


def download(version: SourceVersion, out_dir: Path) -> None:
    path = out_dir / version.file_name
    # Downloads are verified before the rename, so an existing file is complete.
    if path.exists():
        return
    log.info("Downloading %s -> %s", version.blob.name, path.name)
    partial = path.with_name(f".{path.name}.partial")
    version.blob.download_to_filename(partial, checksum="md5")
    partial.rename(path)


@click.command()
@click.argument("dataset")
@click.option(
    "--prefix",
    default="source",
    show_default=True,
    help="File name prefix of the resources to fetch.",
)
@click.option("--dry-run", is_flag=True, help="List the changes without downloading.")
def main(dataset: str, prefix: str, dry_run: bool) -> None:
    """Fetch every distinct version of DATASET's source files from the archive."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    logging.getLogger("google").setLevel(logging.WARNING)
    out_dir = settings.DATA_PATH / "backtrack" / dataset
    bucket = Client().bucket(BUCKET)
    versions = list_daily(bucket, dataset, prefix)
    versions.extend(list_artifacts(bucket, dataset, prefix))
    by_resource: dict[str, list[SourceVersion]] = {}
    for version in versions:
        by_resource.setdefault(version.resource, []).append(version)
    changes: list[SourceVersion] = []
    for resource, resource_versions in sorted(by_resource.items()):
        resource_changes = find_changes(resource_versions)
        log.info(
            "%s: %d copies, %d distinct, %s .. %s",
            resource,
            len(resource_versions),
            len(resource_changes),
            resource_changes[0].timestamp,
            resource_changes[-1].timestamp,
        )
        changes.extend(resource_changes)
    if dry_run:
        for change in changes:
            print(change.file_name, change.blob.size, change.blob.name)
        return
    out_dir.mkdir(parents=True, exist_ok=True)
    with ThreadPoolExecutor(4) as pool:
        list(pool.map(lambda v: download(v, out_dir), changes))


if __name__ == "__main__":
    main()
