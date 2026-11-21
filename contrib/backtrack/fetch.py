"""Download the daily versions of a dataset's source files from the archive.

Source files live in two places in the archive bucket:

* `datasets/{YYYYMMDD}/{dataset}/` holds one copy per day, written by the last
  run of that day; it runs from 2021-05 to 2026-08.
* `artifacts/{dataset}/{version}/` holds one copy per run, from 2026-06 on.

Observations are dated by day, so we keep one copy per day: the last run of
each day where per-run copies exist, and the daily copies before that. Changes
are detected from object MD5 hashes in the bucket listing, so only the days
whose content differs from the previous day are downloaded.

The versions of a resource are stored in one solid archive,
`<data path>/backtrack/<dataset>/<resource>.tar.xz`, oldest first. Its
dictionary is larger than a version, so a day that repeats most of the previous
one costs little. Each member is named `{timestamp}-{resource}`, where the
timestamp is `YYYYMMDDHHMMSS` of the last run on the first day that had this
content; daily copies only carry a date, so their timestamp is padded to
midnight. A re-run appends the new days as a further xz stream, which starts
with an empty dictionary.

A change of the file hash does not imply a change of the list: re-published
files with a new issue date or container timestamps differ byte-wise only.
"""

import logging
import lzma
import tarfile
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

import click
from google.cloud.storage import Blob, Bucket, Client  # type: ignore

from zavod import settings

log = logging.getLogger("backtrack.fetch")
BUCKET = "data.opensanctions.org"
# A dictionary larger than the biggest source version lets the next day's
# copy refer back to all of it.
FILTERS = [{"id": lzma.FILTER_LZMA2, "preset": 1, "dict_size": 256 << 20}]


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
    last_runs: dict[str, SourceVersion] = {}
    for version in versions:
        if version.daily:
            continue
        last = last_runs.get(version.timestamp[:8])
        if last is None or version.timestamp > last.timestamp:
            last_runs[version.timestamp[:8]] = version
    # The daily copy of a day with per-run copies is the last run of that day,
    # so it would sort before the runs it duplicates.
    cutoff = min(last_runs) if last_runs else "99999999"
    daily = [v for v in versions if v.daily and v.timestamp[:8] < cutoff]
    timeline = sorted(daily + list(last_runs.values()), key=lambda v: v.timestamp)
    changes: list[SourceVersion] = []
    for version in timeline:
        if version.blob.md5_hash is None:
            raise ValueError(f"No MD5 hash in bucket listing: {version.blob.name}")
        if changes and changes[-1].blob.md5_hash == version.blob.md5_hash:
            continue
        changes.append(version)
    return changes


def archived_names(archive: Path) -> list[str]:
    """List the member names of a resource archive, or none if it doesn't exist."""
    if not archive.exists():
        return []
    with tarfile.open(archive, "r:xz", ignore_zeros=True) as tar:
        return tar.getnames()


def find_missing(archive: Path, changes: list[SourceVersion]) -> list[SourceVersion]:
    """Return the versions to append to the archive, all newer than its last member."""
    names = archived_names(archive)
    missing = [v for v in changes if v.file_name not in names]
    if len(names) > 0 and len(missing) > 0 and missing[0].file_name < names[-1]:
        raise RuntimeError(
            f"{archive}: {missing[0].file_name} predates the last member, "
            "delete the archive to rebuild it"
        )
    return missing


def append_versions(archive: Path, versions: list[SourceVersion]) -> None:
    """Download versions one at a time and append them as one xz stream."""
    archive.parent.mkdir(parents=True, exist_ok=True)
    partial = archive.with_name(f".{archive.name}.partial")
    size = archive.stat().st_size if archive.exists() else 0
    with open(archive, "ab") as fh:
        try:
            with (
                lzma.LZMAFile(fh, "w", format=lzma.FORMAT_XZ, filters=FILTERS) as xz,
                tarfile.open(fileobj=xz, mode="w|") as tar,
            ):
                for version in versions:
                    log.info("Downloading %s", version.blob.name)
                    version.blob.download_to_filename(partial, checksum="md5")
                    tar.add(partial, arcname=version.file_name)
                    partial.unlink()
        except BaseException:
            # A half-written stream would make the whole archive unreadable.
            fh.truncate(size)
            raise


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
    """Fetch the distinct daily versions of DATASET's source files from the archive."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    logging.getLogger("google").setLevel(logging.WARNING)
    out_dir = settings.DATA_PATH / "backtrack" / dataset
    bucket = Client().bucket(BUCKET)
    versions = list_daily(bucket, dataset, prefix)
    versions.extend(list_artifacts(bucket, dataset, prefix))
    by_resource: dict[str, list[SourceVersion]] = {}
    for version in versions:
        by_resource.setdefault(version.resource, []).append(version)
    for resource, resource_versions in sorted(by_resource.items()):
        changes = find_changes(resource_versions)
        archive = out_dir / f"{resource}.tar.xz"
        missing = find_missing(archive, changes)
        log.info(
            "%s: %d copies, %d distinct, %s .. %s, %d to fetch",
            resource,
            len(resource_versions),
            len(changes),
            changes[0].timestamp,
            changes[-1].timestamp,
            len(missing),
        )
        if dry_run:
            for version in missing:
                print(version.file_name, version.blob.size, version.blob.name)
        elif len(missing) > 0:
            append_versions(archive, missing)


if __name__ == "__main__":
    main()
