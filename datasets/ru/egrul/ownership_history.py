import multiprocessing
from collections import defaultdict
from datetime import datetime, date
from pathlib import Path
from typing import Iterable, Any, Dict, List

import orjson

from followthemoney import model

from zavod import Context, Entity
from zavod.meta import load_dataset_from_path
from crawler import crawl_local_archive, crawl_remote_archive

INTERNAL_DATA_ARCHIVE_PREFIX = "ru_egrul/egrul.itsoft.ru/EGRUL_406/23.01.2022/"
# INTERNAL_DATA_ARCHIVE_PREFIX = "ru_egrul/egrul.itsoft.ru/EGRUL_406/01.01.2022_FULL/"
LOCAL_BUCKET_PATH_FOR_DEBUG = "/home/leon/internal-data/"

# TODO(Leon Handreke): This is really awful, figure out a better way to pass Context to another process!
DATASET_PATH = Path("datasets/ru/egrul/ru_egrul.yml")
# DATASET_PATH = Path("ru_egrul.yml")


def dicts_to_entities(context: Context, entity_dicts: Iterable[dict]) -> List[Entity]:
    # TODO(Leon Handreke): This is a bit hacky, we have to hand Entity objects across process boundaries,
    # is there a better way maybe?
    return [
        Entity.from_dict(model, d, cleaned=True, default_dataset=context.dataset)
        for d in entity_dicts
    ]


def entities_to_dicts(entities: Iterable[Entity]) -> List[dict]:
    # TODO(Leon Handreke): This is a bit hacky, we have to hand Entity objects across process boundaries,
    # is there a better way maybe?
    return [e.to_dict() for e in entities]


def crawl_archive_multiprocessing_wrapper(archive, result_queue):
    context = build_context()

    if LOCAL_BUCKET_PATH_FOR_DEBUG:
        res = crawl_local_archive(context, str(archive))
    else:
        res = crawl_remote_archive(context, str(archive))

    for x in res:
        result_queue.put(entities_to_dicts(x))


def aggregate_archives_by_date(
    archive_paths: Iterable[Path],
) -> Dict[date, Iterable[Path]]:
    archives_by_date = defaultdict(set)
    for archive_path in archive_paths:
        dirname = archive_path.parts[-2]  # [..., "dirname", "archive.zip"]
        dirname = dirname.rstrip("_FULL")
        archive_date = datetime.strptime(dirname, "%d.%m.%Y").date()
        archives_by_date[archive_date].add(archive_path)
    return archives_by_date


def crawl_archives_for_date(
    context: Context,
    archive_date: date,
    archives: Iterable[Any],
):
    """
    Crawl the archives for a date, using a previous cache DB as a base.

    Args:
        context: The context
        archive_date: The date of the archives.
        archives: The iterable of archives.
        previous_cache_db_path: A list to the previous cache DB that will be used as a base for the new one.

    Returns:
        The path of the new cache DB.

    """
    context.data_time = datetime.combine(archive_date, datetime.min.time())

    out_fh = open(
        context.get_resource_path("ownerships-" + archive_date.isoformat() + ".json"),
        "wb",
    )

    context.log.info(
        "Processing %d archives for %s" % (len(archives), archive_date.isoformat())
    )

    pool = multiprocessing.Pool(multiprocessing.cpu_count() - 2)
    manager = multiprocessing.Manager()
    result_queue = manager.Queue()

    n = 0
    res = pool.starmap_async(
        crawl_archive_multiprocessing_wrapper,
        [(str(archive), result_queue) for archive in sorted(archives)],
    )

    while not res.ready():
        try:
            entity_dict_list = result_queue.get(timeout=5)
        except multiprocessing.queues.Empty:
            continue
        out_fh.write(orjson.dumps(entity_dict_list, option=orjson.OPT_APPEND_NEWLINE))
        # entity_list = dicts_to_entities(context, result_queue.get())
        n += 1
        # for e in entity_list[1:]:
        # context.emit(e)
        if n % 10000 == 0:
            context.log.info(f"Emitted ownerships for {n} companies")

    out_fh.close()


def build_context() -> Context:
    dataset = load_dataset_from_path(DATASET_PATH)
    context = Context(dataset)
    return context


def main():
    context = build_context()
    # Left in there for debugging, if you have a local instance of the data
    if LOCAL_BUCKET_PATH_FOR_DEBUG:
        archives = [
            name
            for name in Path(LOCAL_BUCKET_PATH_FOR_DEBUG)
            .joinpath(INTERNAL_DATA_ARCHIVE_PREFIX)
            .glob("*.zip")
        ]
    else:
        pass
        # archives = [
        #     name
        #     for name in list_internal_data(INTERNAL_DATA_ARCHIVE_PREFIX)
        #     if name.endswith(".zip")
        # ]

    archives_by_date = sorted(aggregate_archives_by_date(archives).items())
    # prof.disable()
    # ps = pstats.Stats(prof, stream=sys.stdout)
    # ps.sort_stats('cumulative')
    # ps.print_stats()
    # Go through list of (date, [archives]) tuples that are sorted by date
    # For each of the dates, we apply the data in the archives for that day to the cache database
    # After we've rolled the database forward to the latest archive, we emit the entities
    for archive_date, archives in archives_by_date:
        crawl_archives_for_date(context, archive_date, archives)


if __name__ == "__main__":
    main()
