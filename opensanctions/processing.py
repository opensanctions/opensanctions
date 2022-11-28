from typing import List
from concurrent.futures import Future, wait, ThreadPoolExecutor

from opensanctions import settings
from opensanctions.core import Dataset, Context
from opensanctions.core.resolver import get_resolver
from opensanctions.exporters import export_metadata, export_dataset
from opensanctions.core.loader import Database


def _compute_futures(futures: List[Future]):
    wait(futures)
    for future in futures:
        future.result()


def run_export(
    scope_name: str,
    threads: int = settings.THREADS,
) -> None:
    """Export dump files for all datasets in the given scope."""
    scope = Dataset.require(scope_name)
    resolver = get_resolver()
    database = Database(scope, resolver, cached=True)
    database.view(scope)
    for dataset_ in scope.datasets:
        export_dataset(dataset_, database)
    export_metadata()
    # with ThreadPoolExecutor(max_workers=threads) as executor:
    #     futures: List[Future] = []
    #     futures = []
    #     for dataset_ in scope.datasets:
    #         futures.append(executor.submit(export_dataset, dataset_, database))
    #     futures.append(executor.submit(export_metadata))
    #     _compute_futures(futures)


def run_enrich(scope_name: str, external_name: str, threshold: float):
    scope = Dataset.require(scope_name)
    external = Dataset.require(external_name)
    ctx = Context(external)
    resolver = get_resolver()
    database = Database(scope, resolver, cached=False)
    loader = database.view(scope)
    ctx.enrich(resolver, loader, threshold=threshold)
    resolver.save()
