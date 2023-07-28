from requests.exceptions import RequestException
from datapatch import LookupException

from zavod.meta import Dataset
from zavod.context import Context
from zavod.exc import RunFailedException
from zavod.runtime.stats import ContextStats
from zavod.runtime.loader import load_entry_point
from zavod.runner.enrich import dataset_enricher

# HACK: Importing the enrich module in the test avoids a segfault otherwise happening
# on OS X, probably related to the nested use of import_module.
assert dataset_enricher is not None


def run_dataset(dataset: Dataset, dry_run: bool = False) -> ContextStats:
    """Load the dataset entry point, configure a context, and then execute the entry
    point; finally disband the context."""
    context = Context(dataset, dry_run=dry_run)
    if dataset.disabled:
        context.log.info("Source is disabled", dataset=dataset.name)
        return context.stats

    try:
        context.begin(clear=True)
        context.log.info("Begin runner")
        entry_point = load_entry_point(dataset)
        entry_point(context)
        if context.stats.entities == 0:
            context.log.warn(
                "Runner did not emit entities",
                statements=context.stats.statements,
            )
        context.log.info(
            "Run completed",
            entities=context.stats.entities,
            statements=context.stats.statements,
        )
        return context.stats
    except KeyboardInterrupt as kint:
        context.log.warning("Aborted by user (SIGINT)")
        raise RunFailedException() from kint
    except LookupException as lexc:
        context.log.error(lexc.message, lookup=lexc.lookup.name, value=lexc.value)
        raise RunFailedException() from lexc
    except RequestException as rexc:
        resp = repr(rexc.response)
        context.log.error(str(rexc), url=rexc.request.url, response=resp)
        raise RunFailedException() from rexc
    except Exception as exc:
        context.log.exception("Runner failed: %s" % str(exc))
        raise RunFailedException() from exc
    finally:
        context.close()


# def clear_dataset(dataset: Dataset, data: bool = True) -> None:
#     """Delete all recorded data for a given dataset."""
#     context = Context(dataset, dry_run=False)
#     try:
#         context.issues.clear()
#         context.resources.clear()
#         if data:
#             context.cache.clear()
#             context.sink.clear()
#     finally:
#         context.close()
