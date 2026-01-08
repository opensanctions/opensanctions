from requests.exceptions import RequestException
from datapatch import LookupException

from zavod import settings
from zavod.meta import Dataset
from zavod.context import Context
from zavod.exc import RunFailedException
from zavod.archive import dataset_data_path
from zavod.runtime.stats import ContextStats
from zavod.runtime.loader import load_entry_point
from zavod.runner.enrich import enrich
from zavod.reset import reset_caches

# HACK: Importing the enrich module in the test avoids a segfault otherwise happening
# on OS X, probably related to the nested use of import_module.
assert enrich is not None


def crawl_dataset(dataset: Dataset, dry_run: bool = False) -> ContextStats:
    """Load the dataset entry point, configure a context, and then execute the entry
    point; finally disband the context."""
    context = Context(dataset, dry_run=dry_run)
    if dataset.model.disabled:
        context.log.info("Source is disabled", source=dataset.name)
        return context.stats

    try:
        context.begin(clear=True)
        context.log.info(
            "Running dataset",
            data_path=dataset_data_path(dataset.name),
            data_time=settings.RUN_TIME_ISO,
            version=context.version.id,
        )
        entry_point = load_entry_point(dataset)
        entry_point(context)
        context.flush()
        context.log.info(
            "Run completed",
            entities=context.stats.entities,
            statements=context.stats.statements,
            changed=context.stats.changed,
        )
        if settings.DEBUG:
            context.debug_lookups()
        return context.stats
    except (SystemExit, KeyboardInterrupt) as kint:
        context.log.warning("Interrupted")
        raise RunFailedException() from kint
    except LookupException as lexc:
        context.log.error(lexc.message, lookup=lexc.lookup.name, value=lexc.value)
        raise RunFailedException() from lexc
    except RequestException as rexc:
        url = rexc.request.url if rexc.request else None
        if rexc.response is not None:
            context.log.exception(
                f"Runner failed with {type(rexc).__name__} on {url}",
                error_str=str(rexc),
                url=url,
                response_code=rexc.response.status_code,
                response_text=rexc.response.text,
            )
        else:
            context.log.exception(
                f"Runner failed with {type(rexc).__name__}",
                error_str=str(rexc),
                url=url,
            )
        raise RunFailedException() from rexc
    except Exception as exc:
        context.log.exception("Runner failed: %s" % str(exc))
        raise RunFailedException() from exc
    finally:
        context.close()
        reset_caches()
