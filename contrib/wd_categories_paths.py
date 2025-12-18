import logging
from typing import Tuple
from urllib.parse import urlencode

import click
from zavod.context import Context
from zavod.logs import configure_logging
from zavod.meta.dataset import Dataset


def petscan(context: Context, category_title: str, language: str, depth: int) -> str:
    petscan_query = {
        "doit": "",
        "depth": depth,
        "format": "csv",
        "wikidata_item": "with",
        "wikidata_prop_item_use": "Q5",
        "search_max_results": 1000,
        "sortorder": "ascending",
        "categories": category_title,
        "language": language,
    }
    petscan_url = f"https://petscan.wmcloud.org/?{urlencode(petscan_query)}"
    text = context.fetch_text(petscan_url, cache_days=7)
    assert text is not None
    return text


def find_paths_downwards(
    context: Context,
    category_title: str,
    qid: str,
    language: str,
    max_depth: int,
    path: Tuple[str, ...] = (),
) -> None:
    path = path + (category_title,)
    cursor = None
    url = None
    query = {
        "action": "query",
        "list": "categorymembers",
        "cmtitle": category_title,
        "cmtype": "subcat",
        "format": "json",
    }
    # Prevents getting stuck in circular references
    if max_depth < 1:
        print(". " * len(path), f"Reached max depth in {category_title}")
        return

    while cursor is not None or url is None:
        if cursor is not None:
            query["cmcontinue"] = cursor
        url = f"https://{language}.wikipedia.org/w/api.php?{urlencode(query)}"

        subcategories_result = context.fetch_json(url, cache_days=7)
        if "continue" in subcategories_result:
            cursor = subcategories_result["continue"]["cmcontinue"]
        else:
            cursor = None

        for cat in subcategories_result["query"]["categorymembers"]:
            # "Category" prefix is in the wiki language
            subcategory_title = cat["title"].split(":", 1)[1].replace(" ", "_")
            print(". " * len(path), subcategory_title)
            petscan_response = petscan(context, subcategory_title, language, max_depth)
            if qid in petscan_response:
                print(". " * len(path), f"!!! Found {qid} in {subcategory_title} !!!")
                find_paths_downwards(
                    context, cat["title"], qid, language, max_depth - 1, path
                )


@click.command()
@click.option("--debug", is_flag=True, default=False)
@click.argument("category_title", type=str)
@click.argument("qid", type=str)
@click.argument("language", type=str)
@click.argument("max_depth", type=int, default=3)
def cli(
    category_title: str,
    qid: str,
    language: str,
    max_depth: int = 3,
    debug: bool = False,
) -> None:
    """Find the nested category paths between a page and a category containing it.

    It's useful to understand which specific categories cause a person to be included
    in wd_categories.

    This is SLOWWWW.
    Better would be if this could be fetched using petscan
    https://github.com/magnusmanske/petscan_rs/issues/182

    CATEGORY_TITLE - Without underscores, with Category:, e.g. 'Category:Political office-holders in Guatemala'
    QID - The QID of the person, e.g. Q5234712
    MAX_DEPTH - Use the depth used by the relevant category configuration in wd_categories.yml
    """

    fake_dataset: Dataset = Dataset({"name": "fake"})
    context = Context(fake_dataset)
    level = logging.DEBUG if debug else logging.INFO
    configure_logging(level=level)

    find_paths_downwards(context, category_title, qid, language, max_depth)


if __name__ == "__main__":
    cli()
