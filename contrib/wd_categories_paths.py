import logging
from collections import defaultdict
from typing import Any, Dict, Set, Tuple, cast
from urllib.parse import urlencode

import click
from zavod.context import Context
from zavod.logs import configure_logging
from zavod.meta.dataset import Dataset

IGNORE = {
    "Category:Contents",
    "Category:Commons category Wikidata tracking categories",
    "Category:Tracking categories automatically populated by MediaWiki",
    "Category:Wikipedia extended-confirmed-protected pages",
    "Category:Template Large category TOC via Automatic category TOC on category with 2,001–5,000 pages",
    "Category:Template Category TOC via Automatic category TOC on category with 901–1200 pages",
    "Category:Template Category TOC via Automatic category TOC on category with 101–200 pages",
    "Category:Template Category TOC tracking categories",
    "Category:Commons category link is on Wikidata",
    "Category:Wikipedia categories",
    "Category:Automatic category TOC generates no TOC",
    "Category:Automatic category TOC tracking categories",
    "Category:Commons category link from Wikidata",
    "Category:Container categories",
    "Category:Template Large category TOC via Automatic category TOC on category with over 20,000 pages",
    "Category:Automatic category TOC generates standard Category TOC",
    "Category:Automatic category TOC generates Large category TOC",
    "Category:Tracking categories",
    "Category:Hidden categories",
    "Category:Category series navigation cleanup",
    "Category:Template Large category TOC tracking categories",
    "Category:American people by state or territory",
    "Category:Template Category TOC via Automatic category TOC on category with 301–600 pages",
    "Category:Wikipedia category cleanup",
    "Category:Template Large category TOC via Automatic category TOC on category with 10,001–20,000 pages",
    "Category:Categories requiring diffusion",
    "Category:Template Category TOC via Automatic category TOC on category with 201–300 pages",
    "Category:Categories by country",
    "Category:Categories by location",
    "Category:Category series navigation using skip-gaps parameter",
    "Category:Categories by parameter",
    "Category:Wikipedia administration",
    "Category:Template Category TOC via Automatic category TOC on category with 601–900 pages",
    "Category:Template Large category TOC via Automatic category TOC on category with 1,201–2,000 pages",
    "Category:Category series navigation year and decade",
    "Category:Main topic classifications",
    "Category:Geography by country",
    "Category:Categories by country subdivision",
    "Category:Humans",
    "Category:Categories by continent",
    "Category:Geography by first-level administrative country subdivision",
    "Category:First-level administrative divisions by country",
    "Category:Wikipedia categories named after continents",
    "Category:Categories by type",
    "Category:Categories by occupation",
    "Category:Wikipedia categories named after country subdivisions",
}
CONSIDERED: Dict[str, int] = defaultdict(int)


def find_paths(
    context: Context,
    category_title: str,
    page_title: str,
    path: Tuple[str, ...] = (),
    max_depth: int = 10,
    cursor: Any = None,
) -> Set[Tuple[str, ...]]:
    """
    `category_title` must have `Category:` prefix and not have underscores for spaces.
    `page_title` must have underscores instead of spaces.
    """
    paths: Set[Tuple[str, ...]] = set()
    if len(path) > max_depth:
        return paths
    query = {
        "action": "query",
        "format": "json",
        "prop": "categories",
        "titles": page_title,
    }
    if cursor is not None:
        query["clcontinue"] = cursor
    url = f"https://en.wikipedia.org/w/api.php?{urlencode(query)}"
    data = context.fetch_json(url, cache_days=7)
    pageids = list(data["query"]["pages"].keys())
    assert len(pageids) == 1
    categories = data["query"]["pages"][pageids[0]]["categories"]

    for category in categories:
        if category["title"] in IGNORE:
            continue

        CONSIDERED[category["title"]] += 1

        if category_title == category["title"]:
            paths.add(path + (category_title,))

        else:
            paths.update(
                find_paths(
                    context,
                    category_title,
                    category["title"],
                    path + (category["title"],),
                    max_depth,
                )
            )
    # Paginate
    if "continue" in data:
        paths.update(
            find_paths(
                context,
                category_title,
                page_title,
                path,
                max_depth,
                cursor=data["continue"]["clcontinue"],
            )
        )

    return paths


@click.command()
@click.option("--debug", is_flag=True, default=False)
@click.argument("category_title", type=str)
@click.argument("page_title", type=str)
@click.argument("max_depth", type=int, default=3)
def cli(
    category_title: str, page_title: str, max_depth: int = 3, debug: bool = False
) -> None:
    """Find the nested category paths between a page and a category containing it.

    It's useful to understand which specific categories cause a person to be included
    in wd_categories.

    This is SLOWWWW.
    Better would be if this could be fetched using petscan
    https://github.com/magnusmanske/petscan_rs/issues/182

    CATEGORY_TITLE Without underscores, with Category:, e.g. 'Category:Political office-holders in Guatemala'
    PAGE_TITLE With underscores, e.g. 'Mariano_Paredes_(President_of_Guatemala)'
    MAX_DEPTH Maximum depth to search, default 3 (too low may miss paths, higher takes longer)
    """
    fake_dataset: Dataset = Dataset({"name": "fake"})
    context = Context(fake_dataset)
    level = logging.DEBUG if debug else logging.INFO
    configure_logging(level=level)
    paths = find_paths(context, category_title, page_title, max_depth=max_depth)
    if paths:
        for path in paths:
            print(path)
    else:
        print(
            f"No paths found within max depth {max_depth}. Check your underscores and consider going deeper."
        )
        for cat, count in sorted(CONSIDERED.items(), key=lambda x: x[1]):
            print(count, cat)


if __name__ == "__main__":
    cli()
