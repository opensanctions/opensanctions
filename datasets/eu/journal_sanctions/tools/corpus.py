"""Download every act of the covered frameworks into a local corpus.

The corpus exists to measure how often a pattern recurs across regimes
before anyone builds a shared helper for it — search it with `rg` over
`*/text.txt`. Frameworks are the keys of `config.consolidation` in the
dataset YAML; their acts come from CELLAR (`related_acts`). Each act lands
in `{corpus}/{celex}/` as written by `tools.act`; `index.json` maps each act
to the frameworks it relates to.
"""

import json
from pathlib import Path

import click
import yaml
from zavod.shed.ojeu.cellar import cli_client

from .act import fetch_act, render_text
from .common import DATASET_DIR, related_acts

CORPUS_DIR = DATASET_DIR.parents[2] / "data" / "tmp" / "eu_oj"


def frameworks() -> list[str]:
    path = DATASET_DIR / "eu_journal_sanctions.yml"
    config = yaml.safe_load(path.read_text())["config"]
    return sorted(config["consolidation"])


@click.command()
@click.option("--framework", "selected", multiple=True, help="Limit to these.")
@click.option("--dir", "directory", type=click.Path(path_type=Path), default=CORPUS_DIR)
@click.option("--rerender", is_flag=True, help="Only rewrite text.txt from body.html.")
def main(selected: tuple[str, ...], directory: Path, rerender: bool) -> None:
    """Fetch the framework acts and all their related acts."""
    if rerender:
        broken: list[str] = []
        for body in sorted(directory.glob("*/body.html")):
            try:
                render_text(body.parent)
            except ValueError as exc:
                broken.append(body.parent.name)
                click.echo(str(exc), err=True)
        click.echo(f"re-rendered, {len(broken)} failed: {broken}")
        return
    index: dict[str, dict[str, list[str]]] = {}
    for framework in selected or frameworks():
        index.setdefault(framework, {"frameworks": [], "relations": []})
        index[framework]["frameworks"].append(framework)
        index[framework]["relations"].append("framework")
        for act in related_acts(framework):
            entry = index.setdefault(act.celex, {"frameworks": [], "relations": []})
            entry["frameworks"].append(framework)
            entry["relations"].append(act.relation)
        click.echo(f"{framework}: {len(index)} acts listed so far", err=True)
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "index.json").write_text(json.dumps(index, indent=2))
    failed: list[str] = []
    with cli_client() as client:
        for number, celex in enumerate(sorted(index), start=1):
            target = directory / celex
            if (target / "meta.json").exists():
                continue
            try:
                fetch_act(client, celex, target)
            except Exception as exc:  # noqa: BLE001 - logged, the loop goes on
                failed.append(celex)
                click.echo(f"FAILED {celex}: {exc}", err=True)
            if number % 50 == 0:
                click.echo(f"{number}/{len(index)}", err=True)
    click.echo(f"{len(index)} acts, {len(failed)} failed: {failed}")


if __name__ == "__main__":
    main()
