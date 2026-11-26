"""The op-log tools, as one command: `python -m tools <command>`.

Run from the dataset directory. See ../AGENTS.md for the per-act loop.
"""

import runpy
import sys
from pathlib import Path

import click

from .act import main as act
from .check import check_command, compare_command
from .corpus import main as corpus
from .lookup import entry, resolve
from .migrate import main as migrate
from .replay import main as replay


@click.group()
def cli() -> None:
    """Fetch acts, look up entries, replay and check EU sanctions op-logs."""


@cli.command("run", context_settings={"ignore_unknown_options": True})
@click.argument("script", type=click.Path(exists=True, path_type=Path))
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def run(script: Path, args: tuple[str, ...]) -> None:
    """Run a throwaway SCRIPT with the tools importable (`from tools.common …`)."""
    sys.argv = [str(script), *args]
    runpy.run_path(str(script), run_name="__main__")


cli.add_command(act, "act")
cli.add_command(entry)
cli.add_command(resolve)
cli.add_command(check_command)
cli.add_command(compare_command)
cli.add_command(replay, "replay")
cli.add_command(migrate, "migrate")
cli.add_command(corpus, "corpus")

if __name__ == "__main__":
    cli()
