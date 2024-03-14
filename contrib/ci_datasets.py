import click
from pathlib import Path
from typing import List, Tuple

from zavod.meta import load_dataset_from_path
from followthemoney.cli.util import InPath


@click.command()
@click.argument("dataset_paths", type=InPath, nargs=-1)
def main(dataset_paths: Tuple[Path]):
    for path in dataset_paths:
        dataset = load_dataset_from_path(path)
        if dataset is None:
            raise click.BadParameter("Invalid dataset path: %s" % path)
        if dataset.ci_test:
            print(path.as_posix())
    

if __name__ == '__main__':
    main()