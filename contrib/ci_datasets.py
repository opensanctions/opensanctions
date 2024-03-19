import click
import re
from pathlib import Path
from typing import List, Tuple
from glob import glob

from zavod.meta import load_dataset_from_path
from followthemoney.cli.util import InPath


@click.command()
@click.argument("file_paths", type=InPath, nargs=-1)
def main(file_paths: Tuple[Path]):
    """
    Takes a list of file paths and outputs the paths of dataset yaml files
    whose ci_test flag is not False.
    """
    dataset_paths = set()
    for path in file_paths:
        if re.match(r"\.ya?ml", path.suffix):
            dataset_paths.add(str(path))
        else:
            dir = path.parent
            yamls = glob(str(dir / "*.y*ml"))
            dataset_paths.update(yamls)

    for path in dataset_paths:
        dataset = load_dataset_from_path(Path(path))
        if dataset is None:
            raise click.BadParameter("Invalid dataset path: %s" % path)
        if dataset.ci_test:
            print(path)


if __name__ == "__main__":
    main()
