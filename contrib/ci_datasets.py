import click
import re
from pathlib import Path
from typing import Set, Tuple
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
    dataset_paths: Set[Path] = set()
    for path in file_paths:
        if re.match(r"\.ya?ml", path.suffix):
            dataset_paths.add(path)
        else:
            for yml_path in glob("*.y*ml", root_dir=path.parent):
                dataset_paths.add(path.parent.joinpath(yml_path))

    for path in dataset_paths:
        dataset = load_dataset_from_path(path)
        path_name = path.as_posix()
        if dataset is None:
            raise click.BadParameter("Invalid dataset path: %s" % path_name)
        if dataset.model.ci_test:
            print(path_name)


if __name__ == "__main__":
    main()
