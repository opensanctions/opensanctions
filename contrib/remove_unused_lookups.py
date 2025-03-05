"""
This script removes unused options from a lookup in a dataset file.

The list of unused lookups needs to be generated after a crawl using a snippet like this:

    unused_options = {
        name: [
            option.regex.pattern
            for option in lookup_config.options
            if option.ref_count == 0
        ]
        for name, lookup_config in context.dataset.lookups.items()
    }
    Path("unused_options.json").write_bytes(orjson.dumps(unused_options))

Unfortunately, the YAML load/dump doesn't leave formatting completely intact, so you'll probably want to normalize it
once before you remove unused options to be able to properly see what it does

    $ python contrib/remove_unused_lookups.py --dataset-path dataset.yaml
    $ # git commit the changed file
    $ python contrib/remove_unused_lookups.py --dataset-path dataset.yaml --unused-options-path unused_options.json
    $ # git diff to inspect changes
"""

import copy
import json
from pathlib import Path

import click

import ruamel.yaml
from datapatch import Lookup
from datapatch.option import Option

OptionTypePath = click.Path(dir_okay=False, path_type=Path)


@click.command()
@click.option("--dataset-path", type=OptionTypePath, required=True)
@click.option("--unused-options-path", type=OptionTypePath, required=False)
@click.option("--lookup-name", type=str, required=False)
def main(dataset_path: Path, unused_options_path: Path, lookup_name: str):
    yaml = ruamel.yaml.YAML(typ="rt")
    yaml.width = 10000
    yaml.indent(mapping=2, sequence=4, offset=2)
    yaml.preserve_quotes = True
    yaml.default_flow_style = False

    doc = yaml.load(dataset_path)

    if unused_options_path:
        unused_options = json.loads(unused_options_path.read_text())
        lookup_names = [lookup_name] if lookup_name else unused_options.keys()

        for lookup_name in lookup_names:
            lookup_config = doc["lookups"][lookup_name]
            lookup = Lookup(lookup_name, copy.deepcopy(lookup_config))
            for option_config in lookup_config["options"]:
                option = Option(lookup, copy.deepcopy(option_config))
                if option.regex.pattern in unused_options:
                    print("remove")
                    lookup_config["options"].remove(option_config)

    yaml.dump(doc, dataset_path)


if __name__ == "__main__":
    main()
