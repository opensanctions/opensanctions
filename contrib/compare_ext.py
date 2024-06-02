from pathlib import Path
import shutil
import click
from nomenklatura.statement import Statement
from nomenklatura.resolver import Linker
from nomenklatura.store.base import View as BaseView
from tempfile import mkdtemp
from followthemoney.cli.util import InPath
import json

from zavod.archive import StatementGen, _read_fh_statements
from zavod.dedupe import get_dataset_resolver
from zavod.logs import get_logger
from zavod.entity import Entity
from zavod.meta import Dataset, get_catalog
from zavod.store import Store as BaseStore

log = get_logger(__name__)
View = BaseView[Dataset, Entity]
Dir = click.Path(dir_okay=True, readable=True, path_type=Path, allow_dash=True)


class Store(BaseStore):
    def __init__(
        self,
        name: str,
        path: Path,
        statements_path: Path,
    ):
        metadata = {
            "name": name,
            "title": name,
            "summary": "Synthetic, ad-hoc virtual collection for testing ladi da",
            "hidden": True,
        }
        catalog = get_catalog()
        dataset = Dataset(catalog, metadata)
        resolver = get_dataset_resolver(dataset)
        linker: Linker[Entity] = resolver.get_linker()
        super().__init__(dataset, linker, path)
        self.entity_class = Entity
        self.statements_path = statements_path

    def iter_dataset_statements(self, external: bool = False) -> StatementGen:
        with open(self.statements_path, "r") as fh:
            yield from _read_fh_statements(fh, external)
        return

    def build(self, external: bool = False) -> None:
        log.info("Building local LevelDB aggregator...", scope=self.dataset.name)
        idx = 0
        with self.writer() as writer:
            stmts = self.iter_dataset_statements(external=external)
            for idx, stmt in enumerate(stmts):
                if idx > 0 and idx % 100000 == 0:
                    log.info(
                        "Indexing aggregator...",
                        statements=idx,
                        scope=self.dataset.name,
                    )
                stmt.dataset = self.dataset.name
                writer.add_statement(stmt)
        log.info("Local cache complete.", scope=self.dataset.name, statements=idx)


def dump_dict(filename, dict_):
    with open(filename, "w") as f:
        for entity in dict_.values():
            f.write(json.dumps(entity.to_dict()) + "\n")


@click.option("--clear", is_flag=True, type=bool, default=False)
@click.argument("working_dir", type=Dir)
@click.argument("b_path", type=Dir)
@click.argument("a_path", type=Dir)
@click.command()
def main(a_path: Path, b_path: Path, working_dir: Path, clear: bool = False):
    if clear:
        shutil.rmtree(working_dir, ignore_errors=True)
    a_external_store_path = working_dir / "a.external"
    b_external_store_path = working_dir / "b.external"
    if clear:
        shutil.rmtree(working_dir, ignore_errors=True)
    working_dir.mkdir(parents=True, exist_ok=True)

    if a_external_store_path.is_dir():
        a_external_store = Store("a", a_external_store_path, a_path)
    else:
        a_external_store = Store("a", a_external_store_path, a_path)
        a_external_store.build(external=True)
    if b_external_store_path.is_dir():
        b_external_store = Store("b", b_external_store_path, b_path)
    else:
        b_external_store = Store("b", b_external_store_path, b_path)
        b_external_store.build(external=True)

    a_internals = dict()
    a_externals = dict()
    b_internals = dict()
    b_externals = dict()

    for entity in a_external_store.default_view(external=False).entities():
        a_internals[entity.id] = entity
    for entity in a_external_store.default_view(external=True).entities():
        a_externals[entity.id] = entity
    for entity in b_external_store.default_view(external=False).entities():
        b_internals[entity.id] = entity
    for entity in b_external_store.default_view(external=True).entities():
        b_externals[entity.id] = entity

    log.info(f"a.internals: {len(a_internals)}")
    log.info(f"a.externals: {len(a_externals)}")
    log.info(f"b.internals: {len(b_internals)}")
    log.info(f"b.externals: {len(b_externals)}")

    in_a_not_b_internals = dict(a_internals.items() - b_internals.items())
    dump_dict("in_a_not_b_internals.jsonlines", in_a_not_b_internals)
    print(f"in_a_not_b_internals: {len(in_a_not_b_internals)}")

    in_b_not_a_internals = dict(b_internals.items() - a_internals.items())
    dump_dict("in_b_not_a_internals.jsonlines", in_b_not_a_internals)
    print(f"in_b_not_a_internals: {len(in_b_not_a_internals)}")

    in_a_not_b_externals = dict(a_externals.items() - b_externals.items())
    dump_dict("in_a_not_b_externals.jsonlines", in_a_not_b_externals)
    print(f"in_a_not_b_externals: {len(in_a_not_b_externals)}")

    in_b_not_a_externals = dict(b_externals.items() - a_externals.items())
    dump_dict("in_b_not_a_externals.jsonlines", in_b_not_a_externals)
    print(f"in_b_not_a_externals: {len(in_b_not_a_externals)}")


if __name__ == "__main__":
    main()
