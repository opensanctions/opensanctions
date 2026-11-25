"""Print the `id` for a designation's first ADD row."""

import click

from .common import mint_id


@click.command()
@click.option("--framework", required=True, help="Framework act CELEX.")
@click.option("--record-id", default="", help="Printed entry number, if any.")
@click.option("--name", required=True, help="The name as first listed.")
def main(framework: str, record_id: str, name: str) -> None:
    click.echo(mint_id(framework, record_id, name))


if __name__ == "__main__":
    main()
