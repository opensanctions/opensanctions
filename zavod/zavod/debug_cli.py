from click.testing import CliRunner
import sys
from zavod import cli

# debugpy does not like running zavod.cli directly,
# so we invoke the commands here to allow for debugging of crawlers
if __name__ == "__main__":
    runner = CliRunner()
    f = getattr(cli, sys.argv[1])
    result = runner.invoke(f, sys.argv[2:])
