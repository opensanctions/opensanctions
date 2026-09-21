import shutil

from click.testing import CliRunner
from followthemoney.dataset import VersionHistory
from nomenklatura.db import make_session

from zavod import settings
from zavod.archive import ARTIFACTS, VERSIONS_FILE, dataset_artifact_directory
from zavod.archive import dataset_state_path
from zavod.cli import cli
from zavod.integration import get_resolver
from zavod.meta import Dataset
from zavod.tests.conftest import DATASET_1_YML, DATASET_3_YML


def _read_history(dataset_name: str) -> VersionHistory | None:
    fn = settings.ARCHIVE_PATH / ARTIFACTS / dataset_name / VERSIONS_FILE
    if not fn.exists():
        return None
    return VersionHistory.from_json(fn.read_text())


def test_crawl_dataset():
    runner = CliRunner()
    result = runner.invoke(cli, ["crawl", "/dev/null"])
    path = settings.DATA_PATH / "datasets" / "testdataset1"
    assert result.exit_code != 0, result.output
    assert not path.exists()
    result = runner.invoke(cli, ["crawl", DATASET_1_YML.as_posix()])
    assert result.exit_code == 0, result.output
    assert path.exists()

    result = runner.invoke(cli, ["clear", "/dev/null"])
    assert path.exists()
    result = runner.invoke(cli, ["clear", DATASET_1_YML.as_posix()])
    assert not path.exists()


def test_export_dataset():
    version = settings.RUN_VERSION
    runner = CliRunner()
    result = runner.invoke(cli, ["export", "/dev/null", version.id])
    assert result.exit_code != 0, result.output
    # The version argument is required:
    result = runner.invoke(cli, ["export", DATASET_1_YML.as_posix()])
    assert result.exit_code != 0, result.output
    # Exporting a run that was never crawled fails:
    result = runner.invoke(cli, ["export", DATASET_1_YML.as_posix(), version.id])
    assert result.exit_code != 0, result.output

    result = runner.invoke(cli, ["crawl", DATASET_1_YML.as_posix()])
    assert result.exit_code == 0, result.output
    result = runner.invoke(cli, ["export", DATASET_1_YML.as_posix(), version.id])
    assert result.exit_code == 0, result.output
    shutil.rmtree(settings.DATA_PATH)


def test_export_validation_failed(testdataset3: Dataset):
    version = settings.RUN_VERSION
    artifact_dir = dataset_artifact_directory(testdataset3.name, version)
    runner = CliRunner()
    result = runner.invoke(cli, ["crawl", DATASET_3_YML.as_posix()])
    assert result.exit_code == 0, result.output

    # Validation is on by default and testdataset3 fails its min assertions.
    result = runner.invoke(cli, ["export", DATASET_3_YML.as_posix(), version.id])
    assert result.exit_code != 0, result.output
    assert "Assertion countries failed" in result.output, result.output
    # Partial export files may remain in the artifact directory, but the abort
    # must not produce any success markers: exporters never finish, so nothing
    # is registered as a resource and no success index is written. Keeping the
    # partial files out of the archive is archive_failure's job.
    assert not (artifact_dir / "statistics.json").exists()
    assert not (artifact_dir / "index.json").exists()
    resources_path = artifact_dir / "resources.json"
    if resources_path.exists():
        assert "entities.ftm.json" not in resources_path.read_text()

    result = runner.invoke(
        cli, ["export", "--no-validate", DATASET_3_YML.as_posix(), version.id]
    )
    assert result.exit_code == 0, result.output
    assert (artifact_dir / "entities.ftm.json").exists()
    assert (artifact_dir / "index.json").exists()
    shutil.rmtree(settings.DATA_PATH)


def test_load_db():
    runner = CliRunner()
    result = runner.invoke(cli, ["load-db", "/dev/null"])
    assert result.exit_code != 0, result.output
    result = runner.invoke(cli, ["crawl", DATASET_1_YML.as_posix()])
    assert result.exit_code == 0, result.output
    result = runner.invoke(cli, ["load-db", DATASET_1_YML.as_posix()])
    assert result.exit_code == 0, result.output


def test_dump_file():
    runner = CliRunner()
    out_path = dataset_state_path("x") / "out.csv"
    result = runner.invoke(cli, ["dump-file", "/dev/null", out_path.as_posix()])
    assert result.exit_code != 0, result.output
    result = runner.invoke(cli, ["crawl", DATASET_1_YML.as_posix()])
    assert result.exit_code == 0, result.output
    result = runner.invoke(
        cli, ["dump-file", DATASET_1_YML.as_posix(), out_path.as_posix()]
    )
    assert result.exit_code == 0, result.output
    shutil.rmtree(settings.DATA_PATH)


def test_run_publish_dataset(testdataset1: Dataset):
    artifacts_path = (
        settings.ARCHIVE_PATH / ARTIFACTS / testdataset1.name / settings.RUN_VERSION.id
    )
    assert not artifacts_path.exists()
    runner = CliRunner()
    # zavod run
    result = runner.invoke(cli, ["run", "/dev/null"])
    assert result.exit_code != 0, result.output
    result = runner.invoke(cli, ["run", DATASET_1_YML.as_posix()])
    assert result.exit_code == 0, result.output
    assert artifacts_path.joinpath("index.json").exists()
    assert artifacts_path.joinpath("entities.ftm.json").exists()
    # Warning issues in a published run are published
    with open(artifacts_path / "issues.json") as f:
        assert "This is a test warning" in f.read()
    shutil.rmtree(artifacts_path)

    # zavod publish
    assert not artifacts_path.exists()
    result = runner.invoke(cli, ["publish", "/dev/null", settings.RUN_VERSION.id])
    assert result.exit_code != 0, result.output
    # The version argument is required:
    result = runner.invoke(cli, ["publish", DATASET_1_YML.as_posix()])
    assert result.exit_code != 0, result.output
    result = runner.invoke(
        cli, ["publish", DATASET_1_YML.as_posix(), settings.RUN_VERSION.id]
    )
    assert result.exit_code == 0, result.output
    assert artifacts_path.joinpath("index.json").exists()
    assert artifacts_path.joinpath("entities.ftm.json").exists()


def test_run_validation_failed(testdataset3: Dataset):
    artifacts_path = (
        settings.ARCHIVE_PATH / ARTIFACTS / testdataset3.name / settings.RUN_VERSION.id
    )
    assert not (artifacts_path / "issues.json").exists()
    runner = CliRunner()
    result = runner.invoke(cli, ["run", DATASET_3_YML.as_posix()])
    assert result.exit_code != 0, result.output
    # Validation issues in an aborted run are published
    assert "Assertion countries failed" in result.output, result.output
    with open(artifacts_path / "issues.json") as f:
        assert "Assertion countries failed" in f.read()
    # Only failure information is archived - never partial export artifacts,
    # even though the abort happened mid-export.
    archived = {p.name for p in artifacts_path.iterdir()}
    assert archived == {
        "index.json",
        "issues.json",
        "issues.log",
        "versions.json",
        "manifest.json",
    }
    shutil.rmtree(settings.DATA_PATH)


def test_run_update_last_successful_version(
    testdataset3: Dataset, testdataset1: Dataset
):
    runner = CliRunner()

    # testdataset3 has validation errors: the failed version is registered in
    # the history, but last_successful is never set.
    result = runner.invoke(cli, ["run", DATASET_3_YML.as_posix()])
    assert result.exit_code != 0, result.output
    history = _read_history(testdataset3.name)
    assert history is not None
    assert history.latest == settings.RUN_VERSION
    assert history.last_successful is None

    # testdataset1 succeeds, so last_successful should be set
    result = runner.invoke(cli, ["run", DATASET_1_YML.as_posix()])
    assert result.exit_code == 0, result.output
    history = _read_history(testdataset1.name)
    assert history is not None
    assert history.last_successful is not None
    assert history.last_successful == settings.RUN_VERSION
    shutil.rmtree(settings.DATA_PATH)


def test_xref_dataset(testdataset1: Dataset):
    runner = CliRunner()
    env = {}

    result = runner.invoke(cli, ["crawl", DATASET_1_YML.as_posix()], env=env)
    assert result.exit_code == 0, result.output

    with make_session() as session:
        resolver = get_resolver(session)
        resolver.load_into_memory()
        assert list(resolver.get_candidates()) == []
        assert list(resolver.get_judgements()) == []

    result = runner.invoke(
        cli, ["xref", "--rebuild-store", DATASET_1_YML.as_posix()], env=env
    )
    assert result.exit_code == 0, result.output

    with make_session() as session:
        resolver = get_resolver(session)
        resolver.load_into_memory()
        assert len(list(resolver.get_candidates())) > 1

    result = runner.invoke(cli, ["resolver-prune"], env=env)
    assert result.exit_code == 0, result.output

    with make_session() as session:
        resolver = get_resolver(session)
        resolver.load_into_memory()
        assert list(resolver.get_candidates()) == []
        assert list(resolver.get_judgements()) == []
