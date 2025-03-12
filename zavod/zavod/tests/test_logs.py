import logging
import os

import zavod
from zavod import Context, Dataset
from zavod.archive import dataset_resource_path, ISSUES_FILE
from zavod.logs import (
    RedactingProcessor,
    configure_redactor,
)


def test_redact_str():
    processor = RedactingProcessor({"DEADBEEF": "AAA", "BEEFDEAD": "BBB"})
    assert (
        processor.redact_str("aaa DEADBEEF bbb BEEFDEAD ccc DEADBEEF ddd")
        == "aaa AAA bbb BBB ccc AAA ddd"
    )


def test_redact_list():
    processor = RedactingProcessor({"DEADBEEF": "### Redacted ###"})
    test_list = ["DEADBEEF", "not sensitive", "DEADBEEF"]
    processor.redact_list(test_list)
    assert test_list == [
        "### Redacted ###",
        "not sensitive",
        "### Redacted ###",
    ]


def test_redact_dict():
    processor = RedactingProcessor({"DEADBEEF": "### Redacted ###"})

    test_dict = {"sensitive": "aaa DEADBEEF zzz", "not_sensitive": "not sensitive"}
    processor.redact_dict(test_dict)
    assert test_dict == {
        "sensitive": "aaa ### Redacted ### zzz",
        "not_sensitive": "not sensitive",
    }


def test_redactor():
    processor = RedactingProcessor({"DEADBEEF": "### Redacted ###"})
    test_dict = {
        "event": "A thing happened with DEADBEEF",
        "things": ["DEADBEEF", "not sensitive", {"api_key": "DEADBEEF", "a": "b"}],
        "keyed_things": {"password": "DEADBEEF", "not_sensitive": "not sensitive"},
    }
    redacted_dict = processor("fake_logger", "warning", test_dict)
    assert redacted_dict == {
        "event": "A thing happened with ### Redacted ###",
        "things": [
            "### Redacted ###",
            "not sensitive",
            {"api_key": "### Redacted ###", "a": "b"},
        ],
        "keyed_things": {
            "password": "### Redacted ###",
            "not_sensitive": "not sensitive",
        },
    }


def test_configure_redactor():
    os.environ["SENSITIVE"] = "DEADBEEF"
    processor = configure_redactor()

    url_redacted = processor.redact_str(
        "something something postgres+special://user:password@localhost/db something"
    )
    assert (
        url_redacted
        == "something something postgres+special://***:***@localhost/db something"
    )

    env_redacted = processor.redact_str("something something DEADBEEF something")
    assert env_redacted == "something something ${SENSITIVE} something"


def test_redacts_issue_logger(testdataset1: Dataset):
    os.environ["SENSITIVE_SECRET"] = "correcthorsebatterystaple"

    zavod.logs.configure_logging()
    issues_path = dataset_resource_path(testdataset1.name, ISSUES_FILE)
    context = Context(testdataset1)
    context.begin(clear=True)
    assert not issues_path.exists()

    context.log.warn("This is a warning to correcthorsebatterystaple")
    # Non-structlog logs take a slightly different path
    logging.warning("This is a python logging warning to correcthorsebatterystaple")
    context.close()

    assert issues_path.exists()
    assert "This is a warning to" in issues_path.read_text()
    # assert "This is a python logging warning to" in issues_path.read_text()
    assert "correcthorsebatterystaple" not in issues_path.read_text()
