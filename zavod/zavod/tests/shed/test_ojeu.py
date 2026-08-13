import json
from pathlib import Path

import pytest
import requests

from zavod.shed.ojeu import cellar, celex

FIXTURES = Path(__file__).parent / "fixtures" / "ojeu"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("32026R1708", "32026R1708"),
        ("celex:32026r1708", "32026R1708"),
        (
            "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A32026R1708",
            "32026R1708",
        ),
        ("http://data.europa.eu/eli/reg_impl/2026/1708/oj", "32026R1708"),
        (
            "https://eur-lex.europa.eu/eli/reg/2014/833/2024-06-25",
            "02014R0833-20240625",
        ),
    ],
)
def test_normalize_celex(value: str, expected: str) -> None:
    assert celex.normalize(value) == expected


def test_normalize_celex_rejects_unknown_values() -> None:
    with pytest.raises(ValueError, match="Invalid or unsupported"):
        celex.normalize("not-a-celex")
    with pytest.raises(ValueError, match="does not contain"):
        celex.normalize("https://example.com/no-identifier")


def test_parse_act_results() -> None:
    result = json.loads((FIXTURES / "act.json").read_text())
    act = cellar.parse_act_results("32026R1708", result)

    assert act.celex == "32026R1708"
    assert act.resource_type == "REG_IMPL"
    assert act.amends == ("32024R1485",)
    assert act.based_on == ("12012E/TXT", "32024R1485")
    assert act.latest_consolidated == "02024R1485-20260713"
    assert act.to_dict()["eli_url"].endswith("/eli/reg_impl/2026/1708/oj")
    assert act.request.data == cellar.build_act_query("32026R1708")


def test_framework_results_include_own_consolidated_family() -> None:
    result = json.loads((FIXTURES / "framework.json").read_text())
    act = cellar.parse_act_results("32024R1485", result)

    assert act.celex == "32024R1485"
    assert act.resource_type == "REG"
    assert act.amends == ()
    assert act.consolidated == (
        "02024R1485-20260701",
        "02024R1485-20260713",
    )
    assert act.latest_consolidated == "02024R1485-20260713"
    query = cellar.build_act_query("32024R1485").decode("utf-8")
    assert "BIND(COALESCE(?amended_framework, ?work) AS ?framework)" in query


def test_fetch_expression_http(requests_mock) -> None:
    content = (FIXTURES / "expression.xhtml").read_bytes()
    requests_mock.get(
        cellar.cellar_url("32026R1708"),
        content=content,
        headers={"Content-Type": "application/xhtml+xml; charset=utf-8"},
    )

    expression = cellar.fetch_expression_http("32026R1708")

    assert expression.content == content
    assert expression.media_type == "application/xhtml+xml"
    assert expression.to_dict()["bytes"] == len(content)
    assert len(expression.sha256) == 64
    assert requests_mock.last_request.headers["Accept-Language"] == "eng"


def test_context_adapters_share_requests_and_parsers() -> None:
    result = json.loads((FIXTURES / "act.json").read_text())
    content = (FIXTURES / "expression.xhtml").read_text()

    class FakeContext:
        json_call = None
        text_call = None

        def fetch_json(self, url, **kwargs):
            self.json_call = (url, kwargs)
            return result

        def fetch_text(self, url, **kwargs):
            self.text_call = (url, kwargs)
            return content

    context = FakeContext()
    act = cellar.query_act(context, "32026R1708")  # type: ignore[arg-type]
    expression = cellar.fetch_expression(context, "32026R1708")  # type: ignore[arg-type]

    assert act.latest_consolidated == "02024R1485-20260713"
    assert expression.content == content.encode("utf-8")
    assert context.json_call[1]["data"] == act.request.data
    assert context.json_call[1]["cache_days"] == 1
    assert context.text_call[1]["headers"] == cellar.expression_headers(
        "ENG", "application/xhtml+xml"
    )


def test_cli_prints_stable_json_and_saves_expression(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    result = json.loads((FIXTURES / "act.json").read_text())
    act = cellar.parse_act_results("32026R1708", result)
    content = (FIXTURES / "expression.xhtml").read_bytes()
    expression = cellar.Expression(
        celex="32026R1708",
        language="ENG",
        media_type="application/xhtml+xml",
        request_url=cellar.cellar_url("32026R1708"),
        final_url="https://publications.europa.eu/final.xhtml",
        content=content,
    )
    monkeypatch.setattr(cellar, "query_act_http", lambda value: act)
    monkeypatch.setattr(
        cellar, "fetch_expression_http", lambda value, language, media_type: expression
    )
    output_path = tmp_path / "act.xhtml"

    assert celex.main(["32026R1708", "--save-expression", str(output_path)]) == 0

    output = capsys.readouterr().out
    parsed = json.loads(output)
    assert parsed["celex"] == "32026R1708"
    assert parsed["expression"]["saved_to"] == str(output_path)
    assert output_path.read_bytes() == content
    assert (
        output
        == json.dumps(parsed, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    )


def test_cellar_cli_writes_expression(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    content = (FIXTURES / "expression.xhtml").read_bytes()
    expression = cellar.Expression(
        celex="32026R1708",
        language="ENG",
        media_type="application/xhtml+xml",
        request_url=cellar.cellar_url("32026R1708"),
        final_url="https://publications.europa.eu/final.xhtml",
        content=content,
    )
    monkeypatch.setattr(
        cellar, "fetch_expression_http", lambda value, language, media_type: expression
    )
    output_path = tmp_path / "act.xhtml"

    assert cellar.main(["32026R1708", "--output", str(output_path)]) == 0
    assert output_path.read_bytes() == content


def test_cellar_cli_extracts_body_with_tables(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    content = (FIXTURES / "expression.xhtml").read_bytes()
    expression = cellar.Expression(
        celex="32026R1708",
        language="ENG",
        media_type="application/xhtml+xml",
        request_url=cellar.cellar_url("32026R1708"),
        final_url="https://publications.europa.eu/final.xhtml",
        content=content,
    )
    monkeypatch.setattr(
        cellar, "fetch_expression_http", lambda value, language, media_type: expression
    )
    output_path = tmp_path / "act-body.html"

    assert cellar.main(["32026R1708", "--body-only", "--output", str(output_path)]) == 0
    body = output_path.read_text()
    assert body.startswith("<body>")
    assert "<table>" in body
    assert "Example Entity" in body
    assert "<head>" not in body


def test_expression_preserves_request() -> None:
    content = (FIXTURES / "expression.xhtml").read_bytes()
    url = cellar.cellar_url("32026R1708")
    expression = cellar.Expression(
        celex="32026R1708",
        language="ENG",
        media_type="application/xhtml+xml",
        request_url=url,
        final_url=url,
        content=content,
    )

    assert expression.request == cellar.Request(url)


def test_http_errors_are_not_hidden(requests_mock) -> None:
    requests_mock.post(cellar.SPARQL_ENDPOINT, status_code=503)
    with pytest.raises(requests.HTTPError):
        cellar.query_act_http("32026R1708")
