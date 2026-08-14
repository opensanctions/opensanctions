import json
from datetime import date
from pathlib import Path

import pytest
import requests
from click.testing import CliRunner

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


def test_build_and_parse_related_acts_query() -> None:
    query = cellar.build_related_acts_query(
        ["32024R1485", "32024R1485"],
        date(2025, 1, 1),
        date(2026, 7, 31),
    ).decode("utf-8")
    assert query.count('"32024R1485"^^xsd:string') == 1
    assert '?doc_date >= "2025-01-01"^^xsd:date' in query
    assert '?doc_date <= "2026-07-31"^^xsd:date' in query
    assert "resource_legal_amends_resource_legal" in query
    assert "resource_legal_based_on_resource_legal" in query
    assert (
        "PREFIX rt: <http://publications.europa.eu/resource/authority/resource-type/>"
        in query
    )
    assert "FILTER (?type IN (rt:DEC, rt:DEC_IMPL, rt:REG, rt:REG_IMPL))" in query

    result = json.loads((FIXTURES / "related.json").read_text())
    related = cellar.parse_related_act_results(result)
    assert [item.celex for item in related] == [
        "32025R1980",
        "32026R1708",
        "32026R1708",
    ]
    assert [item.relation for item in related] == ["amends", "amends", "based_on"]
    assert related[0].title is None
    assert related[1].resource_type == "REG_IMPL"


def test_related_acts_query_rejects_invalid_bounds() -> None:
    with pytest.raises(ValueError, match="date_to"):
        cellar.build_related_acts_query(
            "32024R1485", date(2026, 1, 2), date(2026, 1, 1)
        )
    with pytest.raises(ValueError, match="At least one"):
        cellar.build_related_acts_query([], date(2026, 1, 1))
    with pytest.raises(ValueError, match="resource type"):
        cellar.build_related_acts_query(
            "32024R1485", date(2026, 1, 1), resource_types=[]
        )


def test_cellar_client_queries_related_acts(requests_mock) -> None:
    result = json.loads((FIXTURES / "related.json").read_text())
    requests_mock.post(cellar.SPARQL_ENDPOINT, json=result)
    client = cellar.CellarClient(requests.Session())

    related = client.query_related_acts("32024R1485", date(2025, 1, 1))

    assert len(related) == 3
    assert related[-1].framework_celex == "32024R1485"
    assert b'"32024R1485"^^xsd:string' in requests_mock.last_request.body


def test_cellar_client_fetches_expression(requests_mock) -> None:
    content = (FIXTURES / "expression.xhtml").read_bytes()
    requests_mock.get(
        cellar.cellar_url("32026R1708"),
        content=content,
        headers={"Content-Type": "application/xhtml+xml; charset=utf-8"},
    )

    client = cellar.CellarClient(requests.Session())
    expression = client.fetch_expression("32026R1708")

    assert expression.content == content
    assert expression.media_type == "application/xhtml+xml"
    assert expression.to_dict()["bytes"] == len(content)
    assert len(expression.sha256) == 64
    assert requests_mock.last_request.headers["Accept-Language"] == "eng"


def test_cellar_client_caches_metadata_and_exact_expression_bytes(
    requests_mock,
) -> None:
    result = json.loads((FIXTURES / "act.json").read_text())
    content = (FIXTURES / "expression.xhtml").read_bytes()

    class FakeCache:
        values: dict[str, str] = {}

        def get(self, key: str, max_age: int | None = None) -> str | None:
            return self.values.get(key)

        def set(self, key: str, value: str | None) -> None:
            assert value is not None
            self.values[key] = value

        def delete(self, key: str) -> None:
            self.values.pop(key, None)

    requests_mock.post(cellar.SPARQL_ENDPOINT, json=result)
    requests_mock.get(
        cellar.cellar_url("32026R1708"),
        content=content,
        headers={"Content-Type": "application/xhtml+xml; charset=utf-8"},
    )
    client = cellar.CellarClient(requests.Session(), FakeCache())
    act = client.query_act("32026R1708")
    expression = client.fetch_expression("32026R1708")
    cached_act = client.query_act("32026R1708")
    cached_expression = client.fetch_expression("32026R1708")

    assert act.latest_consolidated == "02024R1485-20260713"
    assert cached_act == act
    assert expression.content == content
    assert cached_expression == expression
    assert requests_mock.call_count == 2


def test_expression_cache_varies_by_content_negotiation(requests_mock) -> None:
    requests_mock.get(
        cellar.cellar_url("32026R1708"),
        [
            {"content": b"english"},
            {"content": b"french"},
        ],
    )

    class FakeCache:
        values: dict[str, str] = {}

        def get(self, key: str, max_age: int | None = None) -> str | None:
            return self.values.get(key)

        def set(self, key: str, value: str | None) -> None:
            assert value is not None
            self.values[key] = value

        def delete(self, key: str) -> None:
            self.values.pop(key, None)

    client = cellar.CellarClient(requests.Session(), FakeCache())
    assert client.fetch_expression("32026R1708", language="ENG").content == b"english"
    assert client.fetch_expression("32026R1708", language="FRA").content == b"french"
    assert client.fetch_expression("32026R1708", language="ENG").content == b"english"
    assert requests_mock.call_count == 2


def test_cli_prints_stable_json_and_saves_expression(
    requests_mock, tmp_path: Path
) -> None:
    result = json.loads((FIXTURES / "act.json").read_text())
    content = (FIXTURES / "expression.xhtml").read_bytes()
    requests_mock.post(cellar.SPARQL_ENDPOINT, json=result)
    requests_mock.get(
        cellar.cellar_url("32026R1708"),
        content=content,
        headers={"Content-Type": "application/xhtml+xml"},
    )
    output_path = tmp_path / "act.xhtml"

    result = CliRunner().invoke(
        celex.cli, ["32026R1708", "--save-expression", str(output_path)]
    )
    assert result.exit_code == 0, result.output
    output = result.output
    parsed = json.loads(output)
    assert parsed["celex"] == "32026R1708"
    assert parsed["expression"]["saved_to"] == str(output_path)
    assert output_path.read_bytes() == content
    assert (
        output
        == json.dumps(parsed, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    )


def test_celex_cli_includes_related_acts(requests_mock) -> None:
    act = json.loads((FIXTURES / "framework.json").read_text())
    related = json.loads((FIXTURES / "related.json").read_text())
    requests_mock.post(cellar.SPARQL_ENDPOINT, [{"json": act}, {"json": related}])

    result = CliRunner().invoke(
        celex.cli,
        [
            "32024R1485",
            "--metadata-only",
            "--related-since",
            "2025-01-01",
            "--related-until",
            "2026-07-31",
        ],
    )

    assert result.exit_code == 0, result.output
    output = json.loads(result.output)
    assert len(output["related_acts"]) == 3
    assert output["related_acts"][0]["celex"] == "32025R1980"


def test_cellar_cli_writes_expression(
    requests_mock,
    tmp_path: Path,
) -> None:
    content = (FIXTURES / "expression.xhtml").read_bytes()
    requests_mock.get(
        cellar.cellar_url("32026R1708"),
        content=content,
        headers={"Content-Type": "application/xhtml+xml"},
    )
    output_path = tmp_path / "act.xhtml"

    result = CliRunner().invoke(
        cellar.cli, ["32026R1708", "--output", str(output_path)]
    )
    assert result.exit_code == 0, result.output
    assert output_path.read_bytes() == content


def test_cellar_cli_extracts_body_with_tables(
    requests_mock,
    tmp_path: Path,
) -> None:
    content = (FIXTURES / "expression.xhtml").read_bytes()
    requests_mock.get(
        cellar.cellar_url("32026R1708"),
        content=content,
        headers={"Content-Type": "application/xhtml+xml"},
    )
    output_path = tmp_path / "act-body.html"

    result = CliRunner().invoke(
        cellar.cli,
        ["32026R1708", "--body-only", "--output", str(output_path)],
    )
    assert result.exit_code == 0, result.output
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

    assert expression.request == cellar.expression_request("32026R1708")


def test_http_errors_are_not_hidden(requests_mock) -> None:
    requests_mock.post(cellar.SPARQL_ENDPOINT, status_code=503)
    with pytest.raises(requests.HTTPError):
        cellar.CellarClient(requests.Session()).query_act("32026R1708")
