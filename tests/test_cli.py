import json
from pathlib import Path
from unittest.mock import patch, AsyncMock

from click.testing import CliRunner

from ror_matcher.cli import main


def test_cli_extract(tmp_path):
    import csv
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    csv_file = data_dir / "records.csv"
    with open(csv_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["doi", "institution"])
        writer.writerow(["10.1234/abc", "MIT"])

    config_file = tmp_path / "config.yaml"
    config_file.write_text(f"""
input:
  file: "{csv_file}"
  format: csv
  id_field: doi
  affiliation_fields:
    - institution
query:
  base_url: "https://api.ror.org"
  endpoint: single_search
output:
  file: "{tmp_path}/out.csv"
  format: csv
working_dir: "{tmp_path / '.ror_matcher'}"
""")

    runner = CliRunner()
    result = runner.invoke(main, ["extract", "--config", str(config_file)])
    assert result.exit_code == 0
    assert (tmp_path / ".ror_matcher" / "unique_affiliations.json").exists()


def test_cli_missing_config():
    runner = CliRunner()
    result = runner.invoke(main, ["extract", "--config", "nonexistent.yaml"])
    assert result.exit_code != 0


def test_cli_run(tmp_path):
    import csv
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    csv_file = data_dir / "records.csv"
    with open(csv_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["doi", "institution"])
        writer.writerow(["10.1234/abc", "MIT"])

    config_file = tmp_path / "config.yaml"
    config_file.write_text(f"""
input:
  file: "{csv_file}"
  format: csv
  id_field: doi
  affiliation_fields:
    - institution
query:
  base_url: "https://api.ror.org"
  endpoint: single_search
output:
  file: "{tmp_path}/out.csv"
  format: csv
working_dir: "{tmp_path / '.ror_matcher'}"
""")

    mock_query_run = AsyncMock()
    with patch("ror_matcher.cli.query.run", mock_query_run):
        runner = CliRunner()
        result = runner.invoke(main, ["run", "--config", str(config_file)])

    assert result.exit_code == 0, result.output
    assert "Stage 1: Extracting affiliations..." in result.output
    assert "Extraction complete." in result.output
    assert "Stage 2: Querying ROR API..." in result.output
    assert "Query complete." in result.output
    assert "Stage 3: Reconciling matches..." in result.output
    assert "Pipeline finished." in result.output
    assert (tmp_path / ".ror_matcher" / "unique_affiliations.json").exists()
    assert (tmp_path / ".ror_matcher" / "provenance.jsonl").exists()


def test_cli_run_with_optimize(tmp_path):
    import csv
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    csv_file = data_dir / "records.csv"
    with open(csv_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["doi", "institution"])
        writer.writerow(["10.1234/abc", "MIT"])

    config_file = tmp_path / "config.yaml"
    config_file.write_text(f"""
input:
  file: "{csv_file}"
  format: csv
  id_field: doi
  affiliation_fields:
    - institution
query:
  base_url: "https://api.ror.org"
  endpoint: single_search
output:
  file: "{tmp_path}/out.csv"
  format: csv
working_dir: "{tmp_path / '.ror_matcher'}"
""")

    mock_query_run = AsyncMock()
    mock_optimize = AsyncMock(return_value=42)
    with patch("ror_matcher.cli.query.run", mock_query_run), \
         patch("ror_matcher.cli.throughput.find_optimal_concurrency", mock_optimize):
        runner = CliRunner()
        result = runner.invoke(main, ["run", "--config", str(config_file), "--optimize"])

    assert result.exit_code == 0, result.output
    assert "Optimizing concurrency..." in result.output
    assert "Concurrency set to 42." in result.output
    assert "Pipeline finished." in result.output
