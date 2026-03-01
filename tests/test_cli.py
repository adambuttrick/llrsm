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
