import csv
import json
import re
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from aioresponses import aioresponses

from ror_matcher.config import load_config
from ror_matcher.extract import run as run_extract
from ror_matcher.query import run as run_query
from ror_matcher.reconcile import run as run_reconcile
from ror_matcher import throughput

ROR_API_PATTERN = re.compile(r"^https://api\.ror\.org/v2/organizations")


@pytest.mark.asyncio
async def test_full_pipeline_csv_to_csv(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    csv_file = data_dir / "records.csv"
    with open(csv_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["doi", "institution", "department"])
        writer.writerow(["10.1234/abc", "University of Oxford", "Dept of Physics"])
        writer.writerow(["10.5678/def", "MIT", "Dept of Chemistry"])
        writer.writerow(["10.9999/ghi", "Unknown Place", "Unknown Dept"])

    out_file = tmp_path / "enriched.csv"
    config_file = tmp_path / "config.yaml"
    config_file.write_text(f"""
input:
  file: "{csv_file}"
  format: csv
  id_field: doi
  affiliation_fields:
    - institution
    - department
query:
  base_url: "https://api.ror.org"
  endpoint: single_search
  concurrency: 2
  timeout: 5
output:
  file: "{out_file}"
  format: csv
working_dir: "{tmp_path / '.ror_matcher'}"
""")
    config = load_config(config_file)

    run_extract(config)
    working = Path(config.working_dir)
    assert (working / "unique_affiliations.json").exists()
    assert (working / "provenance.jsonl").exists()

    with open(working / "unique_affiliations.json") as f:
        unique_affiliations = json.load(f)
    assert len(unique_affiliations) == 6

    with aioresponses() as m:
        m.get(ROR_API_PATTERN, payload={"items": []}, repeat=True)
        await run_query(config)

    assert (working / "ror_matches.jsonl").exists()
    assert (working / "ror_failures.jsonl").exists()

    failures_text = (working / "ror_failures.jsonl").read_text().strip()
    failure_lines = [l for l in failures_text.split("\n") if l.strip()]
    assert len(failure_lines) == 6

    run_reconcile(config)
    assert out_file.exists()

    with open(out_file, newline="") as f:
        rows = list(csv.DictReader(f))

    assert len(rows) == 3
    assert "institution_ror_id" in rows[0]
    assert "department_ror_id" in rows[0]
    assert rows[0]["doi"] == "10.1234/abc"
    assert rows[0]["institution"] == "University of Oxford"
    assert rows[0]["department"] == "Dept of Physics"
    assert rows[1]["doi"] == "10.5678/def"
    assert rows[1]["institution"] == "MIT"
    assert rows[2]["doi"] == "10.9999/ghi"
    assert rows[2]["institution"] == "Unknown Place"
    assert rows[0]["institution_ror_id"] == ""
    assert rows[0]["department_ror_id"] == ""


@pytest.mark.asyncio
async def test_full_pipeline_csv_with_delimiter(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    csv_file = data_dir / "records.csv"
    with open(csv_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name", "alt_names"])
        writer.writerow(["1", "MIT", "Stanford | Harvard"])
        writer.writerow(["2", "Oxford", "University of Oxford"])

    out_file = tmp_path / "enriched.csv"
    config_file = tmp_path / "config.yaml"
    config_file.write_text(f"""
input:
  file: "{csv_file}"
  format: csv
  id_field: id
  affiliation_fields:
    - name
    - field: alt_names
      delimiter: " | "
query:
  base_url: "https://api.ror.org"
  endpoint: single_search
  concurrency: 2
  timeout: 5
output:
  file: "{out_file}"
  format: csv
working_dir: "{tmp_path / '.ror_matcher'}"
""")
    config = load_config(config_file)

    run_extract(config)
    working = Path(config.working_dir)
    affiliations = json.loads((working / "unique_affiliations.json").read_text())
    assert len(affiliations) == 5

    provenance = [json.loads(l) for l in (working / "provenance.jsonl").read_text().strip().split("\n")]
    assert len(provenance) == 5

    with aioresponses() as m:
        m.get(ROR_API_PATTERN, payload={"items": []}, repeat=True)
        await run_query(config)

    run_reconcile(config)
    with open(out_file, newline="") as f:
        rows = list(csv.DictReader(f))

    assert len(rows) == 2
    assert "name_ror_id" in rows[0]
    assert "alt_names_ror_id" in rows[0]
    assert rows[0]["name_ror_id"] == ""
    assert rows[0]["alt_names_ror_id"] == ""
    assert rows[1]["name_ror_id"] == ""
    assert rows[1]["alt_names_ror_id"] == ""


@pytest.mark.asyncio
async def test_full_pipeline_via_run_with_optimize(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    csv_file = data_dir / "records.csv"
    with open(csv_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["doi", "institution"])
        writer.writerow(["10.1234/abc", "University of Oxford"])
        writer.writerow(["10.5678/def", "MIT"])

    out_file = tmp_path / "enriched.csv"
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
  concurrency: 10
  timeout: 5
output:
  file: "{out_file}"
  format: csv
working_dir: "{tmp_path / '.ror_matcher'}"
""")
    config = load_config(config_file)

    run_extract(config)
    working = Path(config.working_dir)
    assert (working / "unique_affiliations.json").exists()

    mock_optimize = AsyncMock(return_value=42)
    with patch.object(throughput, "find_optimal_concurrency", mock_optimize):
        optimal = await throughput.find_optimal_concurrency(
            config.query.base_url, timeout=config.query.timeout
        )
    config.query.concurrency = optimal
    assert config.query.concurrency == 42

    with aioresponses() as m:
        m.get(ROR_API_PATTERN, payload={"items": []}, repeat=True)
        await run_query(config)

    run_reconcile(config)
    assert out_file.exists()

    with open(out_file, newline="") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 2
    assert "institution_ror_id" in rows[0]
