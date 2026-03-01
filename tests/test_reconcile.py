import csv
import json
from pathlib import Path

import pytest

from ror_matcher.config import load_config
from ror_matcher.models import hash_affiliation
from ror_matcher.reconcile import run as run_reconcile


@pytest.fixture
def reconcile_setup_csv(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    csv_file = data_dir / "records.csv"
    with open(csv_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["doi", "institution", "department"])
        writer.writerow(["10.1234/abc", "University of Oxford", "Dept of Physics"])
        writer.writerow(["10.5678/def", "MIT", "Dept of Chemistry"])

    working = tmp_path / ".ror_matcher"
    working.mkdir()

    provenance = [
        {"record_id": "10.1234/abc", "field": "institution", "affiliation": "University of Oxford",
         "affiliation_hash": hash_affiliation("University of Oxford"), "row_index": 0},
        {"record_id": "10.1234/abc", "field": "department", "affiliation": "Dept of Physics",
         "affiliation_hash": hash_affiliation("Dept of Physics"), "row_index": 0},
        {"record_id": "10.5678/def", "field": "institution", "affiliation": "MIT",
         "affiliation_hash": hash_affiliation("MIT"), "row_index": 1},
        {"record_id": "10.5678/def", "field": "department", "affiliation": "Dept of Chemistry",
         "affiliation_hash": hash_affiliation("Dept of Chemistry"), "row_index": 1},
    ]
    with open(working / "provenance.jsonl", "w") as f:
        for r in provenance:
            f.write(json.dumps(r) + "\n")

    matches = [
        {"affiliation": "University of Oxford", "affiliation_hash": hash_affiliation("University of Oxford"),
         "ror_id": "https://ror.org/052gg0110"},
        {"affiliation": "MIT", "affiliation_hash": hash_affiliation("MIT"),
         "ror_id": "https://ror.org/042nb2s44"},
    ]
    with open(working / "ror_matches.jsonl", "w") as f:
        for r in matches:
            f.write(json.dumps(r) + "\n")

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
output:
  file: "{out_file}"
  format: csv
working_dir: "{working}"
""")
    return load_config(config_file), out_file


def test_reconcile_csv_output(reconcile_setup_csv):
    config, out_file = reconcile_setup_csv
    run_reconcile(config)
    assert out_file.exists()
    with open(out_file, newline="") as f:
        reader = list(csv.DictReader(f))
    assert len(reader) == 2
    assert reader[0]["institution_ror_id"] == "https://ror.org/052gg0110"
    assert reader[0]["department_ror_id"] == ""
    assert reader[1]["institution_ror_id"] == "https://ror.org/042nb2s44"
    assert reader[1]["department_ror_id"] == ""


def test_reconcile_csv_preserves_original_columns(reconcile_setup_csv):
    config, out_file = reconcile_setup_csv
    run_reconcile(config)
    with open(out_file, newline="") as f:
        reader = list(csv.DictReader(f))
    assert reader[0]["doi"] == "10.1234/abc"
    assert reader[0]["institution"] == "University of Oxford"
    assert reader[0]["department"] == "Dept of Physics"


@pytest.fixture
def reconcile_setup_jsonl(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    jsonl_file = data_dir / "records.jsonl"
    records = [
        {"id": "10.1234/abc", "institution": "University of Oxford"},
        {"id": "10.5678/def", "institution": "MIT"},
    ]
    with open(jsonl_file, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")

    working = tmp_path / ".ror_matcher"
    working.mkdir()

    provenance = [
        {"record_id": "10.1234/abc", "field": "institution",
         "affiliation": "University of Oxford",
         "affiliation_hash": hash_affiliation("University of Oxford"), "row_index": 0},
        {"record_id": "10.5678/def", "field": "institution",
         "affiliation": "MIT",
         "affiliation_hash": hash_affiliation("MIT"), "row_index": 1},
    ]
    with open(working / "provenance.jsonl", "w") as f:
        for r in provenance:
            f.write(json.dumps(r) + "\n")

    matches = [
        {"affiliation": "University of Oxford",
         "affiliation_hash": hash_affiliation("University of Oxford"),
         "ror_id": "https://ror.org/052gg0110"},
    ]
    with open(working / "ror_matches.jsonl", "w") as f:
        for r in matches:
            f.write(json.dumps(r) + "\n")

    out_file = tmp_path / "enriched.jsonl"
    config_file = tmp_path / "config.yaml"
    config_file.write_text(f"""
input:
  file: "{jsonl_file}"
  format: jsonl
  id_field: id
  affiliation_fields:
    - institution
query:
  base_url: "https://api.ror.org"
  endpoint: single_search
output:
  file: "{out_file}"
  format: jsonl
working_dir: "{working}"
""")
    return load_config(config_file), out_file


def test_reconcile_jsonl_output(reconcile_setup_jsonl):
    config, out_file = reconcile_setup_jsonl
    run_reconcile(config)
    records = [json.loads(l) for l in out_file.read_text().strip().split("\n")]
    assert len(records) == 2
    assert records[0]["institution_ror_id"] == "https://ror.org/052gg0110"
    assert records[1]["institution_ror_id"] == ""


@pytest.fixture
def reconcile_setup_json(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    json_file = data_dir / "records.json"
    records = [
        {"id": "10.1234/abc", "institution": "MIT"},
    ]
    with open(json_file, "w") as f:
        json.dump(records, f)

    working = tmp_path / ".ror_matcher"
    working.mkdir()

    provenance = [
        {"record_id": "10.1234/abc", "field": "institution",
         "affiliation": "MIT",
         "affiliation_hash": hash_affiliation("MIT"), "row_index": 0},
    ]
    with open(working / "provenance.jsonl", "w") as f:
        for r in provenance:
            f.write(json.dumps(r) + "\n")

    matches = [
        {"affiliation": "MIT",
         "affiliation_hash": hash_affiliation("MIT"),
         "ror_id": "https://ror.org/042nb2s44"},
    ]
    with open(working / "ror_matches.jsonl", "w") as f:
        for r in matches:
            f.write(json.dumps(r) + "\n")

    out_file = tmp_path / "enriched.json"
    config_file = tmp_path / "config.yaml"
    config_file.write_text(f"""
input:
  file: "{json_file}"
  format: json
  id_field: id
  affiliation_fields:
    - institution
query:
  base_url: "https://api.ror.org"
  endpoint: single_search
output:
  file: "{out_file}"
  format: json
working_dir: "{working}"
""")
    return load_config(config_file), out_file


def test_reconcile_json_output(reconcile_setup_json):
    config, out_file = reconcile_setup_json
    run_reconcile(config)
    records = json.loads(out_file.read_text())
    assert len(records) == 1
    assert records[0]["institution_ror_id"] == "https://ror.org/042nb2s44"


def test_reconcile_custom_output_name(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    csv_file = data_dir / "records.csv"
    with open(csv_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["doi", "institution"])
        writer.writerow(["10.1234/abc", "MIT"])

    working = tmp_path / ".ror_matcher"
    working.mkdir()

    provenance = [
        {"record_id": "10.1234/abc", "field": "institution",
         "affiliation": "MIT",
         "affiliation_hash": hash_affiliation("MIT"), "row_index": 0},
    ]
    with open(working / "provenance.jsonl", "w") as f:
        for r in provenance:
            f.write(json.dumps(r) + "\n")

    matches = [
        {"affiliation": "MIT", "affiliation_hash": hash_affiliation("MIT"),
         "ror_id": "https://ror.org/042nb2s44"},
    ]
    with open(working / "ror_matches.jsonl", "w") as f:
        for r in matches:
            f.write(json.dumps(r) + "\n")

    out_file = tmp_path / "enriched.csv"
    config_file = tmp_path / "config.yaml"
    config_file.write_text(f"""
input:
  file: "{csv_file}"
  format: csv
  id_field: doi
  affiliation_fields:
    - field: institution
      output_name: inst_ror
query:
  base_url: "https://api.ror.org"
  endpoint: single_search
output:
  file: "{out_file}"
  format: csv
working_dir: "{working}"
""")
    config = load_config(config_file)
    run_reconcile(config)
    with open(out_file, newline="") as f:
        reader = list(csv.DictReader(f))
    assert "inst_ror" in reader[0]
    assert reader[0]["inst_ror"] == "https://ror.org/042nb2s44"


def test_reconcile_csv_multiple_ror_ids_joined(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    csv_file = data_dir / "records.csv"
    with open(csv_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "names"])
        writer.writerow(["1", "MIT | Stanford"])

    working = tmp_path / ".ror_matcher"
    working.mkdir()

    provenance = [
        {"record_id": "1", "field": "names",
         "affiliation": "MIT",
         "affiliation_hash": hash_affiliation("MIT"), "row_index": 0},
        {"record_id": "1", "field": "names",
         "affiliation": "Stanford",
         "affiliation_hash": hash_affiliation("Stanford"), "row_index": 0},
    ]
    with open(working / "provenance.jsonl", "w") as f:
        for r in provenance:
            f.write(json.dumps(r) + "\n")

    matches = [
        {"affiliation": "MIT", "affiliation_hash": hash_affiliation("MIT"),
         "ror_id": "https://ror.org/042nb2s44"},
        {"affiliation": "Stanford", "affiliation_hash": hash_affiliation("Stanford"),
         "ror_id": "https://ror.org/00f54p054"},
    ]
    with open(working / "ror_matches.jsonl", "w") as f:
        for r in matches:
            f.write(json.dumps(r) + "\n")

    out_file = tmp_path / "enriched.csv"
    config_file = tmp_path / "config.yaml"
    config_file.write_text(f"""
input:
  file: "{csv_file}"
  format: csv
  id_field: id
  affiliation_fields:
    - field: names
      delimiter: " | "
query:
  base_url: "https://api.ror.org"
  endpoint: single_search
output:
  file: "{out_file}"
  format: csv
working_dir: "{working}"
""")
    config = load_config(config_file)
    run_reconcile(config)
    with open(out_file, newline="") as f:
        rows = list(csv.DictReader(f))
    assert rows[0]["names_ror_id"] == "https://ror.org/042nb2s44 | https://ror.org/00f54p054"


def test_reconcile_csv_partial_match(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    csv_file = data_dir / "records.csv"
    with open(csv_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "names"])
        writer.writerow(["1", "MIT | Unknown Place"])

    working = tmp_path / ".ror_matcher"
    working.mkdir()

    provenance = [
        {"record_id": "1", "field": "names",
         "affiliation": "MIT",
         "affiliation_hash": hash_affiliation("MIT"), "row_index": 0},
        {"record_id": "1", "field": "names",
         "affiliation": "Unknown Place",
         "affiliation_hash": hash_affiliation("Unknown Place"), "row_index": 0},
    ]
    with open(working / "provenance.jsonl", "w") as f:
        for r in provenance:
            f.write(json.dumps(r) + "\n")

    matches = [
        {"affiliation": "MIT", "affiliation_hash": hash_affiliation("MIT"),
         "ror_id": "https://ror.org/042nb2s44"},
    ]
    with open(working / "ror_matches.jsonl", "w") as f:
        for r in matches:
            f.write(json.dumps(r) + "\n")

    out_file = tmp_path / "enriched.csv"
    config_file = tmp_path / "config.yaml"
    config_file.write_text(f"""
input:
  file: "{csv_file}"
  format: csv
  id_field: id
  affiliation_fields:
    - field: names
      delimiter: " | "
query:
  base_url: "https://api.ror.org"
  endpoint: single_search
output:
  file: "{out_file}"
  format: csv
working_dir: "{working}"
""")
    config = load_config(config_file)
    run_reconcile(config)
    with open(out_file, newline="") as f:
        rows = list(csv.DictReader(f))
    assert rows[0]["names_ror_id"] == "https://ror.org/042nb2s44"


def test_reconcile_csv_deduplicates_ror_ids(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    csv_file = data_dir / "records.csv"
    with open(csv_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "names"])
        writer.writerow(["1", "MIT | Massachusetts Institute of Technology"])

    working = tmp_path / ".ror_matcher"
    working.mkdir()

    provenance = [
        {"record_id": "1", "field": "names",
         "affiliation": "MIT",
         "affiliation_hash": hash_affiliation("MIT"), "row_index": 0},
        {"record_id": "1", "field": "names",
         "affiliation": "Massachusetts Institute of Technology",
         "affiliation_hash": hash_affiliation("Massachusetts Institute of Technology"), "row_index": 0},
    ]
    with open(working / "provenance.jsonl", "w") as f:
        for r in provenance:
            f.write(json.dumps(r) + "\n")

    same_id = "https://ror.org/042nb2s44"
    matches = [
        {"affiliation": "MIT", "affiliation_hash": hash_affiliation("MIT"),
         "ror_id": same_id},
        {"affiliation": "Massachusetts Institute of Technology",
         "affiliation_hash": hash_affiliation("Massachusetts Institute of Technology"),
         "ror_id": same_id},
    ]
    with open(working / "ror_matches.jsonl", "w") as f:
        for r in matches:
            f.write(json.dumps(r) + "\n")

    out_file = tmp_path / "enriched.csv"
    config_file = tmp_path / "config.yaml"
    config_file.write_text(f"""
input:
  file: "{csv_file}"
  format: csv
  id_field: id
  affiliation_fields:
    - field: names
      delimiter: " | "
query:
  base_url: "https://api.ror.org"
  endpoint: single_search
output:
  file: "{out_file}"
  format: csv
working_dir: "{working}"
""")
    config = load_config(config_file)
    run_reconcile(config)
    with open(out_file, newline="") as f:
        rows = list(csv.DictReader(f))
    assert rows[0]["names_ror_id"] == same_id
