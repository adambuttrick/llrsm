import csv
import json
from pathlib import Path

from .models import Config, ProvenanceRecord, hash_affiliation


def _split_value(value: str, delimiter: str | None) -> list[str]:
    if delimiter is None:
        return [value]
    return [p.strip() for p in value.split(delimiter) if p.strip()]


def _extract_csv(config: Config, working_dir: Path):
    unique: set[str] = set()
    provenance: list[dict] = []

    with open(config.input.file, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row_index, row in enumerate(reader):
            record_id = row[config.input.id_field]
            for af in config.input.affiliation_fields:
                raw_value = row.get(af.field_name, "").strip()
                if not raw_value:
                    continue
                for value in _split_value(raw_value, af.delimiter):
                    unique.add(value)
                    provenance.append(
                        ProvenanceRecord(
                            record_id=record_id,
                            field=af.field_name,
                            affiliation=value,
                            affiliation_hash=hash_affiliation(value),
                            row_index=row_index,
                        ).__dict__
                    )

    return unique, provenance


def _resolve_path(record: dict, path: str) -> list[tuple[str, list[int]]]:
    segments = path.split(".")
    results: list[tuple] = [(record, [])]

    for segment in segments:
        next_results = []
        if segment.endswith("[]"):
            key = segment[:-2]
            for obj, indices in results:
                arr = obj.get(key, []) if isinstance(obj, dict) else []
                for i, item in enumerate(arr):
                    next_results.append((item, indices + [i]))
        else:
            for obj, indices in results:
                if isinstance(obj, dict) and segment in obj:
                    next_results.append((obj[segment], indices))
        results = next_results

    return [(val, idx) for val, idx in results if isinstance(val, str) and val.strip()]


def _get_nested(obj: dict, dotpath: str):
    for key in dotpath.split("."):
        if isinstance(obj, dict):
            obj = obj.get(key)
        else:
            return None
    return obj


def _extract_from_json_record(
    config: Config, record: dict, row_index: int,
    unique: set[str], provenance: list[dict],
):
    record_id = _get_nested(record, config.input.id_field)
    if record_id is None:
        return
    record_id = str(record_id)

    for af in config.input.affiliation_fields:
        if af.path:
            for value, path_indices in _resolve_path(record, af.path):
                value = value.strip()
                if not value:
                    continue
                unique.add(value)
                provenance.append(
                    ProvenanceRecord(
                        record_id=record_id,
                        field=af.field_name,
                        affiliation=value,
                        affiliation_hash=hash_affiliation(value),
                        row_index=row_index,
                        path_indices=path_indices,
                    ).__dict__
                )
        else:
            raw_value = _get_nested(record, af.field_name)
            if not raw_value or not isinstance(raw_value, str) or not raw_value.strip():
                continue
            raw_value = raw_value.strip()
            for value in _split_value(raw_value, af.delimiter):
                unique.add(value)
                provenance.append(
                    ProvenanceRecord(
                        record_id=record_id,
                        field=af.field_name,
                        affiliation=value,
                        affiliation_hash=hash_affiliation(value),
                        row_index=row_index,
                    ).__dict__
                )


def _extract_jsonl(config: Config, working_dir: Path):
    unique: set[str] = set()
    provenance: list[dict] = []

    with open(config.input.file) as f:
        for row_index, line in enumerate(f):
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            _extract_from_json_record(config, record, row_index, unique, provenance)

    return unique, provenance


def _extract_json(config: Config, working_dir: Path):
    unique: set[str] = set()
    provenance: list[dict] = []

    with open(config.input.file) as f:
        records = json.load(f)

    for row_index, record in enumerate(records):
        _extract_from_json_record(config, record, row_index, unique, provenance)

    return unique, provenance


def _write_outputs(unique: set[str], provenance: list[dict], working_dir: Path):
    working_dir.mkdir(parents=True, exist_ok=True)
    with open(working_dir / "unique_affiliations.json", "w") as f:
        json.dump(sorted(unique), f)
    with open(working_dir / "provenance.jsonl", "w") as f:
        for record in provenance:
            cleaned = {k: v for k, v in record.items() if v is not None}
            f.write(json.dumps(cleaned) + "\n")


def run(config: Config):
    working_dir = Path(config.working_dir)
    if config.input.format == "csv":
        unique, provenance = _extract_csv(config, working_dir)
    elif config.input.format == "jsonl":
        unique, provenance = _extract_jsonl(config, working_dir)
    elif config.input.format == "json":
        unique, provenance = _extract_json(config, working_dir)
    else:
        raise ValueError(f"Unsupported format: {config.input.format}")
    _write_outputs(unique, provenance, working_dir)
