import csv
import json
from collections import defaultdict
from pathlib import Path

from .models import Config


def _load_matches(working_dir: Path) -> dict[str, str]:
    matches: dict[str, str] = {}
    matches_path = working_dir / "ror_matches.jsonl"
    if not matches_path.exists():
        return matches
    for line in matches_path.read_text().strip().split("\n"):
        if line.strip():
            record = json.loads(line)
            matches[record["affiliation_hash"]] = record["ror_id"]
    return matches


def _load_provenance(working_dir: Path) -> list[dict]:
    prov_path = working_dir / "provenance.jsonl"
    records = []
    for line in prov_path.read_text().strip().split("\n"):
        if line.strip():
            records.append(json.loads(line))
    return records


def _get_output_field_name(af, config: Config) -> str:
    if af.output_name:
        return af.output_name
    return f"{af.field_name}_{config.output.ror_id_field}"


def _reconcile_csv(config: Config, matches: dict[str, str], provenance: list[dict]):
    lookup: dict[tuple[int, str], list[str]] = defaultdict(list)
    for prov in provenance:
        h = prov["affiliation_hash"]
        if h in matches:
            ror_id = matches[h]
            ids = lookup[(prov["row_index"], prov["field"])]
            if ror_id not in ids:
                ids.append(ror_id)

    output_fields = [
        _get_output_field_name(af, config) for af in config.input.affiliation_fields
    ]

    with open(config.input.file, newline="", encoding="utf-8-sig") as inf:
        reader = csv.DictReader(inf)
        fieldnames = list(reader.fieldnames or []) + output_fields

        with open(config.output.file, "w", newline="") as outf:
            writer = csv.DictWriter(outf, fieldnames=fieldnames)
            writer.writeheader()
            for row_index, row in enumerate(reader):
                for af, out_field in zip(config.input.affiliation_fields, output_fields):
                    ids = lookup.get((row_index, af.field_name), [])
                    row[out_field] = " | ".join(ids) if ids else ""
                writer.writerow(row)


def _reconcile_jsonl(config: Config, matches: dict[str, str], provenance: list[dict]):
    path_lookup: dict = defaultdict(dict)
    flat_lookup: dict[tuple[int, str], list[str]] = defaultdict(list)
    for prov in provenance:
        h = prov["affiliation_hash"]
        if h in matches:
            key = (prov["row_index"], prov["field"])
            path_indices = tuple(prov["path_indices"]) if prov.get("path_indices") else None
            if path_indices:
                path_lookup[key][path_indices] = matches[h]
            else:
                ror_id = matches[h]
                ids = flat_lookup[key]
                if ror_id not in ids:
                    ids.append(ror_id)

    output_field_map = {
        af.field_name: _get_output_field_name(af, config)
        for af in config.input.affiliation_fields
    }

    with open(config.input.file) as inf, open(config.output.file, "w") as outf:
        for row_index, line in enumerate(inf):
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            for af in config.input.affiliation_fields:
                out_field = output_field_map[af.field_name]
                key = (row_index, af.field_name)
                if af.path is None:
                    ids = flat_lookup.get(key, [])
                    record[out_field] = " | ".join(ids) if ids else ""
            outf.write(json.dumps(record) + "\n")


def _reconcile_json(config: Config, matches: dict[str, str], provenance: list[dict]):
    lookup: dict[tuple[int, str], list[str]] = defaultdict(list)
    for prov in provenance:
        h = prov["affiliation_hash"]
        if h in matches:
            key = (prov["row_index"], prov["field"])
            ror_id = matches[h]
            ids = lookup[key]
            if ror_id not in ids:
                ids.append(ror_id)

    output_field_map = {
        af.field_name: _get_output_field_name(af, config)
        for af in config.input.affiliation_fields
    }

    with open(config.input.file) as f:
        records = json.load(f)

    for row_index, record in enumerate(records):
        for af in config.input.affiliation_fields:
            out_field = output_field_map[af.field_name]
            key = (row_index, af.field_name)
            ids = lookup.get(key, [])
            record[out_field] = " | ".join(ids) if ids else ""

    with open(config.output.file, "w") as f:
        json.dump(records, f, indent=2)


def run(config: Config):
    working_dir = Path(config.working_dir)
    matches = _load_matches(working_dir)
    provenance = _load_provenance(working_dir)

    Path(config.output.file).parent.mkdir(parents=True, exist_ok=True)

    if config.output.format == "csv":
        _reconcile_csv(config, matches, provenance)
    elif config.output.format == "jsonl":
        _reconcile_jsonl(config, matches, provenance)
    elif config.output.format == "json":
        _reconcile_json(config, matches, provenance)
    else:
        raise ValueError(f"Unsupported output format: {config.output.format}")
