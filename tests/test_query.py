import asyncio
import json
import re
from pathlib import Path

import pytest
from aioresponses import aioresponses

from ror_matcher.query import RorClient, Checkpoint, run as run_query
from ror_matcher.config import load_config

ROR_API_PATTERN = re.compile(r"^https://api\.ror\.org/v2/organizations\?")


@pytest.fixture
def ror_response_chosen():
    return {
        "items": [
            {
                "chosen": True,
                "organization": {"id": "https://ror.org/052gg0110"},
            }
        ]
    }


@pytest.fixture
def ror_response_empty():
    return {"items": []}


@pytest.mark.asyncio
async def test_query_single_search_match(ror_response_chosen):
    with aioresponses() as m:
        m.get(
            ROR_API_PATTERN,
            payload=ror_response_chosen,
        )
        client = RorClient("https://api.ror.org", "single_search", timeout=5, retries=1)
        result = await client.query_affiliation("University of Oxford")
        assert result == "https://ror.org/052gg0110"


@pytest.mark.asyncio
async def test_query_multisearch_match(ror_response_chosen):
    with aioresponses() as m:
        m.get(
            ROR_API_PATTERN,
            payload=ror_response_chosen,
        )
        client = RorClient("https://api.ror.org", "multisearch", timeout=5, retries=1)
        result = await client.query_affiliation("University of Oxford")
        assert result == "https://ror.org/052gg0110"


@pytest.mark.asyncio
async def test_query_no_match_returns_none(ror_response_empty):
    with aioresponses() as m:
        m.get(
            ROR_API_PATTERN,
            payload=ror_response_empty,
        )
        client = RorClient("https://api.ror.org", "single_search", timeout=5, retries=1)
        result = await client.query_affiliation("Unknown Institution")
        assert result is None


@pytest.mark.asyncio
async def test_query_retries_on_500():
    with aioresponses() as m:
        m.get(ROR_API_PATTERN, status=500)
        m.get(
            ROR_API_PATTERN,
            payload={"items": [{"chosen": True, "organization": {"id": "https://ror.org/abc123"}}]},
        )
        client = RorClient("https://api.ror.org", "single_search", timeout=5, retries=3, retry_backoff=0)
        result = await client.query_affiliation("Test University")
        assert result == "https://ror.org/abc123"


@pytest.mark.asyncio
async def test_query_raises_on_4xx():
    with aioresponses() as m:
        m.get(ROR_API_PATTERN, status=400)
        client = RorClient("https://api.ror.org", "single_search", timeout=5, retries=1)
        with pytest.raises(Exception, match="400"):
            await client.query_affiliation("Bad Request")


def test_checkpoint_save_and_load(tmp_path):
    cp_path = tmp_path / "test.checkpoint"
    cp = Checkpoint(cp_path)
    cp.mark_processed("abc123")
    cp.mark_processed("def456")
    cp.save()

    loaded = Checkpoint.load(cp_path)
    assert loaded.is_processed("abc123")
    assert loaded.is_processed("def456")
    assert not loaded.is_processed("unknown")


def test_checkpoint_load_nonexistent(tmp_path):
    cp = Checkpoint.load(tmp_path / "nonexistent.checkpoint")
    assert not cp.is_processed("anything")
    assert len(cp) == 0


@pytest.mark.asyncio
async def test_query_full_pipeline(tmp_path):
    working = tmp_path / ".ror_matcher"
    working.mkdir()
    affiliations = ["University of Oxford", "MIT"]
    (working / "unique_affiliations.json").write_text(json.dumps(affiliations))

    config_file = tmp_path / "config.yaml"
    config_file.write_text(f"""
input:
  file: "unused.csv"
  format: csv
  id_field: doi
  affiliation_fields:
    - institution
query:
  base_url: "https://api.ror.org"
  endpoint: single_search
  concurrency: 2
  timeout: 5
output:
  file: "{tmp_path}/out.csv"
  format: csv
working_dir: "{working}"
""")
    config = load_config(config_file)

    with aioresponses() as m:
        m.get(
            ROR_API_PATTERN,
            payload={"items": [{"chosen": True, "organization": {"id": "https://ror.org/052gg0110"}}]},
            repeat=True,
        )
        await run_query(config)

    matches_path = working / "ror_matches.jsonl"
    assert matches_path.exists()
    matches = [json.loads(l) for l in matches_path.read_text().strip().split("\n")]
    assert len(matches) == 2

    checkpoint_path = working / "query.checkpoint"
    assert checkpoint_path.exists()


@pytest.mark.asyncio
async def test_query_resume_skips_processed(tmp_path):
    working = tmp_path / ".ror_matcher"
    working.mkdir()
    affiliations = ["University of Oxford", "MIT"]
    (working / "unique_affiliations.json").write_text(json.dumps(affiliations))

    from ror_matcher.models import hash_affiliation
    cp = Checkpoint(working / "query.checkpoint")
    cp.mark_processed(hash_affiliation("University of Oxford"))
    cp.save()

    (working / "ror_matches.jsonl").write_text("")

    config_file = tmp_path / "config.yaml"
    config_file.write_text(f"""
input:
  file: "unused.csv"
  format: csv
  id_field: doi
  affiliation_fields:
    - institution
query:
  base_url: "https://api.ror.org"
  endpoint: single_search
  concurrency: 2
  timeout: 5
output:
  file: "{tmp_path}/out.csv"
  format: csv
working_dir: "{working}"
""")
    config = load_config(config_file)

    with aioresponses() as m:
        m.get(
            ROR_API_PATTERN,
            payload={"items": [{"chosen": True, "organization": {"id": "https://ror.org/abc"}}]},
            repeat=True,
        )
        await run_query(config, resume=True)

    matches = [json.loads(l) for l in (working / "ror_matches.jsonl").read_text().strip().split("\n") if l.strip()]
    assert len(matches) == 1
    assert matches[0]["affiliation"] == "MIT"
