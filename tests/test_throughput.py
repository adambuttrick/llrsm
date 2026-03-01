import re

import pytest
from aioresponses import aioresponses

from ror_matcher.throughput import test_concurrency_level as run_concurrency_test
from ror_matcher.throughput import find_optimal_concurrency

ROR_API_PATTERN = re.compile(r"^https://api\.ror\.org/v2/organizations")


@pytest.mark.asyncio
async def test_concurrency_level_passes_on_success():
    with aioresponses() as m:
        m.get(
            ROR_API_PATTERN,
            payload={"items": []},
            repeat=True,
        )
        result = await run_concurrency_test("https://api.ror.org", 5, timeout=5)
        assert result.passed is True
        assert result.error_count == 0


@pytest.mark.asyncio
async def test_concurrency_level_fails_on_errors():
    with aioresponses() as m:
        for _ in range(20):
            m.get(ROR_API_PATTERN, status=500)
        result = await run_concurrency_test("https://api.ror.org", 20, timeout=5)
        assert result.passed is False
        assert result.error_count > 0


@pytest.mark.asyncio
async def test_find_optimal_concurrency():
    with aioresponses() as m:
        m.get(
            ROR_API_PATTERN,
            payload={"items": []},
            repeat=True,
        )
        optimal = await find_optimal_concurrency(
            "https://api.ror.org", timeout=5, ceiling=20
        )
        assert optimal > 0
