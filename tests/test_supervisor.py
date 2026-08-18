import asyncio

import pytest

from marketspike.engine.supervisor import supervise


async def test_supervise_restarts_after_failure():
    attempts = {"count": 0}

    async def flaky():
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise RuntimeError("boom")
        await asyncio.sleep(10)

    task = asyncio.ensure_future(supervise("flaky", flaky, max_backoff_s=0.01))
    await asyncio.sleep(0.3)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert attempts["count"] >= 3


async def test_supervise_reports_errors_to_callback():
    seen = []

    async def always_fails():
        raise ValueError("nope")

    task = asyncio.ensure_future(
        supervise("bad", always_fails, on_error=seen.append, max_backoff_s=0.01)
    )
    await asyncio.sleep(0.1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert seen and isinstance(seen[0], ValueError)


async def test_supervise_propagates_cancellation_without_restart():
    started = {"count": 0}

    async def long_running():
        started["count"] += 1
        await asyncio.sleep(10)

    task = asyncio.ensure_future(supervise("long", long_running))
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert started["count"] == 1
