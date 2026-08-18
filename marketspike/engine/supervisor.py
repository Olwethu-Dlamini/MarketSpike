import asyncio
import logging
import random
from typing import Awaitable, Callable, Optional

LOGGER = logging.getLogger(__name__)


async def supervise(
    name: str,
    factory: Callable[[], Awaitable[None]],
    on_error: Optional[Callable[[BaseException], None]] = None,
    max_backoff_s: float = 30.0,
) -> None:
    """Run a coroutine forever, restarting it with backoff on failure.

    A bare asyncio.Task that raises dies silently — the feed stops and nothing
    is logged (spec §14.1). Every long-lived task runs under this wrapper.
    """
    backoff = 0.5
    while True:
        try:
            await factory()
            backoff = 0.5
        except asyncio.CancelledError:
            raise
        except BaseException as error:  # noqa: BLE001 - deliberate catch-all
            LOGGER.exception("task %s failed; restarting", name)
            if on_error is not None:
                on_error(error)
            await asyncio.sleep(min(backoff, max_backoff_s) + random.random() * 0.1)
            backoff = min(backoff * 2, max_backoff_s)
