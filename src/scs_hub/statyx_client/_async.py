import asyncio
import concurrent.futures
from collections.abc import Coroutine
from typing import Any, TypeVar

T = TypeVar("T")


def run_coro(coro: Coroutine[Any, Any, T]) -> T:
    """
    Run a coroutine to completion, whether or not an event loop is already
    running in this thread. Plain scripts / python REPL have no running loop,
    so asyncio.run() works directly. Interactive consoles built on IPython
    (Jupyter, Positron) already have one running, which asyncio.run() refuses
    to nest inside — so in that case, run the coroutine on a separate thread
    with its own fresh loop instead.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        # no loop running here — the normal case
        return asyncio.run(coro)

    # a loop is already running (e.g. Positron/IPython console) — hand the
    # coroutine to a new thread that starts its own loop
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(asyncio.run, coro).result()
