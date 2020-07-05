import asyncio
import asynctest
import logging
import pytest
import time
import types

from aiozk.exc import TimeoutError
from aiozk.states import States

log = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_creation_failure_deadlock(zk, path):
    lock = zk.recipes.Lock(path)
    await lock.ensure_path()

    async def change_state():
        await asyncio.sleep(1)
        zk.session.state.transition_to(States.SUSPENDED)

    zk.session.conn.read_loop_task.cancel()
    zk.session.conn.read_loop_task = None
    # wait for that read loop task is cancelled
    await asyncio.sleep(1)

    asyncio.create_task(change_state())
    lock_acquired = False
    with pytest.raises(TimeoutError):
        # lock is created at zookeeper but response can not be returned because
        # read loop task was cancelled.
        async with await lock.acquire(timeout=2):
            lock_acquired = True

    assert not lock_acquired
    assert not lock.owned_paths

    lock2 = zk.recipes.Lock(path)
    try:
        async with await lock2.acquire(timeout=2):
            lock_acquired = True

        assert lock_acquired
    finally:
        await zk.deleteall(path)


@pytest.mark.asyncio
async def test_acquisition_failure_deadlock(zk, path):
    lock = zk.recipes.Lock(path)
    await lock.ensure_path()

    async with await lock.acquire(timeout=0.5):
        lock2 = zk.recipes.Lock(path)
        await lock2.ensure_path()

        lock2.analyze_siblings_orig = lock2.analyze_siblings

        # analyze_siblings() is called by .wait_in_line()
        async def analyze_siblings_fail(self):
            await self.analyze_siblings_orig()
            raise TimeoutError('fail', 1234)

        lock2.analyze_siblings = types.MethodType(analyze_siblings_fail, lock2)

        lock_acquired = False
        with pytest.raises(TimeoutError):
            async with await lock2.acquire(timeout=0.5):
                lock_acquired = True

        assert not lock_acquired

    try:
        lock3 = zk.recipes.Lock(path)
        await lock.ensure_path()
        lock_acquired2 = False
        async with await lock3.acquire(timeout=0.5):
            lock_acquired2 = True

        assert lock_acquired2
    finally:
        await zk.deleteall(path)


@pytest.mark.asyncio
async def test_timeout_accuracy(zk, path):
    lock = zk.recipes.Lock(path)

    async with await lock.acquire():
        lock2 = zk.recipes.Lock(path)
        analyze_siblings = lock2.analyze_siblings
        lock2.analyze_siblings = asynctest.CoroutineMock()

        async def slow_analyze():
            await asyncio.sleep(0.5)
            return await analyze_siblings()

        lock2.analyze_siblings.side_effect = slow_analyze

        acquired = False
        start = time.perf_counter()
        with pytest.raises(TimeoutError):
            async with await lock2.acquire(timeout=0.5):
                acquired = True

        elapsed = time.perf_counter() - start

    await zk.deleteall(path)

    assert not acquired
    assert elapsed < 1


@pytest.mark.asyncio
async def test_acquire_lock(zk, path):
    lock = zk.recipes.Lock(path)
    acquired = False
    try:
        async with await lock.acquire():
            acquired = True

        assert acquired
    finally:
        await zk.deleteall(path)


@pytest.mark.asyncio
async def test_async_context_manager(zk, path):
    acquired = False
    try:
        async with zk.recipes.Lock(path, timeout=1):
            acquired = True
            await asyncio.sleep(1)

        assert acquired
    finally:
        await zk.deleteall(path)


@pytest.mark.asyncio
async def test_async_context_manager_deadlock(zk, path):
    acquired = False
    acquired2 = False
    try:
        async with zk.recipes.Lock(path, timeout=1):
            acquired = True

            with pytest.raises(TimeoutError):
                async with zk.recipes.Lock(path, timeout=1):
                    acquired2 = True

        assert acquired
        assert not acquired2
    finally:
        await zk.deleteall(path)


@pytest.mark.asyncio
async def test_async_context_manager_reentrance(zk, path):
    """re-entrance is not permitted"""
    lock = zk.recipes.Lock(path, timeout=1)
    acquired = False
    acquired2 = False
    try:
        async with lock:
            acquired = True
            with pytest.raises(ValueError):
                async with lock:
                    acquired2 = True

        assert acquired
        assert not acquired2
    finally:
        await zk.deleteall(path)


@pytest.mark.asyncio
async def test_async_context_manager_reuse(zk, path):
    lock = zk.recipes.Lock(path, timeout=1)
    acquired = False
    acquired2 = False
    try:
        async with lock:
            acquired = True

        async with lock:
            acquired2 = True

        assert acquired
        assert acquired2
    finally:
        await zk.deleteall(path)


@pytest.mark.asyncio
async def test_async_context_manager_contention(zk, path):
    CONTENDERS = 8
    done = 0
    cond = asyncio.Condition()

    async def create_contender():
        async with zk.recipes.Lock(path):
            async with cond:
                await asyncio.sleep(0.1)
                nonlocal done
                done += 1
                cond.notify()

    for _ in range(CONTENDERS):
        asyncio.create_task(create_contender())

    try:
        async with cond:
            await asyncio.wait_for(cond.wait_for(lambda: done == CONTENDERS),
                                   timeout=2)

        assert CONTENDERS == done
    finally:
        await zk.deleteall(path)
