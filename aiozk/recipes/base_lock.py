import asyncio
import logging

from aiozk import exc, states, Deadline
from .sequential import SequentialRecipe

ZNODE_LABEL = 'lock'
log = logging.getLogger(__name__)


class BaseLock(SequentialRecipe):
    def __init__(self, base_path):
        super().__init__(base_path)
        self._session_lost_future = None
        self._alert_task = None
        self._used = False

    async def wait_in_line(self, znode_label, timeout=None, blocked_by=None):
        deadline = Deadline(timeout)
        await self.create_unique_znode(znode_label)

        while True:
            if deadline.has_passed:
                await self.delete_unique_znode(znode_label)
                raise exc.TimeoutError

            try:
                owned_positions, contenders = await self.analyze_siblings()
            except exc.TimeoutError:
                # state may change to SUSPENDED
                await self.client.session.state.wait_for(states.States.CONNECTED)
                continue

            if znode_label not in owned_positions:
                raise exc.SessionLost

            blockers = contenders[:owned_positions[znode_label]]
            if blocked_by:
                blockers = [
                    contender for contender in blockers
                    if self.determine_znode_label(contender) in blocked_by
                ]

            if not blockers:
                break

            try:
                await self.wait_on_sibling(blockers[-1], deadline.timeout)
            except exc.TimeoutError:
                # state may change to SUSPENDED
                await self.client.session.state.wait_for(states.States.CONNECTED)
                continue

    def make_contextmanager(self, znode_label):
        state = {"acquired": True}

        def still_acquired():
            return state["acquired"]

        state_fut = self.client.session.state.wait_for(states.States.LOST)

        async def handle_session_loss():
            await state_fut
            if not state["acquired"]:
                return

            log.warning(
                "Session expired at some point, lock %s no longer acquired.",
                self)
            state["acquired"] = False

        fut = asyncio.ensure_future(handle_session_loss(),
                                    loop=self.client.loop)

        async def on_exit():
            state["acquired"] = False
            if not fut.done():
                if not state_fut.done():
                    self.client.session.state.remove_waiting(
                        state_fut, states.States.LOST)
                fut.cancel()

            await self.delete_unique_znode(znode_label)

        class Lock:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                await on_exit()

        return Lock()

    async def _acquire2(self, timeout=None):
        if self._used:
            raise ValueError('re-entrance is not permitted')

        deadline = Deadline(timeout)
        while True:
            try:
                await self.wait_in_line(ZNODE_LABEL, deadline.timeout)
                break
            except exc.SessionLost:
                continue
        self._session_lost_future = self.client.session.state.wait_for(
            states.States.LOST)
        self._alert_task = asyncio.create_task(self._alert_session_lost())
        self._used = True

    async def _alert_session_lost(self):
        await self._session_lost_future
        log.warning(
            "Session expired at some point, lock %s no longer acquired.", self)

    async def _release2(self):
        if not self._session_lost_future.done():
            self.client.session.state.remove_waiting(self._session_lost_future,
                                                     states.States.LOST)
            self._sesion_lost_future = None

        if not self._alert_task.done():
            self._alert_task.cancel()
            self._alert_task = None

        await self.delete_unique_znode(ZNODE_LABEL)
        self._used = False


class LockLostError(exc.ZKError):
    pass
