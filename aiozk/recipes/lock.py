from aiozk import exc, Deadline

from .base_lock import BaseLock

ZNODE_LABEL = 'lock'


class Lock(BaseLock):

    async def acquire(self, timeout=None):
        deadline = Deadline(timeout)
        result = None
        while not result:
            try:
                await self.wait_in_line(ZNODE_LABEL, deadline.timeout)
                result = self.make_contextmanager(ZNODE_LABEL)
            except exc.SessionLost:
                continue
        return result
