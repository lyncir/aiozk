from aiozk import exc, Deadline

from .base_lock import BaseLock

ZNODE_LABEL = 'lock'


class Lock(BaseLock):

    def __init__(self, base_path, timeout=None):
        super().__init__(base_path)
        self.timeout = timeout

    async def __aenter__(self):
        await self._acquire2(timeout=self.timeout)

    async def __aexit__(self, exc_type, exc, tb):
        await self._release2()

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
