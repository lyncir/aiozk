from aiozk import exc, Deadline

from .base_lock import BaseLock


class SharedLock(BaseLock):
    async def acquire_read(self, timeout=None):
        deadline = Deadline(timeout)
        result = None
        while not result:
            try:
                await self.wait_in_line(
                    "read", deadline.timeout, blocked_by=("write")
                )
                result = self.make_contextmanager("read")
            except exc.SessionLost:
                continue
        return result

    async def acquire_write(self, timeout=None):
        deadline = Deadline(timeout)
        result = None
        while not result:
            try:
                await self.wait_in_line("write", deadline.timeout)
                result = self.make_contextmanager("write")
            except exc.SessionLost:
                continue
        return result
