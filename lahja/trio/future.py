from concurrent.futures import CancelledError
from types import TracebackType
from typing import Any, Optional, Tuple

import trio


class InvalidState(Exception):
    pass


class EMPTY:
    pass


class Future:
    _error: Optional[Tuple[
        Optional[BaseException],
        Optional[TracebackType],
    ]] = None

    def __init__(self):
        self._error = None
        self._result = EMPTY
        self._cancelled = False
        self._done = trio.Event()

    def cancelled(self) -> bool:
        return self._cancelled

    def done(self) -> bool:
        return self._done.is_set()

    def result(self) -> Any:
        if self.cancelled():
            raise CancelledError
        elif self._result is not EMPTY:
            return self._result
        elif self._error is not None:
            exc_value, exc_tb = self._error
            raise exc_value.with_traceback(exc_tb)
        else:
            raise InvalidState()

    def set_result(self, result: Any) -> None:
        self._result = result
        self._done.set()

    def set_exception(self, exc_value, exc_tb) -> None:
        self._error = exc_value, exc_tb
        self._done.set()

    def cancel(self) -> None:
        if self.done():
            return False
        self._done.set()
        self._cancelled = True
        return True

    def exception(self) -> BaseException:
        if self.cancelled():
            raise CancelledError
        elif self.done():
            return self._error[0]
        else:
            raise InvalidState()

    async def _await(self):
        await self._done.wait()
        return self.result()

    def __await__(self) -> Any:
        return self._await().__await__()
