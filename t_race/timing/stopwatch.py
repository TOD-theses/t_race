from time import perf_counter_ns
from types import TracebackType
from typing import Optional


class StopWatch:
    def __enter__(self):
        self._start = perf_counter_ns()
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> bool:
        self._elapsed = perf_counter_ns() - self._start
        return False

    def elapsed_ms(self) -> int:
        return round(self._elapsed / 1_000_000)
