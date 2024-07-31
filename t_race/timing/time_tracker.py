import csv
from pathlib import Path
from types import TracebackType
from typing import Optional, Sequence
from typing_extensions import override

from t_race.timing.stopwatch import StopWatch


class TimeTracker:
    def __init__(self, csv_path: Path) -> None:
        self._csv_path = csv_path
        self._csv_writer = None
        self._csv_file = None

    def __enter__(self) -> "TimeTracker":
        self._csv_file = open(self._csv_path, "w", newline="")
        self._csv_writer = csv.writer(self._csv_file)
        self._csv_writer.writerow(("task", "elapsed_ms"))
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> bool:
        if self._csv_file is not None and not self._csv_file.closed:
            self._csv_file.close()
        return False

    def task(self, task: Sequence[str]) -> "TimeTrackerWatch":
        return TimeTrackerWatch(self, task)

    def save_time_ms(self, task: Sequence[str], elapsed_ms: int):
        self._csv_writer.writerow(("|".join(task), elapsed_ms))  # type: ignore


class TimeTrackerWatch(StopWatch):
    def __init__(self, time_tracker: TimeTracker, task: Sequence[str]):
        self._time_tracker = time_tracker
        self._task = task

    @override
    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> bool:
        super().__exit__(exc_type, exc_value, traceback)
        self._time_tracker.save_time_ms(self._task, self.elapsed_ms())
        return False
