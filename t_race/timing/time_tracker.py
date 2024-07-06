import csv
from pathlib import Path
from types import TracebackType
from typing import Optional
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
        self._csv_writer.writerow(("type", "component", "step", "elapsed_ms"))
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

    def step(self, component: str, step: str) -> "TimeTrackerWatchStep":
        return TimeTrackerWatchStep(self, component, step)

    def component(self, component: str) -> "TimeTrackerWatchComponent":
        return TimeTrackerWatchComponent(self, component)

    def save_time_step_ms(self, component: str, step: str, elapsed_ms: int):
        self._csv_writer.writerow(("step", component, step, elapsed_ms))  # type: ignore

    def save_time_component_ms(self, component: str, elapsed_ms: int):
        self._csv_writer.writerow(("component", component, "", elapsed_ms))  # type: ignore


class TimeTrackerWatchComponent(StopWatch):
    def __init__(self, time_tracker: TimeTracker, component: str):
        self._time_tracker = time_tracker
        self._component = component

    @override
    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> bool:
        super().__exit__(exc_type, exc_value, traceback)
        self._time_tracker.save_time_component_ms(self._component, self.elapsed_ms())
        return False


class TimeTrackerWatchStep(StopWatch):
    def __init__(self, time_tracker: TimeTracker, component: str, step: str):
        self._time_tracker = time_tracker
        self._component = component
        self._step = step

    @override
    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> bool:
        super().__exit__(exc_type, exc_value, traceback)
        self._time_tracker.save_time_step_ms(
            self._component, self._step, self.elapsed_ms()
        )
        return False
