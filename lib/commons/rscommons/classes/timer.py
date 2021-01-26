import time
from rscommons.util import pretty_duration


class Timer:

    def __init__(self):
        self._start_time = None
        self._stop_time = None
        self.reset()

    def reset(self):
        self._start_time = time.perf_counter()
        self._stop_time = None

    def toString(self) -> str:
        return pretty_duration(self.ellapsed())

    def ellapsed(self) -> int:
        self._stop_time = time.perf_counter()
        return self._stop_time - self._start_time
