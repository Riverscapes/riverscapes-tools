import time


class Timer:

    def __init__(self):
        self.reset()

    def reset(self):
        self._start_time = time.perf_counter()
        self._stop_time = None

    def ellapsed(self):
        self._stop_time = time.perf_counter()
        return self._stop_time - self._start_time
