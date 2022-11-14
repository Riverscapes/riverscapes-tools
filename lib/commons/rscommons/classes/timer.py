from __future__ import annotations
import time
import os
from typing import Dict, List
import csv
from copy import copy
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


class Borg:
    _shared_state = {}

    def __init__(self):
        self.__dict__ = self._shared_state


class TimerBuckets(Borg):
    timers: Dict(str, float)
    ticks: List(TimerBuckets.Tick)
    """Useful for when you want to time things into buckets. Includes tick function for using within loops

        with TimerBuckets("bucketname"):
            EVERYTHING IN HERE GETS COUNTED IN THIS BUCKET

    """

    class Tick:
        timers: Dict(str, float) = {}
        total = 0
        meta = {}

        def __init__(self, timers, total, meta):
            self.timers = timers
            self.total = total
            self.meta = meta

    def __init__(self, key=None, meta: Dict = None, csv_file: str = None):
        Borg.__init__(self)
        if "timers" not in self.__dict__:
            self.timers = {}
            self.total = 0
            self.tick_total = 0
            self.ticks = []

        self.timer = Timer()

        self.key = key
        if csv_file is not None:
            self.csv_file = csv_file
        if meta is not None:
            self.meta = meta

    def reset(self):
        self.timers = {}
        self.total = 0
        self.tick_total = 0
        self.ticks = []

    def tick(self, meta: Dict = {}):
        """For loops you can call this to freeze these timers to a row
        """
        # If we only tick when there's something to do then we can tick at the top or the bottom of the loop
        # this is helpful to catch errors partway through the loop
        if self.tick_total > 0 or len(self.timers.keys()) > 0:
            self.ticks.append(TimerBuckets.Tick(self.timers, self.tick_total, self.meta))

        self.meta = meta
        self.timers = {}
        self.tick_total = 0
        self.write_csv()

    def __enter__(self):
        """Behaviour on open when using the "with TimerBuckets():" Syntax
        """
        if self.key is not None:
            if self.key not in self.timers:
                self.timers[self.key] = 0
            self.timer.reset()

    def __exit__(self, _type, _value, _traceback):
        """Behaviour on close when using the "with TimerBuckets():" Syntax
        """
        # self.log.debug('__exit__ called. Cleaning up.')
        if self.key is not None:
            self.timers[self.key] += self.timer.ellapsed()
            self.total += self.timer.ellapsed()
            self.tick_total += self.timer.ellapsed()

    def write_csv(self):
        """Write all our Timer ticks to a CSV file

        Args:
            csv_path (str): _description_
        """
        if self.csv_file is None:
            return

        field_names = ["tick"]
        # First add all the meta keys to the dictionary
        csv_arr = []
        for idx, row in enumerate(self.ticks):
            csv_row = {"tick": idx}
            for k in row.meta.keys():
                if k not in field_names:
                    field_names.append(k)
                csv_row[k] = row.meta[k]
            for k in row.timers.keys():
                kname = f'tmr_{k}'
                if kname not in field_names:
                    field_names.append(kname)
                csv_row[kname] = round(row.timers[k], 1)
            csv_arr.append(csv_row)
            csv_row["timer_total"] = round(row.total, 1)
        field_names.append('timer_total')

        with open(self.csv_file, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=field_names)
            writer.writeheader()
            for row in csv_arr:
                writer.writerow(row)
