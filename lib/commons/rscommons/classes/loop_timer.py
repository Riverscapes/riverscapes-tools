"""_summary_
"""
import sys
import time
import shutil
import os
from math import cos, sin, asin, sqrt, radians
from rscommons import Logger

# Set if this environment variable is set don't show any UI
NO_UI = os.environ.get('NO_UI') is not None


class LoopTimer:
    """Timer is good for figuring out how long loops are running
    """

    def __init__(self, name, logger=Logger("TIMER"), useMs=False, timer=500):
        self.useMs = useMs
        self.logger = logger
        self.timer = 20000 if NO_UI else timer
        self.start = time.time()
        self.name = name
        self.lastupdate = time.time()
        self.visible = False
        # average
        self.total = 0
        self.ticks = 0

    def reset(self):
        self.start = time.time()

    def tick(self):
        """Indicate that a tick has happened
        """
        self.total += time.time() - self.start
        self.ticks += 1
        self.start = time.time()

    def print(self, midStr=None, useMs=False):
        """Tick Print

        Args:
            midStr (_type_, optional): _description_. Defaults to None.
            useMs (bool, optional): _description_. Defaults to False.
        """
        if NO_UI:
            return
        if self.visible:
            self.erase()
        middleStr = "::{}".format(midStr) if midStr else ""
        if self.ticks > 0:
            avg = self.total / self.ticks
            if useMs or self.useMs:
                avg = avg * 1000
            self.logger.debug("{}{}:: Count: {:,}, Total Time: {:f}s, Average: {:f}{}"
                              .format(self.name, middleStr, self.ticks, self.total, avg, "ms" if self.useMs else "s"))
        else:
            ellapsed = time.time() - self.start
            if useMs or self.useMs:
                ellapsed = ellapsed * 1000
            self.logger.debug("{}{}::{:f}{}".format(self.name, middleStr, ellapsed, "ms" if self.useMs else "s"))

    def erase(self):
        if self.visible:
            sys.stdout.write("\033[F")  # back to previous line
            sys.stdout.write("\033[K")  # wipe line
        self.visible = False

    def progprint(self, midStr=None):
        if NO_UI:
            return
        middleStr = "::{}".format(midStr) if midStr else ""
        since_last = 1000 * (time.time() - self.lastupdate)
        if self.ticks > 0 and since_last > self.timer:
            self.erase()
            avg = self.total / self.ticks
            if self.useMs:
                avg = avg * 1000
            tSize = shutil.get_terminal_size((80, 20))
            self.lastupdate = time.time()
            writestr = "\rAVG_TIMER::{}{}:: Count: {:,}, Total Time: {:f}s, Average: {:f}{}     \n".format(self.name, middleStr, self.ticks, self.total, avg, "ms" if self.useMs else "s")
            if len(writestr) > tSize.columns - 1:
                writestr = writestr[0:tSize.columns - 4] + '   \n'
            sys.stdout.write(writestr)
            sys.stdout.flush()
            self.visible = True
