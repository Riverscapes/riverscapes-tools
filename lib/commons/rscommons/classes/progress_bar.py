import sys
import time
import gc
import sys
import glob
import shutil
import os
from math import cos, sin, asin, sqrt, radians
from rscommons import Logger
from rscommons.util import sizeof_fmt

# Set if this environment variable is set don't show any UI
NO_UI = os.environ.get('NO_UI') is not None


class ProgressBar:
    """Progress Bar is good for giving the user feedback during a long process
    """

    def __init__(self, total, char_size=50, text=None, timer=500, byteFormat=False):
        self.log = Logger('ProgressBar')
        self.char_size = char_size
        self.text = text
        self.byteFormat = byteFormat
        self.start_time = None
        self.lastupdate = time.time()
        self.timer = 20000 if NO_UI else timer
        self.progress = 0
        self.visible = False
        self.total = total

    def update(self, progress):
        self.progress = progress
        self.output()

    def erase(self):
        if NO_UI:
            return
        if self.visible:
            sys.stdout.write("\033[F")  # back to previous line
            sys.stdout.write("\033[K")  # wipe line
        self.visible = False

    def finish(self):
        self.erase()
        if self.byteFormat:
            writestr = "Finished: {}    {}     \n".format(sizeof_fmt(self.total), self.text)
        else:
            writestr = "Finished: {:,}    {}     \n".format(self.total, self.text)
        sys.stdout.write(writestr)
        sys.stdout.flush()

    def output(self):
        first_time = False
        if self.start_time is None:
            first_time = True
            self.start_time = time.time()
        elapsed_time = 1000 * (time.time() - self.lastupdate)
        # For NO_UI we still want a keepalive signal but we don't want the quick-update progress bars
        if NO_UI:
            if first_time or elapsed_time > self.timer:
                self.lastupdate = time.time()
                writestr = ""
                time_since_begun = int(time.time() - self.start_time)
                if self.byteFormat:
                    writestr = "        PROGRESS: {} / {}    {}     Ellapsed: {:n}s\n".format(sizeof_fmt(self.progress), sizeof_fmt(self.total), self.text, time_since_begun)
                else:
                    pct_done = int(100 * (self.progress / self.total))
                    writestr = "        PROGRESS: {:,} / {:,}  ({}%)  {}     Ellapsed: {:n}s\n".format(self.progress, self.total, pct_done, self.text, time_since_begun)
                sys.stdout.write(writestr)
                sys.stdout.flush()
            return
        if first_time or elapsed_time > self.timer:
            tSize = shutil.get_terminal_size((80, 20))
            self.lastupdate = time.time()
            done = 0
            if self.total > 0:
                done = int(50 * self.progress / self.total)
            self.erase()
            writestr = ""
            if self.byteFormat:
                writestr = "\r[{}{}]    {} / {}    {}     \n".format('=' * done, ' ' * (50 - done), sizeof_fmt(self.progress), sizeof_fmt(self.total), self.text)
            else:
                writestr = "\r[{}{}]    {:,} / {:,}    {}     \n".format('=' * done, ' ' * (50 - done), self.progress, self.total, self.text)

            if len(writestr) > tSize.columns - 1:
                writestr = writestr[0:tSize.columns - 4] + '   \n'

            sys.stdout.write(writestr)
            sys.stdout.flush()
            self.visible = True
