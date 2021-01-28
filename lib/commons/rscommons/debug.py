from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor
from time import sleep
from datetime import datetime
import csv
import numpy as np
import os
import resource
import matplotlib.pyplot as plt

# https://medium.com/survata-engineering-blog/monitoring-memory-usage-of-a-running-python-program-49f027e3d1ba


class MemoryMonitor:
    def __init__(self, logfile: str, loop_delay=1):
        self.keep_measuring = True
        self.filepath = logfile
        self.loop_delay = loop_delay

    def init_file(self):
        with open(self.filepath, 'w') as csvfile:
            csvfile.write('datetime,r_usage_self,r_usage_children\n')

    def measure_usage(self):
        self.init_file()
        max_usage_self = 0
        max_usage_children = 0
        while self.keep_measuring:
            r_usage_self = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            r_usage_children = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
            max_usage_self = max(max_usage_self, r_usage_self)
            max_usage_children = max(max_usage_children, r_usage_children)
            with open(self.filepath, 'a') as csvfile:
                csvfile.write('{}, {}, {}\n'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), r_usage_self, r_usage_children))

            sleep(self.loop_delay)

        # with open(self.filepath, 'a') as csvfile:
        #     csvfile.write('TOTAL, {}, {}\n'.format(max_usage_self, max_usage_children))

        return max_usage_self, max_usage_children

    def write_plot(self, imgpath: str):
        x = []
        data = {}
        with open(self.filepath) as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                for key in row.keys():
                    if key == 'datetime':
                        x.append(row[key])
                    elif key not in data:
                        data[key] = [int(row[key]) / (1000000)]
                    else:
                        data[key].append(int(row[key]) / (1000000))

        chart_title = 'Memory Usage'
        xlabel = 'time'
        ylabel = 'Mb'

        plt.clf()

        for key in data.keys():
            plt.plot(x, data[key], label=key)
        plt.title = chart_title
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)

        plt.xticks(rotation=45, ticks=np.arange(0, len(x) + 1, 25))
        plt.grid(True)

        plt.legend(loc='upper left')
        # plt.tight_layout()

        plt.savefig(imgpath)


def ThreadRun(callback, logfile, *args):
    with ThreadPoolExecutor() as executor:
        monitor = MemoryMonitor(logfile, 1)
        mem_thread = executor.submit(monitor.measure_usage)
        try:
            fn_thread = executor.submit(callback, *args)
            result = fn_thread.result()
        finally:
            monitor.keep_measuring = False
            max_usage_self, max_usage_children = mem_thread.result()


<< << << < HEAD
monitor.write_plot(os.path.splitext(logfile)[0] + '.png')
== == == =
monitor.write_plot(os.path.basename(logfile))
>>>>>> > initial memusage profiling
return result, max_usage_self, max_usage_children
