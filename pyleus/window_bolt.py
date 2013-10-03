from collections import defaultdict, namedtuple
import threading
import time

import numpy

from .storm import SimpleBolt

PERCENTILES = (50, 75, 95, 99)
WINDOW_SIZES = (60, 300)

Record = namedtuple('Record', "key timestamp value")


class Window(object):

    def __init__(self):
        self.records = dict((window_size, []) for window_size in WINDOW_SIZES)
        self.records_lock = threading.Lock()

    def insert_record(self, record):
        with self.records_lock:
            for window_size in WINDOW_SIZES:
                self.records[window_size].append(record)

    def cull_old_events(self, start_time):
        for window_size in WINDOW_SIZES:
            records = self.records[window_size]
            lowest_time_allowed = start_time - window_size

            first_idx_to_keep = -1

            for i, record in enumerate(records):
                if record.timestamp > lowest_time_allowed:
                    break

                first_idx_to_keep = i

            del records[:first_idx_to_keep + 1]

    def compute_stats(self, start_time):
        self.cull_old_events(start_time)

        for window_size in WINDOW_SIZES:
            with self.records_lock:
                self.records[window_size].sort()
                num_records = len(self.records[window_size])

            arr = numpy.array([record.value
                for record in self.records[window_size][:num_records]])

            percentiles = numpy.percentile(arr, PERCENTILES)
            yield window_size, percentiles


class WindowBolt(SimpleBolt):
    """Calculate and emit common percentiles over the given windows

    As tuples come in, add them to a list. Periodically (every
    COMPUTE_STATS_FREQ seconds) cull and sort the list, calculate
    50/75/95/99th percentiles, and emit the stats.

    Storm configuration:
        Input tuple: (key, timestamp, value)
        Output tuple: (key, timestamp, window_size, percentiles)
    """

    COMPUTE_STATS_FREQ = 10 # seconds

    def __init__(self, *args, **kwargs):
        super(WindowBolt, self).__init__(*args, **kwargs)
        self.windows = defaultdict(Window)

    def initialize(self, conf, context):
        self._schedule_compute_stats(self.COMPUTE_STATS_FREQ)

    def process_tuple(self, tup):
        record = Record(*tup.values)
        window = self.windows[record.key]
        window.insert_record(record)

    def compute_stats(self, start_time):
        for key, window in self.windows.items():
            for window_size, percentiles in window.compute_stats(start_time):
                percentiles_str = ','.join(str(pct) for pct in percentiles)
                tup = key, start_time, window_size, percentiles_str
                self.emit(tup)

    def _compute_stats_callback(self):
        start_time = time.time()
        self.compute_stats(start_time)
        delay = max(0, self.COMPUTE_STATS_FREQ - (time.time() - start_time))
        self._schedule_compute_stats(delay)

    def _schedule_compute_stats(self, delay):
        threading.Timer(delay, self._compute_stats_callback).start()


if __name__ == '__main__':
    WindowBolt().run()
