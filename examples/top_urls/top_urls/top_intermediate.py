from __future__ import absolute_import

from collections import defaultdict
import logging
from operator import itemgetter
import time

from pyleus.storm import SimpleBolt

from top_urls.fields import Fields

log = logging.getLogger('top_N_intermediate_bolt')


class TopIntermediateBolt(SimpleBolt):

    OPTIONS = ["N", "time_window", "min_records"]
    OUTPUT_FIELDS = ["top_N"]

    def initialize(self):
        self.urls = defaultdict(list)
        self.time_window = self.options["time_window"]
        self.N = self.options["N"]
        self.min_records = self.options["min_records"]

    def process_tick(self):
        self.cull_old_records()
        top_N = self.calculate_top_N()
        log.debug("-------------")
        log.debug(top_N)
        self.emit((top_N,))

    def process_tuple(self, tup):
        fields = Fields(*tup.values)
        self.urls[fields.url].append(fields.timestamp)
        self.urls[fields.url].sort() # Sorts by timestamp

    def cull_old_records(self):
        now = time.time()

        for url, records in self.urls.iteritems():
            earliest_time_allowed = now - (self.time_window)
            first_idx_to_keep = -1

            for i, timestamp in enumerate(records):
                if timestamp > earliest_time_allowed:
                    break

                first_idx_to_keep = i

            del records[:first_idx_to_keep + 1]

    def calculate_top_N(self):
        top_urls = sorted((
            (url, len(records))
            for url, records in self.urls.iteritems()
            if records # avoid ZeroDivisionError
            and len(records) >= self.min_records
        ), key=itemgetter(1), reverse=True)

        return top_urls[:self.N]


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/tmp/top_urls_top_intermediate.log',
        filemode='a',
    )

    TopIntermediateBolt().run()
