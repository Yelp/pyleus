from __future__ import absolute_import, division

from collections import defaultdict, namedtuple
import logging
from operator import itemgetter
import time

from pyleus.storm import SimpleBolt, is_tick

from .fields import Fields

log = logging.getLogger('top_ten_intermediate_bolt')

MIN_RECORDS = 3

Query = namedtuple('Query', "desc loc")
Record = namedtuple('Record', "timestamp duration")


class TopTenIntermediateBolt(SimpleBolt):

    OUTPUT_FIELDS = ["top_ten"]

    def initialize(self, conf, context, options=None):
        self.queries = defaultdict(list)

    def process_tuple(self, tup):
        if is_tick(tup):
            self.cull_old_records()
            top_ten = self.calculate_top_ten()
            self.emit((top_ten,), anchors=[tup])
            return

        fields = Fields(*tup.values)
        log.debug(fields)

        query = Query(fields.query_desc, fields.query_loc)
        record = Record(fields.timestamp, fields.duration)

        self.queries[query].append(record)
        self.queries[query].sort() # Sorts by timestamp

    def cull_old_records(self):
        now = time.time()

        for query, records in self.queries.iteritems():
            earliest_time_allowed = now - (60 * 10) # 10 minutes ago
            first_idx_to_keep = -1

            for i, record in enumerate(records):
                if record.timestamp > earliest_time_allowed:
                    break

                first_idx_to_keep = i

            del records[:first_idx_to_keep + 1]

    def calculate_top_ten(self):
        slowest_queries = sorted((
            (query, len(records), sum(r.duration for r in records) / len(records), max(r.duration for r in records))
            for query, records in self.queries.iteritems()
                if records # avoid ZeroDivisionError
                    and len(records) >= MIN_RECORDS
        ), key=itemgetter(2), reverse=True)

        return slowest_queries[:10]


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/nail/tmp/top_ten_intermediate.log',
        filemode='a',
    )

    TopTenIntermediateBolt().run()
