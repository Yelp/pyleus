from __future__ import absolute_import, division

import logging
from operator import itemgetter

from pyleus.storm import SimpleBolt, is_tick

from .top_ten_intermediate import Query

log = logging.getLogger('top_ten_global_bolt')


class TopTenGlobalBolt(SimpleBolt):

    OUTPUT_FIELDS = ["top_ten"]

    def initialize(self, conf, context, options=None):
        self.rankings = {}

    def process_tuple(self, tup):
        if is_tick(tup):
            top_ten = self.calculate_top_ten()
            log.debug(top_ten)
            self.emit((top_ten,), anchors=[tup])
            self.cull_old_records()
            return

        interm_task_id = tup.task
        interm_top_ten_raw = tup.values[0]

        # Convert query tuples into Rankable namedtuples
        self.rankings[interm_task_id] = [
            (Query(*query), n, duration, max_duration)
            for query, n, duration, max_duration in interm_top_ten_raw
        ]

    def cull_old_records(self):
        self.rankings.clear()

    def calculate_top_ten(self):
        slowest_queries = []
        for ranking in self.rankings.values():
            slowest_queries += ranking
        slowest_queries.sort(key=itemgetter(2), reverse=True)
        return slowest_queries[:10]


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/nail/tmp/top_ten_global.log',
        filemode='a',
    )

    TopTenGlobalBolt().run()
