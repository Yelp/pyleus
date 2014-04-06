from __future__ import absolute_import

import logging
from operator import itemgetter

from pyleus.storm import SimpleBolt

log = logging.getLogger('top_global_bolt')


class TopGlobalBolt(SimpleBolt):

    OPTIONS = ["N"]
    OUTPUT_FIELDS = ["top_N"]

    def initialize(self):
        self.top_N = []
        self.N = self.options["N"]

    def process_tick(self):
        log.debug("-------------")
        log.debug(self.top_N)
        self.emit((self.top_N,))
        self.top_N = []

    def process_tuple(self, tup):
        task_ranking, = tup.values
        log.debug("Task {0}: {1}".format(tup.task, task_ranking))

        # Update top N
        self.top_N.extend(task_ranking)
        self.top_N.sort(key=itemgetter(1), reverse=True)
        self.top_N = self.top_N[:self.N]


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/tmp/top_urls_top_global.log',
        filemode='a',
    )

    TopGlobalBolt().run()
