from __future__ import absolute_import

import datetime
import logging

from pyleus.storm import SimpleBolt

log = logging.getLogger('log_top')


class LogTopBolt(SimpleBolt):

    def process_tuple(self, tup):
        top, = tup.values
        log.debug("---{0}---".format(str(datetime.datetime.now())))
        for record in top:
            log.debug(record)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/tmp/top_urls_result.log',
        filemode='a',
    )

    LogTopBolt().run()
