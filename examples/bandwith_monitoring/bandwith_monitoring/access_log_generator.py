from __future__ import absolute_import

import logging

from collections import namedtuple
from random import choice
import time

from pyleus.storm import Spout


log = logging.getLogger('access_log_generator')

Request = namedtuple("Request", "ip_address request size")

_dash_to_zero = lambda size: 0 if size == "-" else long(size)


class AccessLogGeneratorSpout(Spout):

    OPTIONS = ["base_log"]
    OUTPUT_FIELDS = Request

    def initialize(self):
        self.ip = set()
        self.request = set()
        self.size = set()
        with open(self.options["base_log"], "r") as base_log:
            for line in base_log:
                line = line.split()
                self.ip.add(line[0])
                self.request.add(line[4])
                self.size.add(_dash_to_zero(line[-1]))
            self.ip = list(self.ip)
            self.request = list(self.request)
            self.size = list(self.size)

    def next_tuple(self):
        time.sleep(0.01)
        request = Request(
            choice(self.ip),
            choice(self.request),
            choice(self.size))
        log.debug(request)
        self.emit(request)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/tmp/bandwith_monitoring_access_log_generator.log',
        filemode='a',
    )

    AccessLogGeneratorSpout().run()
