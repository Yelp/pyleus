from __future__ import absolute_import, division

from array import array
import logging
from collections import defaultdict
from collections import namedtuple

from pyleus.storm import SimpleBolt
from bandwith_monitoring.access_log_generator import Request


log = logging.getLogger('traffic_aggregator')

Traffic = namedtuple("Traffic", "ip_address traffic")


class SlotsCounter(object):
    def __init__(self, size):
        self.slots = array('I', [0] * size)
        self.counter = 0

    def __repr__(self):
        return "{0} : {1}".format(self.counter, self.slots)


class TrafficAggregatorBolt(SimpleBolt):

    OUTPUT_FIELDS = Traffic
    OPTIONS = ["time_window", "threshold"]

    def initialize(self):
        self.time_window = self.options["time_window"]
        self.threshold = self.options["threshold"]
        if self.time_window % self.conf.tick_tuple_freq != 0:
            raise ValueError("Time window must be a multiple of"
                             " tick_tuple_freq_secs")
        self.N = int(self.time_window / self.conf.tick_tuple_freq)
        self.slot_counters = defaultdict(lambda: SlotsCounter(self.N))
        self.curr = 0

    def process_tick(self):
        for ip_address, slcnt in self.slot_counters.iteritems():
            if slcnt.counter > self.threshold:
                log.debug(Traffic(ip_address, slcnt.counter))
                self.emit(Traffic(ip_address, slcnt.counter))
        self.advance_window()

    def process_tuple(self, tup):
        request = Request(*tup.values)
        slcnt = self.slot_counters[request.ip_address]
        slcnt.counter += request.size
        slcnt.slots[self.curr] += request.size

    def advance_window(self):
        log.debug("----------------------------")
        log.debug("BEFORE (curr={0}): {1}"
                  .format(self.curr, str(self.slot_counters)))

        self.curr = (self.curr + 1) % self.N
        for ip_address, slcnt in self.slot_counters.iteritems():
            slcnt.counter -= slcnt.slots[self.curr]
            slcnt.slots[self.curr] = 0
        for ip_address in self.slot_counters.keys():
            if self.slot_counters[ip_address].counter == 0:
                del self.slot_counters[ip_address]

        log.debug("AFTER (curr={0}): {1}"
                  .format(self.curr, self.slot_counters))


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/tmp/bandwith_monitoring_traffic_aggregator.log',
        filemode='a',
    )

    TrafficAggregatorBolt().run()
