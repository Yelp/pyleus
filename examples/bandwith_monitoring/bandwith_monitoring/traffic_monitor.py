from __future__ import absolute_import, division

import logging

from pyleus.storm import SimpleBolt, is_tick
from bandwith_monitoring.traffic_aggregator import Traffic


log = logging.getLogger('traffic_monitor')


class TrafficMonitorBolt(SimpleBolt):

    def initialize(self, conf, context, _):
        self.records = {}

    def process_tuple(self, tup):
        # a tick tuple is triggered every 2 second, as declared in the yaml
        if is_tick(tup):
            log.debug("-------------------------------")
            for ip_address, traffic in self.records.iteritems():
                log.debug("{0}: {1} B".format(ip_address, traffic))
            self.records.clear()
            return

        # when a normal tuple is received
        record = Traffic(*tup.values)
        self.records[record.ip_address] = record.traffic


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/tmp/traffic_monitor.log',
        filemode='a',
    )

    TrafficMonitorBolt().run()
