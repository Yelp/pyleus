from __future__ import absolute_import, division

import logging
import time
from pyleus.storm import SimpleBolt
from bandwith_monitoring.traffic_aggregator import Traffic


log = logging.getLogger('traffic_monitor')


class TrafficMonitorBolt(SimpleBolt):

    def initialize(self):
        self.records = {}

    def process_tick(self):
        log.debug("-- {0} ---------------------".format(time.time()))
        for ip_address, traffic in self.records.iteritems():
            log.debug("{0}: {1} B".format(ip_address, traffic))
        self.records.clear()

    def process_tuple(self, tup):
        record = Traffic(*tup.values)
        self.records[record.ip_address] = record.traffic


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/tmp/bandwith_monitoring_traffic_monitor.log',
        filemode='a',
    )

    TrafficMonitorBolt().run()
