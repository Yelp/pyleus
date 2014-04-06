from __future__ import absolute_import

from array import array
from collections import defaultdict
import logging
import time

from pyleus.storm import SimpleBolt

from apparent_temperature.measure_generator import Measure

log = logging.getLogger('heat_index_bolt')

CX = (
    -42.379,
    2.04901523,
    10.14333127,
    -0.22475541,
    -6.83783 * 0.001,
    -5.481717 * 0.01,
    1.22874 * 0.001,
    8.5282 * pow(10, -4),
    -1.99 * pow(10, -6))


def _heat_index(temp, hum):
    temp2 = temp * temp
    hum2 = hum * hum
    factors = (1, temp, hum, temp * hum, temp2, hum2, temp2 * hum, temp * hum2,
               temp2 * hum2)
    heat = 0
    for idx in range(len(CX)):
        heat += CX[idx] * factors[idx]
    return round(heat, 2)


class HeatIndexBolt(SimpleBolt):

    def initialize(self):
        # The two measures to be joined will be stored togheter in an array,
        # where the inf value means that the measure is missing
        default = lambda: array('f', [float('inf')] * 2)

        # The array will be stored in a dictionary using as key the sensor_id
        # which produced the measures
        self.join = defaultdict(default)

    def process_tick(self):
        for idx, vals in self.join.iteritems():
            if vals[0] != float('inf') and vals[1] != float('inf'):
                log.debug("id: {0}, heat-index: {1} F"
                          .format(idx, _heat_index(*vals)))
        self.join.clear()

    def process_tuple(self, tup):
        measure = Measure(*tup.values)

        # accept only measures within the update time window
        if measure.timestamp < time.time() - self.conf.tick_tuple_freq:
            return

        # Since all components of this topology have a single output stream,
        # the component id can be used to discriminate between the two streams
        if tup.comp == "temperature-monitor":
            if measure.measure > 80:
                self.join[measure.id_sensor][0] = measure.measure
        elif tup.comp == "humidity-monitor":
            if measure.measure > 40:
                self.join[measure.id_sensor][1] = measure.measure
        else:
            raise ValueError("Unexpected component stream: {0}"
                             .format(tup.comp))

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/tmp/apparent_temperature_heat_index.log',
        filemode='a',
    )

    HeatIndexBolt().run()
