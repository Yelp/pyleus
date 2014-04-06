from __future__ import absolute_import

from array import array
from collections import defaultdict
import logging
import time

from pyleus.storm import SimpleBolt

from apparent_temperature.measure_generator import Measure

log = logging.getLogger('wind_chill_bolt')


def _wind_chill(temp, wind):
    wind_pow = pow(wind, 0.16)
    chill = 35.74 + 0.6215 * temp - 35.75 * wind_pow + 0.4275 * temp * wind_pow
    return round(chill, 2)


class WindChillBolt(SimpleBolt):

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
                log.debug("id: {0}, wind-chill: {1} F"
                          .format(idx, _wind_chill(*vals)))
        self.join.clear()

    def process_tuple(self, tup):
        measure = Measure(*tup.values)

        # accept only measures within the update time window
        if measure.timestamp < time.time() - self.conf.tick_tuple_freq:
            return

        # Since all components of this topology have a single output stream,
        # the component id can be used to discriminate between the two streams
        if tup.comp == "temperature-monitor":
            if measure.measure <= 50:
                self.join[measure.id_sensor][0] = measure.measure
        elif tup.comp == "wind-speed-monitor":
            if measure.measure > 3:
                self.join[measure.id_sensor][1] = measure.measure
        else:
            raise ValueError("Unexpected component stream: {0}"
                             .format(tup.comp))


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/tmp/apparent_temperature_wind_chill.log',
        filemode='a',
    )

    WindChillBolt().run()
