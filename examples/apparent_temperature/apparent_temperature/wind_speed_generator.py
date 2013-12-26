from __future__ import absolute_import

import logging

from collections import namedtuple
import random

from apparent_temperature.measure_generator import MeasureGeneratorSpout

log = logging.getLogger('wind_speed_generator')

WindSpeedMeasure = namedtuple(
    "WindSpeedMeasure",
    "id_sensor timestamp wind_speed")


class WindSpeedSpout(MeasureGeneratorSpout):

    OUTPUT_FIELDS = WindSpeedMeasure

    SENSORS = {
        1042: (15, 8),
        1077: (8, 6),
        1078: (3, 7),
        1079: (8, 5),
        1082: (11, 4),
        1126: (28, 9),
        1156: (22, 5),
        1178: (12, 12),
        1201: (34, 18),
        1234: (12, 5),
        1312: (0, 12),
        1448: (20, 8),
        2089: (6, 6),
    }

    def measure(self, *args):
        return max(0, random.normalvariate(*args))

    def log(self, measure):
        log.debug("id: {0}, time: {1}, wind-speed: {2} mph"
                  .format(*measure))


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/tmp/apparent_temperature_wind_speed.log',
        filemode='a',
    )

    WindSpeedSpout().run()
