from __future__ import absolute_import

import logging

from collections import namedtuple
import random

from apparent_temperature.measure_generator import MeasureGeneratorSpout

log = logging.getLogger('temperature_generator')

TemperatureMeasure = namedtuple(
    "TemperatureMeasure",
    "id_sensor timestamp temperature")


class TemperatureSpout(MeasureGeneratorSpout):

    OUTPUT_FIELDS = TemperatureMeasure

    SENSORS = {
        1042: (48, 12),
        1077: (75, 10),
        1078: (84, 20),
        1079: (67, 8),
        1082: (72, 4),
        1126: (38, 10),
        1156: (81, 5),
        1178: (37, 11),
        1201: (43, 14),
        1234: (29, 16),
        1312: (31, 6),
        1448: (88, 8),
        2089: (86, 6),
    }

    def measure(self, *args):
        return random.normalvariate(*args)

    def log(self, measure):
        log.debug("id: {0}, time: {1}, temperature: {2} F"
                  .format(*measure))


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/tmp/apparent_temperature_temperature.log',
        filemode='a',
    )

    TemperatureSpout().run()
