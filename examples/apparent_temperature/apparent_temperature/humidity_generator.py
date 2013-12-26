from __future__ import absolute_import

import logging

from collections import namedtuple
import random

from apparent_temperature.measure_generator import MeasureGeneratorSpout

log = logging.getLogger('humidity_generator')

HumidityMeasure = namedtuple(
    "HumidityMeasure",
    "id_sensor timestamp humidity")


class HumiditySpout(MeasureGeneratorSpout):

    OUTPUT_FIELDS = HumidityMeasure

    SENSORS = {
        1042: (56, 17),
        1077: (47, 22),
        1078: (22, 19),
        1079: (12, 15),
        1082: (67, 15),
        1126: (70, 12),
        1156: (51, 19),
        1178: (43, 14),
        1201: (57, 11),
        1234: (55, 7),
        1312: (12, 9),
        1448: (56, 22),
        2089: (32, 30),
    }

    def measure(self, *args):
        return min(100, random.normalvariate(*args))

    def log(self, measure):
        log.debug("id: {0}, time: {1}, humidity: {2}%"
                  .format(*measure))


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/tmp/apparent_temperature_humidity.log',
        filemode='a',
    )

    HumiditySpout().run()
