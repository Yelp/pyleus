from __future__ import absolute_import

from collections import namedtuple
import logging
import random
import time

from pyleus.storm import Spout

log = logging.getLogger(__name__)

# All the output fields namedtuples defined by the subclasses should be
# compatible with the following
Measure = namedtuple("Measure", "id_sensor timestamp measure")


class MeasureGeneratorSpout(Spout):
    """Base class for all the spouts generating fake data as if it was coming
    from sensors.
    """

    # Override in subclass
    SENSORS = None

    RELIABLE = (True, True, True, True, False)

    def measure(self, *args):
        raise NotImplementedError

    def log(self, measure):
        raise NotImplementedError

    def next_tuple(self):
        time.sleep(2)
        for idx, vals in self.SENSORS.iteritems():
            if random.choice(self.RELIABLE):
                time.sleep(random.uniform(0, 0.05))
                measure = (idx, time.time(), round(self.measure(*vals), 2))
                self.log(measure)
                self.emit(measure)
