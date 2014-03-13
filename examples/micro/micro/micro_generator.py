import logging
import random
import time

from pyleus.storm import Spout

log = logging.getLogger('microtransactions')

CURRENCY = ["dollar", "dollar", "dollar", "euro", "pound"]
VALUES = {
    "dollar": [0.50, 0.99, 1.37],
    "pound": [0.36, 0.72, 1.0],
    "euro": [0.30, 0.61, 0.84],
}


class MicroGeneratorSpout(Spout):

    OUTPUT_FIELDS = {
        "dollar": ["value"],
        "pound": ["value"],
        "euro": ["value"],
    }

    def next_tuple(self):
        time.sleep(0.01)
        currency = random.choice(CURRENCY)
        value = random.choice(VALUES[currency])
        log.debug("{0} {1}".format(value, currency))
        self.emit((value,), currency)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/tmp/micro_microtransactions.log',
        format="%(message)s",
        filemode='a',
    )

    MicroGeneratorSpout().run()
