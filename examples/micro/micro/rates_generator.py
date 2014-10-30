from collections import namedtuple
import logging
import random
import time

from pyleus.storm import Spout

log = logging.getLogger('exchange_rates')

ExchangeRate = namedtuple("ExchangeRate", "currency rate")

BASE = {"euro": 1.37, "pound": 1.62}
RANGE = 0.25


class RatesGeneratorSpout(Spout):

    OUTPUT_FIELDS = ExchangeRate

    def next_tuple(self):
        time.sleep(0.1)
        currency = random.choice(list(BASE.keys()))
        rate = BASE[currency] + random.uniform(-RANGE, RANGE)
        log.debug("{0} {1}".format(currency, rate))
        self.emit((currency, rate,))


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/tmp/micro_exchange_rates.log',
        format="%(message)s",
        filemode='a',
    )

    RatesGeneratorSpout().run()
