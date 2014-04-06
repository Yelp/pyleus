import logging

from pyleus.storm import SimpleBolt

log = logging.getLogger('earnings')

GEO = {"pound": "UK", "euro": "Italy", "dollar": "US", "total": "total"}


class EarningsBolt(SimpleBolt):

    def initialize(self):
        self.earnings = {"pound": 0, "euro": 0, "dollar": 0, "total": 0}

    def process_tick(self):
        log.debug("-------------------------")
        for currency, earning in self.earnings.iteritems():
            log.debug("{0}: {1}USD".format(GEO[currency], earning))

    def process_tuple(self, tup):
        if tup.stream not in self.earnings:
            raise ValueError("Unknown stream: {0}".format(tup.stream))

        value, = tup.values
        self.earnings[tup.stream] += value
        self.earnings["total"] += value


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/tmp/micro_earnings.log',
        format="%(message)s",
        filemode='a',
    )

    EarningsBolt().run()
