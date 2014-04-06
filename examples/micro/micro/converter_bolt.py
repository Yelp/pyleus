import logging

from pyleus.storm import SimpleBolt

from micro.rates_generator import ExchangeRate

log = logging.getLogger('conversions')

SHORT = {"pound": "GBP", "euro": "EUR"}


class ConverterBolt(SimpleBolt):

    OUTPUT_FIELDS = {
        "pound": ["value"],
        "euro": ["value"],
    }

    def initialize(self):
        self.rates = {"pound": None, "euro": None}

    def process_tuple(self, tup):
        if tup.comp == "micro-transactions":
            if tup.stream not in self.rates:
                raise ValueError("Unknown stream: {0}".format(tup.stream))
            if self.rates[tup.stream] is not None:
                value, = tup.values
                converted = round(value * self.rates[tup.stream], 2)
                log.debug("{0} {1} -> USD{2} ({3})".format(
                    SHORT[tup.stream],
                    value,
                    converted,
                    self.rates[tup.stream]))
                self.emit((converted,), tup.stream)

        elif tup.comp == "exchange-rates":
            rate = ExchangeRate(*tup.values)
            self.rates[rate.currency] = rate.rate

        else:
            raise ValueError("Unknown component: {0}".format(tup.component))


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/tmp/micro_conversions.log',
        format="%(message)s",
        filemode='a',
    )

    ConverterBolt().run()
