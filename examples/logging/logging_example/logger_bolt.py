import logging
import logging.config

from pyleus.storm import SimpleBolt

log = logging.getLogger("logging_example.logger_bolt")


class LoggerBolt(SimpleBolt):

    def process_tuple(self, tup):
        timestamp = tup.values[0]
        log.info("Received: %r", timestamp)


if __name__ == '__main__':
    LoggerBolt().run()
